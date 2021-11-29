//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Z Database

#ifndef Zdb_HPP
#define Zdb_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <lz4.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuPrint.hpp>
// #include <zlib/ZuPOD.hpp>
#include <zlib/ZuGrow.hpp>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmFn.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmPLock.hpp>

#include <zlib/ZtString.hpp>
#include <zlib/ZtEnum.hpp>

#include <zlib/ZePlatform.hpp>
#include <zlib/ZeLog.hpp>

#include <zlib/ZiFile.hpp>
#include <zlib/ZiMultiplex.hpp>
#include <zlib/ZiIOBuf.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZfbField.hpp>
#include <zlib/ZvTelemetry.hpp>
#include <zlib/ZvTelServer.hpp>

#if defined(ZDEBUG) && !defined(ZdbRep_DEBUG)
#define ZdbRep_DEBUG
#endif

#ifdef ZdbRep_DEBUG
#define ZdbDEBUG(env, e) do { if ((env)->debug()) ZeLOG(Debug, (e)); } while (0)
#else
#define ZdbDEBUG(env, e) ((void)0)
#endif

using ZdbRN = uint64_t;		// record ID
#define ZdbNullRN (~static_cast<uint64_t>(0))
#define ZdbMaxRN ZdbNullRN

#define ZdbFileRecs		16384
#define ZdbFileShift		14
#define ZdbFileMask		0x3fffU
#define ZdbFileSuperBlks	128	
#define ZdbFileSuperShift	7
#define ZdbFileSuperMask	0x7fU
#define ZdbFileIndexRecs	128	
#define ZdbFileIndexMask	0x7fU

// new file structure with variable-length flatbuffer-format records

// initial 1Kb super-block of 128 offsets, each to index-block
// each index-block is 1Kb index of 128 record offsets
// super-block is immediately followed by first index-block
// each record is {length,data,magic} - data is optionally lz4 compressed
// (compression/decompression happens on app load/save, otherwise
// data is uninterpreted by Zdb) - superblock is write-through cached
// in cached Zdb_File_ - along with LRU cache of 8 most recently accessed
// index blocks

// record data and replication messages are flatbuffers

// database ID is string (rarely used to index, so ok)

#include <zlib/zdbnet_fbs.h>

namespace ZdbNet {

namespace Op {
  ZfbEnumValues(Op, Add, Upd, Del);
}

// we use a custom header with an explicitly little-endian uint32 length
#pragma pack(push, 1)
struct Hdr {
  ZuLittleEndian<uint32_t>	len;	// length of body

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

// call following Finish() to ensure alignment
inline void saveHdr(Zfb::Builder &fbb) {
  unsigned len = fbb.GetSize();
  Hdr hdr{len};
  fbb.PushBytes(reinterpret_cast<uint8_t *>(&hdr), sizeof(Hdr));
}
// returns the total length of the message including the header,
// INT_MAX if not enough bytes have been read yet, -1 if corrupt
inline int loadHdr(const uint8_t *data, unsigned len) {
  if (ZuUnlikely(len < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  uint32_t bodyLen = hdr->len;
  if (bodyLen > (1U<<20)) return -1; // 1Mbyte
  return sizeof(Hdr) + bodyLen;
}

} // namespace ZdbNet

on-disk record format:

exact length (of data)
<flatbuffer data>
<4-byte alignment padding>
ZdbTrailer (rn, prevRN, magic)

deletions are null data

rn, prevRN are in ZdbObject (derives from ZmPolymorph, underlies all in-memory records)

per-DB persistent:
O -> type of object (used to calculate size and invoke destructor)

DB Handler (all defaulted to ZfbField):
LoadFn -> loads o from fbs
SaveFn -> saves o to fbs

push()
LoadFn -> (void *) placement new O (passed to ZdbObject ctor, invoked by that to initialize contained T)
(replaces AllocFn, is templated per-push() caller to permit efficient captures/ctor arguments, defaults to default ctor)

next() -> allocates next RN
push(RN, args...) -> idempotent push, passes args... to object ctor
put(o) -> cache o, save o to IOBuf, write IOBuf

(no need to pin, since IOBuf is used for I/O)

update()
  - DB file write is entire record via save()
  - update is mutation in-place, update of RN (and re-caching/indexing),
    streaming out of serialized updates - if app requests prevRN, then
    that will be a cache-miss load

del() is similar to update, but file write and replication of empty data

class ZdbEnv;				// database environment
class Zdb;				// individual database
class ZdbHost;				// host
class Zdb_Cxn;				// connection

// file index block cache

struct Zdb_FileIndex_ {
  Zdb_FileIndex_(unsigned super_) : super{super_} {
    memset(offset, 0, 128 * sizeof(uint64_t));
  }
  uint64_t		offset[128];	// RN -> record offset
  unsigned		super = 0;	// superblock offset

  Zdb_FileIndex_() = delete;
  Zdb_FileIndex_(const Zdb_FileIndex_ &) = delete;
  Zdb_FileIndex_ &operator =(const Zdb_FileIndex_ &) = delete;
  Zdb_FileIndex_(Zdb_FileIndex_ &&) = delete;
  Zdb_FileIndex_ &operator =(Zdb_FileIndex_ &&) = delete;
};
struct Zdb_FileIndex_SuperAccessor {
  static unsigned get(const Zdb_FileIndex_ &index) { return index.super; }
};

using Zdb_FileIndexLRU =
  ZmList<Zdb_FileIndex_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using Zdb_FileIndexLRUNode = Zdb_FileIndexLRU::Node;

struct Zdb_FileIndexHeapID {
  static constexpr const char *id() { return "Zdb.FileIndex"; }
};
using Zdb_FileIndexCache =
  ZmHash<Zdb_FileIndexLRUNode,
    ZmHashKey<Zdb_FileIndex_SuperAccessor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<Zdb_FileIndexHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;
using Zdb_FileIndex = Zdb_FileIndexCache::Node;

// file cache

class Zdb_File_ : public ZiFile {
friend Zdb_File_IDAccessor;

public:
  Zdb_File_(uint64_t id) : m_id{id} { }

  uint64_t id() const { return m_id; }

  bool del(unsigned i) {
    if (!m_deleted[i]) {
      m_deleted[i].set();
      --m_undelCount;
    }
    return !m_undelCount;
  }

  bool open(

  static constexpr unsigned superSize() {
    return ZdbFileSuperBlks * sizeof(uint64_t);
  }
  static constexpr unsigned indexSize() {
    return ZdbFileIndexRecs * sizeof(uint64_t);
  }
  uint64_t appendIndex(Zdb *db, ZdbFileIndex *index) {
    auto off = m_offset;
    if (off < superSize()) off = superSize();
    m_super[index->super] = off;
    if (ZuUnlikely((r = pwrite(
	      0, &m_super[0], superSize(), &e)) != Zi::OK)) {
      m_super[index->super] = 0;
      db->fileWrError_(this, off, e);
      return 0;
    }
    if (ZuUnlikely((r = pwrite(
	      off, &(index->offset)[0], indexSize(), &e)) != Zi::OK)) {
      db->fileWrError_(this, off, e);
      return 0;
    }
    m_offset = off + indexSize();
    return off;
  }
  uint64_t offsetIndex(ZdbRN rn) {
    return m_super[(rn>>ZdbFileSuperShift) & ZdbFileSuperMask];
  }

  uint64_t append(ZdbRN rn) {

  }

  void checkpoint() { sync(); }

private:
  uint64_t		m_id = 0;	// (RN>>14)
  unsigned		m_undelCount = ZdbFileRecs;
  uint64_t		m_offset = 0;	// append offset
  ZuBitmap<ZdbFileRecs>	m_deleted;
  uint64_t		m_super[128];	// (RN>>7) -> index block offset
};
struct Zdb_File_IDAccessor {
  static unsigned get(const Zdb_File_ &file) { return file.index(); }
};

using Zdb_FileLRU =
  ZmList<Zdb_File_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using Zdb_FileLRUNode = Zdb_FileLRU::Node;

struct Zdb_FileHeapID {
  static constexpr const char *id() { return "Zdb.File"; }
};
using Zdb_FileCache =
  ZmHash<Zdb_FileLRUNode,
    ZmHashKey<Zdb_File_IDAccessor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<Zdb_FileHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;
using Zdb_File = Zdb_FileCache::Node;

  // FIXME - superblock
  // FIXME - global index-block write-through cache
  //
  // FIXME - fix getFile(), rn2file() - may want to specialize
  // rn2file(rn, write==true) writes from reads
  //
  // initial 1Kb super-block of 128 offsets, each to index-block
  // each index-block is 1Kb index of 128 record offsets
  // (128*128 == 16384)
  //

// ZuFields(Zdb_File_, ((index, RdFn)));


// pinned file + RN within file

class Zdb_FileRec {
public:
  Zdb_FileRec() { }
  Zdb_FileRec(ZmRef<Zdb_File> file, uint64_t offset) :
    m_file{ZuMv(file)}, m_offset{offset} { }

  bool operator !() const { return !m_file; }
  ZuOpBool

  Zdb_File *file() const { return m_file; }
  uint64_t offset() const { return m_offset; }

private:
  ZmRef<Zdb_File>	m_file = nullptr;
  uint64_t		m_offset = 0;
};

#define ZdbAllocated 0xa110c8ed // "allocated"
#define ZdbCommitted 0xc001da7a // "cool data"
#define ZdbDeleted   0xdeadda7a	// "dead data"

#pragma pack(push, 1)
struct ZdbTrailer {
  ZdbRN		rn;
  ZdbRN		prevRN;
  uint32_t	magic;
};
#pragma pack(pop)

struct ZdbBuf_HeapID {
  static constexpr const char *id() { return "ZdbBuf"; }
};
template <typename Heap>
struct ZdbBuf__ : public ZiIOBuf_<ZuGrow(0, 1), Heap> {
  ZmRef<ZdbAnyObject>	object;
  ZdbRN			rn = ZdbNullRN;
  ZdbRN			prevRN = ZdbNullRN;
  int			op = ZdbNet::Op::Invalid;
  unsigned		ulen = 0;
  bool			recSend = false;
};
using ZdbBuf_Heap = ZmHeap<ZdbBuf_HeapID, sizeof(ZdbBuf_<ZuNull>)>;
class ZdbAPI ZdbBuf : public ZdbBuf_<ZdbBuf_Heap> {
  using Base = ZdbBuf_<ZdbBuf_Heap>;

friend Zdb_Cxn;
friend ZdbAnyObject;

  template <typename ...Args>
  ZdbBuf(Args &&... args) : Base{ZuFwd<Args>(args)...} { }

  void send(ZiIOContext &);
  void sent(ZiIOContext &);

  int write();
};

namespace ZdbNet {

// FIXME - make use of ZiRx/Tx

}

using ZdbLRU =
  ZmList<ZmPolymorph,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using ZdbLRUNode = ZdbLRU::Node;

struct ZdbLRUNode_RNAccessor {
  static ZdbRN get(const ZdbLRUNode &pod);
};

struct Zdb_Cache_ID {
  static constexpr const char *id() { return "Zdb.Cache"; }
};

using Zdb_Cache =
  ZmHash<ZdbLRUNode,
    ZmHashKey<ZdbLRUNode_RNAccessor,
      ZmHashObject<ZmPolymorph,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<ZuNull,
	    ZmHashID<Zdb_Cache_ID,
	      ZmHashLock<ZmNoLock> > > > > > >;
using Zdb_CacheNode = Zdb_Cache::Node;

struct ZdbAPI ZdbAnyObject : public Zdb_CacheNode, public ZuPrintable {
  Zdb		*db;
  ZdbRN		rn;
  ZdbRN		prevRN = ZdbNullRN;
  bool		deleted = false;

  ZdbAnyObject(Zdb *db_, ZdbRN rn_) : db{db_}, rn{rn_} { }
  virtual ~ZdbAnyObject();

  ZmRef<ZdbBuf> replicate(int type, int op, bool compress);

  virtual void *ptr_() = 0;
  const void *ptr_() const { return const_cast<ZdbAnyObject *>(this)->ptr_(); }

  template <typename S> void print(S &s) const {
    s << "rn=" << rn << " prevRN=" << prevRN;
  }

  ZdbAnyObject() = delete;
  ZdbAnyObject(const ZdbAnyObject &) = delete;
  ZdbAnyObject &operator =(const ZdbAnyObject &) = delete;
  ZdbAnyObject(ZdbAnyObject &&) = delete;
  ZdbAnyObject &operator =(ZdbAnyObject &&) = delete;
};

ZdbRN ZdbLRUNode_RNAccessor::get(const ZdbLRUNode &node)
{
  return static_cast<const ZdbAnyObject &>(node).rn;
}

struct ZdbObject_HeapID { 
  static constexpr const char *id() { return "Zdb.Object"; }
};
template <typename T, typename Heap>
struct ZdbObject_ : public Heap, public ZdbAnyObject {
  ZdbObject_() = delete;
  ZdbObject_(const ZdbObject_ &) = delete;
  ZdbObject_ &operator =(const ZdbObject_ &) = delete;
  ZdbObject_(ZdbObject_ &&) = delete;
  ZdbObject_ &operator =(ZdbObject_ &&) = delete;

  template <typename L>
  ZdbObject_(Zdb *db_, ZdbRN rn_, L l) : ZdbAnyObject{db_, rn_} {
    l(static_cast<void *>(&m_data[0]));
  }

  void *ptr_() { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~ZdbObject() { ptr()->~T(); }

  const T &data() const & { return *ptr(); }
  T &data() & { return *ptr(); }
  T &&data() && { return ZuMv(*ptr()); }

  template <typename S> void print(S &s) const {
    ZdbAnyObject::print(s);
    s << ' ' << this->data();
  }

private:
  char		m_data[sizeof(T)];
};
template <typename T>
using ZdbObject_Heap = ZmHeap<ZdbObject_HeapID, sizeof(ZdbObject_<T, ZuNull>)>;
template <typename T>
using ZdbObject = ZdbObject_<T, ZdbObject_Heap<T>>;

// CtorFn(ptr, data, length) - construct new object from flatbuffer
typedef ZdbAnyObject *(*ZdbCtorFn)(Zdb *, ZdbRN, const uint8_t *, unsigned);
// LoadFn(object, data, length) - load flatbuffer into object
typedef ZdbAnyObject *(*ZdbLoadFn)(ZdbAnyObject *, const uint8_t *, unsigned);
// SaveFn(fbb, ptr) - save *ptr into flatbuffer fbb
typedef void (*ZdbSaveFn)(Zfb::Builder &, const void *);
// AddFn(ptr, op, recovered) - object recovered or replicated from peer
// Note: ptr can be null if op is ZdbOp::Delete
typedef void (*ZdbAddFn)(void *, int, bool);
// WriteFn(ptr, op) - object written (created/updated)
typedef void (*ZdbWriteFn)(void *, int);

struct ZdbHandler {
  ZdbLoadFn		ctorFn =
    [](Zdb *db, ZdbRN rn, const uint8_t *, unsigned) ->
	ZdbAnyObject * { return new ZdbAnyObject{db, rn}; };
  ZdbSaveFn		saveFn = nullptr;
  ZdbAddFn		addFn = nullptr;
  ZdbWriteFn		writeFn = nullptr;

  template <typename T>
  static ZdbHandler bind() {
    return ZdbHandler{
      .ctorFn = [](Zdb *db, ZdbRN rn, const uint8_t *data, unsigned len) ->
	  ZdbAnyObject * {
	auto fbo = Zfb::verify<T>(data, len);
	if (ZuUnlikely(!fbo)) return nullptr;
	return new ZdbObject<T>{db, rn, [fbo](void *ptr) {
	  Zfb::ctor<T>(ptr, fbo);
	}};
      },
      .loadFn =
	[](ZdbAnyObject *object, const uint8_t *data, unsigned len) ->
	  ZdbAnyObject * {
	auto fbo = Zfb::verify<T>(data, len);
	if (ZuUnlikely(!fbo)) return nullptr;
	auto &data = static_cast<ZdbObject<T> *>(object)->data();
	Zfb::load<T>(data, fbo);
	return object;
      },
      .saveFn = [](Zfb::Builder &fbb, const void *ptr) {
	Zfb::save<T>(fbb, *reinterpret_cast<const T *>(ptr));
      }
    };
  }
};

namespace ZdbCacheMode {
  using namespace ZvTelemetry::DBCacheMode;
}

struct ZdbCf {
  ZuID			id;
  unsigned		version = 0;
  int			cacheMode = ZdbCacheMode::Normal;
  unsigned		preAlloc = 0;	// #records to pre-allocate
  uint8_t		repMode = 0;	// 0 - deferred, 1 - in put()
  bool			compress = false;

  ZdbCf() = default;
  ZdbCf(ZuString id_) : id{id_} { }
  ZdbCf(ZuString id_, const ZvCf *cf) : id{id_} {
    version = cf->getInt("version", 0, INT_MAX, false, 0);
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", false, ZdbCacheMode::Normal);
    preAlloc = cf->getInt("preAlloc", 0, 10<<24, false, 0);
    repMode = cf->getInt("repMode", 0, 1, false, 0);
    compress = cf->getInt("compress", 0, 1, false, 0);
  }

  struct IDAxor { static ZuID get(const ZdbCf &cf) { return cf.id; } };
};
struct ZdbCfs_HeapID {
  static constexpr const char *id() { return "ZdbEnv.DBCfs"; }
};
using ZdbCfs =
  ZmRBTree<ZdbCf,
    ZmRBTreeKey<ZdbCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZdbCfs_HeapID> > > >;

class ZdbAPI Zdb : public ZmPolymorph {
friend ZdbEnv;
friend ZdbAnyObject;

  using Cache = Zdb_Cache;
  using FileLRU = Zdb_FileLRU;
  using FileCache = Zdb_FileCache;
  using FileIndexCache = Zdb_FileIndexCache;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using FSLock = ZmLock;
  using FSGuard = ZmGuard<FSLock>;
  using FSReadGuard = ZmReadGuard<FSLock>;

  Zdb(ZdbEnv *env, ZdbCf *cf, ZdbHandler handler);

public:
  ~Zdb();

private:
  void init();
  void final();

  bool open();
  void close();

  bool recover();
  void checkpoint();
  void checkpoint_();

public:
  struct IDAxor {
    static ZuID get(const Zdb &db) { return db.m_id; }
  };

  const ZdbCf &config() const { return *m_cf; }

  ZdbEnv *env() const { return m_env; }
  ZuID id() const { return m_id; }
  unsigned recSize() { return m_recSize; }
  unsigned dataSize() { return m_dataSize; }
  unsigned cacheSize() { return m_cacheSize; } // unclean read
  unsigned filesMax() { return m_filesMax; }

  // first RN that is committed (will be ZdbMaxRN if DB is empty)
  ZdbRN minRN() { return m_minRN; }
  // next RN that will be allocated
  ZdbRN nextRN() { return m_nextRN; }

  // create new placeholder record (null RN, in-memory only, never in DB)
  ZmRef<ZdbAnyObject> placeholder();

  // create new record
  ZmRef<ZdbAnyObject> push();
  // create new record (idempotent)
  ZmRef<ZdbAnyObject> push(ZdbRN rn);
  // allocate RN only for new record, for later use with push(rn)
  ZdbRN pushRN();
  // commit record following push() - causes replication / sync
  void put(ZdbAnyObject *);
  // abort push()
  void abort(ZdbAnyObject *);

  // get record
  ZmRef<ZdbAnyObject> get(ZdbRN rn);	// use for read-only queries
  ZmRef<ZdbAnyObject> get_(ZdbRN rn);	// use for RMW - does not update cache

  // update record
  ZmRef<ZdbAnyObject> update(ZdbAnyObject *prev);
  // update record (idempotent)
  ZmRef<ZdbAnyObject> update(ZdbAnyObject *prev, ZdbRN rn);
  // update record (with prevRN, without prev POD)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN);
  // update record (idempotent) (with prevRN, without prev POD)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN, ZdbRN rn);
  // commit record following update(), potentially a partial update
  void putUpdate(ZdbAnyObject *);

  // delete record following get() / get_()
  void del(ZdbAnyObject *);

  // delete all records < minRN
  void purge(ZdbRN minRN);

  using Telemetry = ZvTelemetry::DB;

  void telemetry(Telemetry &data) const;

private:
  // application call handlers
  void recover(ZmRef<ZdbAnyObject> pod, int op);
  void replicate(ZdbAnyObject *pod, void *ptr, int op);

  // push initial record
  ZmRef<ZdbAnyObject> push_();
  // idempotent push
  ZmRef<ZdbAnyObject> push_(ZdbRN rn);

  // low-level get, does not filter deleted records
  ZmRef<ZdbAnyObject> get__(ZdbRN rn);

  // replication data rcvd (copy/commit, called while env is unlocked)
  ZmRef<ZdbAnyObject> replicated(
      ZdbRN rn, ZdbRN prevRN, int op, const uint8_t *data, unsigned len);

  // apply received replication data
  ZmRef<ZdbAnyObject> replicated_(
      ZdbRN rn, ZdbRN prevRN, int op, const uint8_t *data, unsigned len);

  ZiFile::Path dirName(uint64_t id) const {
    return ZiFile::append(m_cf->path, ZuStringN<8>() <<
	ZuBox<unsigned>(id>>20).hex(ZuFmt::Right<5>()));
  }
  ZiFile::Path fileName(ZiFile::Path dir, uint64_t id) const {
    return ZiFile::append(dir, ZuStringN<12>() <<
	ZuBox<unsigned>(id & 0xfffffU).hex(ZuFmt::Right<5>()) << ".zdb");
  }
  ZiFile::Path fileName(uint64_t id) const {
    return fileName(dirName(id), id);
  }

  Zdb_FileRec rn2file(ZdbRN rn, bool write);

  ZmRef<Zdb_File> getFile(unsigned i, bool create);
  ZmRef<Zdb_File> openFile(unsigned i, bool create);
  void delFile(Zdb_File *file);
  void recover(Zdb_File *file);
  void scan(Zdb_File *file);

  ZmRef<ZdbAnyObject> read_(const Zdb_FileRec &);

  void write(ZmRef<ZdbAnyObject> pod);
  void write_(ZdbRN rn, ZdbRN prevRN, const void *ptr, int op);

  void fileRdError_(Zdb_File *, ZiFile::Offset, int, ZeError e);
  void fileWrError_(Zdb_File *, ZiFile::Offset, ZeError e);

  void cache(ZdbAnyObject *object);
  void cache_(ZdbAnyObject *object);
  void cacheDel_(ZdbRN rn);

  ZdbEnv		*m_env;
  const ZdbCf		*m_cf = nullptr;
  ZuID			m_id = 0;
  ZdbHandler		m_handler;
  unsigned		m_recSize = 0;
  unsigned		m_dataSize = 0;
  unsigned		m_fileSize = 0;
  Lock			m_lock;
    ZdbRN		  m_minRN = ZdbMaxRN;
    ZdbRN		  m_nextRN = 0;
    ZdbRN		  m_fileRN = 0;
    ZdbLRU		  m_lru;
    ZmRef<Cache>	  m_cache;
    unsigned		  m_cacheSize = 0;
    uint64_t		  m_cacheLoads = 0;
    uint64_t		  m_cacheMisses = 0;
  FSLock		m_fsLock;	// guards files
    FileLRU		  m_filesLRU;
    ZmRef<FileCache>	  m_files;
    ZmRef<FileIndexCache> m_fileIndices;
    unsigned		  m_filesMax = 0;
    unsigned		  m_lastFile = 0;
    uint64_t		  m_fileLoads = 0;
    uint64_t		  m_fileMisses = 0;
};
struct Zdbs_HeapID {
  static constexpr const char *id() { return "ZdbEnv.DBs"; }
};
using Zdbs =
  ZmRBTree<Zdb,
    ZmRBTreeKey<Zdb::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<Zdbs_HeapID> > > >;

using Zdb_DBState_ = ZmLHashKV<ZuID, ZdbRN, ZmLHashLocal<>>;
struct Zdb_DBState : public Zdb_DBState_ {
  Zdb_DBState(unsigned size) : Zdb_DBState_{ZmHashParams{size}} { }

  Zdb_DBState(Zfb::Vector<const ZdbNet::fbs::DBState *> *envState) :
      Zdb_DBState_{ZmHashParams{envState->size()}} {
    using namespace ZdbNet;
    using namespace Zfb::Load;
    all(envState, [this](unsigned, fbs::DBState *dbState) {
      add(id(dbState->db()), dbState->rn());
    });
  }
  Zfb::Offset<Zfb::Vector<const fbs::DBState *>> save(Zfb::Builder &fbb) {
    using namespace ZdbNet;
    using namespace Zfb::Save;
    auto i = readIterator();
    return structVecIter(fbb, count(), [&i](void *ptr, unsigned) {
      if (auto node = i.iterate())
	new (ptr) fbs::DBState{id(node->key()), node->val()};
      else
	new (ptr) fbs::DBState{}; // unused
    });
  }

  int cmp(const Zdb_DBState &r) {
    bool inconsistent;
    {
      unsigned n = count();
      inconsistent = n != r.count();
      if (!n && !inconsistent) return 0;
    }
    int diff, ret = 0;
    {
      auto i = readIterator();
      while (auto node = i.iterate()) {
	auto rnode = r.find(node->key());
	if (!rnode)
	  diff = !!node->val();
	else
	  diff = ZuCmp<ZdbRN>::cmp(node->val(), rnode->val());
	if (diff < 0) {
	  if (ret > 0) return ZuCmp<int>::null();
	  ret += diff;
	} else if (diff > 0) {
	  if (ret < 0) return ZuCmp<int>::null();
	  ret += diff;
	}
      }
    }
    if (inconsistent) {
      auto i = r.readIterator();
      while (auto rnode = i.iterate()) {
	auto node = find(rnode->key());
	if (!node)
	  diff = -!!rnode->val();
	else
	  diff = ZuCmp<ZdbRN>::cmp(node->val(), rnode->val());
	if (diff < 0) {
	  if (ret > 0) return ZuCmp<int>::null();
	  ret += diff;
	} else if (diff > 0) {
	  if (ret < 0) return ZuCmp<int>::null();
	  ret += diff;
	}
      }
    }
    return ret;
  }
};
struct Zdb_DBState_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, const Zdb_DBState &a) {
    unsigned n = a.count();
    if (ZuUnlikely(!n)) return;
    unsigned j = 0;
    auto i = a.readIterator();
    while (auto node = i.iterate()) {
      if (j++) s << ',';
      s << '{' << node->key() << ',' << ZuBoxed(node->val()) << '}';
    }
  }
};
Zdb_DBState_Print ZuPrintType(Zdb_DBState *);

struct ZdbHostCf {
  unsigned	id = 0;
  unsigned	priority = 0;
  ZiIP		ip;
  uint16_t	port = 0;
  ZtString	up;
  ZtString	down;

  ZdbHostCf(const ZtString &key, const ZvCf *cf) {
    id = ZvCf::toInt(cf, "ID", key, 0, 1<<30);
    priority = cf->getInt("priority", 0, 1<<30, true);
    ip = cf->get("IP", true);
    port = cf->getInt("port", 1, (1<<16) - 1, true);
    up = cf->get("up");
    down = cf->get("down");
  }

  struct IDAxor {
    static ZuID get(const ZdbHostCf &cfg) { return cfg.id; }
  };
};
struct ZdbHostCfs_HeapID {
  static constexpr const char *id() { return "ZdbEnv.HostCfs"; }
};
using ZdbHostCfs =
  ZmRBTree<ZdbHostCf,
    ZmRBTreeKey<ZdbHostCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZdbHostCfs_HeapID> > > >;

namespace ZdbHostState {
  using namespace ZvTelemetry::DBHostState;
}

class ZdbAPI ZdbHost : public ZmPolymorph {
friend ZdbEnv;
friend Zdb_Cxn;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  ZdbHost(ZdbEnv *env, const ZdbHostCf *config);

public:
  struct IDAxor { static int get(const ZdbHost &h) { return h->id(); } };

  const ZdbHostCf &config() const { return *m_cf; }

  unsigned id() const { return m_cf->id; }
  unsigned priority() const { return m_cf->priority; }
  ZiIP ip() const { return m_cf->ip; }
  uint16_t port() const { return m_cf->port; }

  bool voted() const { return m_voted; }
  int state() const { return m_state; }

  static const char *stateName(int);

  using Telemetry = ZvTelemetry::DBHost;

  void telemetry(Telemetry &data) const;

  template <typename S> void print(S &s) const {
    s << "[ID:" << id() << " PRI:" << priority() << " V:" << voted() <<
      " S:" << state() << "] " << dbState();
  }
  friend ZuPrintFn ZuPrintType(ZdbHost *);

private:
  ZmRef<Zdb_Cxn> cxn() const { return m_cxn; }

  void state(int s) { m_state = s; }

  const Zdb_DBState &dbState() const { return m_dbState; }
  Zdb_DBState &dbState() { return m_dbState; }

  bool active() const {
    using namespace ZdbHostState;
    switch (m_state) {
      case Activating:
      case Active:
	return true;
    }
    return false;
  }

  int cmp(const ZdbHost *host) const {
    int i;
    if (i = m_dbState.cmp(host->m_dbState)) return i;
    if (i = ZuCmp<bool>::cmp(active(), host->active())) return i;
    return ZuCmp<int>::cmp(priority(), host->priority());
  }

#if 0
  int cmp(const ZdbHost *host) {
    int i = cmp_(host);

    printf("cmp(host %d priority %d dbState %d, "
	   "host %d priority %d dbState %d): %d\n",
	   (int)this->id(),
	   (int)this->config().m_priority, (int)this->dbState()[0],
	   (int)host->id(),
	   (int)host->config().m_priority,
	   (int)((ZdbHost *)host)->dbState()[0], i);
    return i;
  }
#endif

  void voted(bool v) { m_voted = v; }

  void connect();
  void connectFailed(bool transient);
  void reconnect();
  void reconnect2();
  void cancelConnect();
  ZiConnection *connected(const ZiCxnInfo &ci);
  void associate(Zdb_Cxn *cxn);
  void disconnected();

  void reactivate();

  ZdbEnv		*m_env;
  const ZdbHostCf	*m_cf;
  ZiMultiplex		*m_mx;

  Lock			m_lock;
    ZmRef<Zdb_Cxn>	  m_cxn;

  ZmScheduler::Timer	m_connectTimer;

  // guarded by ZdbEnv

  int			m_state = ZdbHostState::Instantiated;
  Zdb_DBState		m_dbState;
  bool			m_voted = false;
};
struct ZdbHosts_HeapID {
  static constexpr const char *id() { return "ZdbEnv.Hosts"; }
};
using ZdbHosts =
  ZmRBTree<ZdbHost,
    ZmRBTreeKey<ZdbHost::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZdbHosts_HeapID> > > >;

using ZdbHost_Ptr = ZdbHost *;
struct ZdbHost_Ptr_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, ZdbHost_Ptr v) {
    if (!v)
      s << "(null)";
    else
      s << *v;
  }
};
ZdbHost_Ptr_Print ZuPrintType(ZdbHost_Ptr *);

class Zdb_Cxn : public ZiConnection {
  Zdb_Cxn(const Zdb_Cxn &);
  Zdb_Cxn &operator =(const Zdb_Cxn &);	// prevent mis-use

friend ZdbEnv;
friend ZdbHost;

  Zdb_Cxn(ZdbEnv *env, ZdbHost *host, const ZiCxnInfo &ci);

  ZdbEnv *env() const { return m_env; }
  void host(ZdbHost *host) { m_host = host; }
  ZdbHost *host() const { return m_host; }
  ZiMultiplex *mx() const { return m_mx; }

  void connected(ZiIOContext &);
  void disconnected();

  void msgRead(ZiIOContext &);
  bool msgRcvd(ZiIOContext &);

  bool hbRcvd(ZiIOContext &);
  void hbTimeout();
  void hbSend();
  void hbSend_(ZiIOContext &);
  void hbSent(ZiIOContext &);
  void hbSent2(ZiIOContext &);

  bool repRcvd(ZiIOContext &);
  void repDataRead(ZiIOContext &);
  void repDataRcvd(ZiIOContext &);

  void repSend(ZmRef<ZdbAnyObject> pod, int type, int op, bool compress);
  void repSend(ZmRef<ZdbAnyObject> pod);

  void ackRcvd();
  void ackSend(int type, ZdbAnyObject *o);

  ZdbEnv		*m_env;
  ZiMultiplex		*m_mx;
  ZdbHost		*m_host;	// 0 if not yet associated

  ZmRef<ZdbBuf>		m_rxBuf;
#if 0
  Zdb_Msg_Hdr		m_recvHdr;
  ZtArray<char>		m_recvData;
  ZtArray<char>		m_recvData2;

  Zdb_Msg_Hdr		m_hbSendHdr;

  Zdb_Msg_Hdr		m_ackSendHdr;
#endif

  ZmScheduler::Timer	m_hbTimer;
};

// ZdbEnv configuration
struct ZdbEnvCf {
  ZtString			path;
  ZmThreadName			writeThread;
  mutable unsigned		writeTID = 0;
  ZdbCfs			dbCfs;
  ZdbHostCfs			hostCfs;
  unsigned			hostID = 0;
  unsigned			nAccepts = 0;
  unsigned			heartbeatFreq = 0;
  unsigned			heartbeatTimeout = 0;
  unsigned			reconnectFreq = 0;
  unsigned			electionTimeout = 0;
  ZmHashParams			cxnHash;
#ifdef ZdbRep_DEBUG
  bool				debug = 0;
#endif

  ZdbEnvCf() = default;
  ZdbEnvCf(const ZvCf *cf) {
    path = cf->get("path", true);
    writeThread = cf->get("writeThread", true);
    const ZtArray<ZtString> *names = cf->getMultiple("dbs", 0, 100, true);
    dbCfs.size(names->length());
    for (unsigned i = 0; i < names->length(); i++) {
      ZmRef<ZvCf> dbCf = cf->subset((*names)[i], true);
      dbConfigs.addNode(new ZdbCfs::Node{(*names)[i], dbCf});
    }
    {
      ZvCf::Iterator i(cf->subset("dbs", true));
      ZuString key;
      while (ZmRef<ZvCf> dbCf = i.subset(key))
	dbCfs.addNode(new ZdbCfs::Node{key, dbCf});
    }
    {
      ZvCf::Iterator i(cf->subset("hosts", true));
      ZuString key;
      while (ZmRef<ZvCf> hostCf = i.subset(key))
	hostCfs.addNode(new HostCfs::Node{key, hostCf});
    }
    hostID = cf->getInt("hostID", 0, 1<<30, true);
    nAccepts = cf->getInt("nAccepts", 1, 1<<10, false, 8);
    heartbeatFreq = cf->getInt("heartbeatFreq", 1, 3600, false, 1);
    heartbeatTimeout = cf->getInt("heartbeatTimeout", 1, 14400, false, 4);
    reconnectFreq = cf->getInt("reconnectFreq", 1, 3600, false, 1);
    electionTimeout = cf->getInt("electionTimeout", 1, 3600, false, 8);
    cxnHash.init(cf->get("cxnHash", false, "Zdb.CxnHash"));
#ifdef ZdbRep_DEBUG
    debug = cf->getInt("debug", 0, 1, false, 0);
#endif
  }
};

class ZdbAPI ZdbEnv : public ZmPolymorph {
  ZdbEnv(const ZdbEnv &);
  ZdbEnv &operator =(const ZdbEnv &);		// prevent mis-use

friend Zdb;
friend ZdbHost;
friend Zdb_Cxn;
friend ZdbAnyObject;

  struct CxnHash_HeapID {
    static constexpr const char *id() { return "ZdbEnv.CxnHash"; }
  };
  using CxnHash =
    ZmHash<ZmRef<Zdb_Cxn>,
      ZmHashLock<ZmPLock,
	ZmHashObject<ZuNull,
	  ZmHashHeapID<CxnHash_HeapID> > > >;

  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  using StateCond = ZmCondition<Lock>;

#ifdef ZdbRep_DEBUG
  bool debug() const { return m_cf.debug; }
#endif

public:
  ZdbEnv();
  ~ZdbEnv();

  void init(ZdbEnvCf config, ZiMultiplex *mx,
      ZmFn<> activeFn, ZmFn<> inactiveFn);
  void final();

  bool open();
  void close();

  void start();
  void stop();

  void checkpoint();

  const ZdbEnvCf &config() const { return m_cf; }
  ZiMultiplex *mx() const { return m_mx; }

  int state() const {
    return m_self ? m_self->state() : ZdbHostState::Instantiated;
  }
  void state(int n) {
    if (!m_self) {
      ZeLOG(Fatal, ZtString() <<
	  "ZdbEnv::state(" << ZuBoxed(n) << ") called out of order");
      return;
    }
    m_self->state(n);
  }
  bool running() {
    using namespace ZdbHostState;
    switch (state()) {
      case Electing:
      case Activating:
      case Active:
      case Deactivating:
      case Inactive:
	return true;
    }
    return false;
  }
  bool active() {
    using namespace ZdbHostState;
    switch (state()) {
      case Activating:
      case Active:
	return true;
    }
    return false;
  }

  ZdbHost *self() const { return m_self; }
  ZdbHost *host(unsigned id) const { return m_hosts.findKey(id); }
  template <typename L> bool allHosts(L l) const {
    auto i = m_hosts.readIterator();
    while (auto node = i.iterate())
      if (!l(node->key())) return false;
    return true;
  }

private:
  Zdb *db_(ZuID id, ZdbHandler handler);
public:
  Zdb *db(ZuID id);
  template <typename T>
  Zdb *add(ZuID id) {
    return db_(id, ZdbHandler::bind<T>());
  }
private:
  template <typename L> bool allDBs_(L l) const {
    auto i = m_dbs.readIterator();
    while (auto node = i.iterate())
      if (!l(node)) return false;
    return true;
  }
public:
  template <typename L> bool allDBs(L l) const {
    ReadGuard guard(m_lock);
    return allBDs_(ZuMv(l));
  }

  using Telemetry = ZvTelemetry::DBEnv;

  void telemetry(Telemetry &data) const;

  ZvTelemetry::DBEnvFn telFn() {
    return ZvTelemetry::DBEnvFn{ZmMkRef(this), [](
	ZdbEnv *dbEnv,
	ZmFn<const ZvTelemetry::DBEnv &> envFn,
	ZmFn<const ZvTelemetry::DBHost &> hostFn,
	ZmFn<const ZvTelemetry::DB &> dbFn) {
      {
	ZvTelemetry::DBEnv data;
	dbEnv->telemetry(data);
	envFn(data);
      }
      dbEnv->allHosts([&hostFn](const ZdbHost *host) {
	ZvTelemetry::DBHost data;
	host->telemetry(data);
	hostFn(data);
	return true;
      });
      dbEnv->allDBs([&dbFn](const Zdb *db) {
	ZvTelemetry::DB data;
	db->telemetry(data);
	dbFn(data);
	return true;
      });
    }};
  }

private:
  unsigned dbCount() { return m_dbs.count(); }

  void listen();
  void listening(const ZiListenInfo &);
  void listenFailed(bool transient);
  void stopListening();

  bool disconnectAll();

  void holdElection();	// elect new master
  void deactivate();	// become client (following dup master)
  void reactivate(ZdbHost *host);	// re-assert master

  ZiConnection *accepted(const ZiCxnInfo &ci);
  void connected(Zdb_Cxn *cxn);
  void associate(Zdb_Cxn *cxn, int hostID);
  void associate(Zdb_Cxn *cxn, ZdbHost *host);
  void disconnected(Zdb_Cxn *cxn);

  void hbDataRcvd(ZdbHost *host, const ZdbNet::fbs::Msg_HB *hb);
  void vote(ZdbHost *host);

  void hbStart();
  void hbSend();		// send heartbeat and reschedule self
  void hbSend_(Zdb_Cxn *cxn);	// send heartbeat (once)

  void dbStateRefresh();	// refresh m_self->dbState() (with guard)
  void dbStateRefresh_();	// '' (unlocked)

  ZdbHost *setMaster();	// returns old master
  void setNext(ZdbHost *host);
  void setNext();

  void startReplication();
  void stopReplication();

  void repDataRcvd(ZdbHost *host, const fbs::Msg_Rep *rep);

  void repSend(ZmRef<ZdbAnyObject> pod, int type, int op, bool compress);
  void repSend(ZmRef<ZdbAnyObject> pod);
  void recSend();

  void ackRcvd(ZdbHost *host, bool positive, ZuID db, ZdbRN rn);

  void write(ZmRef<ZdbAnyObject> pod, int type, int op, bool compress);

  ZdbEnvCf		m_cf;
  ZiMultiplex		*m_mx;

  ZmFn<>		m_activeFn;
  ZmFn<>		m_inactiveFn;

  Lock			m_lock;
    StateCond		m_stateCond;
    bool		m_appActive;
    ZdbHost		*m_self;
    ZdbHost		*m_master;	// == m_self if Active
    ZdbHost		*m_prev;	// previous-ranked host
    ZdbHost		*m_next;	// next-ranked host
    ZmRef<Zdb_Cxn>	m_nextCxn;	// replica peer's cxn
    bool		m_recovering;	// recovering next-ranked host
    Zdb_DBState		m_recover;	// recovery state
    Zdb_DBState		m_recoverEnd;	// recovery end
    int			m_nPeers;	// # up to date peers
					// # votes received (Electing)
					// # pending disconnects (Stopping)
    ZmTime		m_hbSendTime;
    Zdbs		m_dbs;
    ZdbHosts		m_hosts;

  ZmScheduler::Timer	m_hbSendTimer;
  ZmScheduler::Timer	m_electTimer;

  ZmRef<CxnHash>	m_cxns;
};

#endif /* Zdb_HPP */
