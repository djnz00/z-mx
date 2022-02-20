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
#include <zlib/ZuInt.hpp>
#include <zlib/ZuGrow.hpp>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmFn.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmPLock.hpp>
#include <zlib/ZmDRing.hpp>

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

#define ZdbMagic	0x0db3a61c	// file magic number
#define ZdbVersion	1		// file format version

// get(prevRN) returns newly instantiated object that may be superceded
// get(rn) where rn was deleted returns tombstone
// once deleted, predecessors are no longer guaranteed to be available
//   (they are removed at some unspecified time later)

#if defined(ZDEBUG) && !defined(ZdbRep_DEBUG)
#define ZdbRep_DEBUG
#endif

#ifdef ZdbRep_DEBUG
#define ZdbDEBUG(env, e) do { if ((env)->debug()) ZeLOG(Debug, (e)); } while (0)
#else
#define ZdbDEBUG(env, e) ((void)0)
#endif

using ZdbRN = uint64_t;		// record ID
#define ZdbMaxRN (~static_cast<uint64_t>(0))
#define ZdbNullRN (ZuCmp<ZdbRN>::null())

namespace Zdb_ {
  inline constexpr unsigned fileShift()		{ return 14; }
  inline constexpr unsigned fileMask()		{ return 0x7fU; }
  inline constexpr unsigned fileIndices()	{ return 128; }
  inline constexpr unsigned fileRecs()		{ return 16384; }
  inline constexpr unsigned fileRecMask()	{ return 0x3fff; }
  inline constexpr unsigned indexShift()	{ return 7; }
  inline constexpr unsigned indexMask()		{ return 0x7fU; }
  inline constexpr unsigned indexRecs()		{ return 128; }
}

// new file structure with variable-length flatbuffer-format records

// initial 1Kb super-block of 128 offsets, each to index-block
// each index-block is 2Kb index of 128 {record offset, length} tuples
// super-block is immediately followed by first index-block
// each record is {data,trailer}
// LRU write-through cache of files is maintained
// each cached file containes a write-through cache of the superblock
// a global LRU write-through cache of index blocks is maintained

// record data and replication messages are flatbuffers

// database ID is string (rarely used to index, so ok)

#include <zlib/zdbnet_fbs.h>

namespace ZdbNet {

// custom header with an explicitly little-endian uint32 length
#pragma pack(push, 1)
struct Hdr {
  ZuLittleEndian<uint32_t>	length;	// length of body

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

// call following Finish() to push header and detach buffer
template <typename Builder, typename Owner>
inline auto saveHdr(Builder &fbb, Owner *owner) {
  unsigned length = fbb.GetSize();
  new (Zfb::Save::extend(fbb, sizeof(Hdr))) Hdr{length};
  auto buf = fbb.buf();
  buf->owner = owner;
  return buf;
}
template <typename Builder>
inline auto saveHdr(Builder &fbb) {
  return saveHdr(fbb, static_cast<void *>(nullptr));
}
// returns the total length of the message including the header,
// INT_MAX if not enough bytes have been read yet, -1 if corrupt
inline int loadHdr(const uint8_t *data, unsigned length) {
  if (ZuUnlikely(length < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Buf>
inline int verifyHdr(const ZmRef<Buf> &buf, Fn fn) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  int i = fn(hdr, buf);
  if (i < 0) return i;
  return sizeof(Hdr) + i;
}

inline const fbs::Msg *msg(const Hdr *hdr) {
  if (ZuUnlikely(!hdr)) return nullptr;
  auto data = hdr->data();
  if (ZuUnlikely(
	!Zfb::Verifier{data, hdr->length}.VerifyBuffer<fbs::Msg>()))
    return nullptr;
  return Zfb::GetRoot<fbs::Msg>(data);
}
inline const fbs::Msg *msg_(const Hdr *hdr) {
  return Zfb::GetRoot<fbs::Msg>(hdr->data());
}
inline const fbs::Heartbeat *hb(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  switch (static_cast<int>(msg->body_type())) {
    default:			// should never occur
      return nullptr;
    case fbs::Body_HB:
      return static_cast<const fbs::Heartbeat *>(msg->body());
  }
}
inline const fbs::Heartbeat *hb_(const fbs::Msg *msg) {
  return static_cast<const fbs::Heartbeat *>(msg->body());
}
inline bool recovery(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return false;
  return msg->body_type() == fbs::Hdr_Rec;
}
inline bool recovery_(const fbs::Msg *msg) {
  return msg->body_type() == fbs::Hdr_Rec;
}
inline const fbs::Record *record(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  switch (static_cast<int>(msg->body_type())) {
    default:			// should never occur
      return nullptr;
    case fbs::Body_Rep:
    case fbs::Body_Rec:
      return static_cast<const fbs::Record *>(msg->body());
  }
}
inline const fbs::Record *record_(const fbs::Msg *msg) {
  return static_cast<const fbs::Record *>(msg->body());
}
inline const fbs::Gap *gap(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  if (static_cast<int>(msg->body_type()) != fbs::Body_Gap)
    return nullptr;
  return static_cast<const fbs::Gap *>(msg->body());
}
inline const fbs::Gap *gap_(const fbs::Msg *msg) {
  return static_cast<const fbs::Gap *>(msg->body());
}
template <typename T>
inline const T *data(const fbs::Record *record) {
  if (ZuUnlikely(!record)) return nullptr;
  auto data = Zfb::Load::bytes(record->data());
  if (ZuUnlikely(!data)) return nullptr;
  if (ZuUnlikely(
	!Zfb::Verifier{data.data(), data.length()}.VerifyBuffer<T>()))
    return nullptr;
  return Zfb::GetRoot<T>(data.data());
}
template <typename T>
inline const T *data_(const fbs::Record *record) {
  auto data = Zfb::Load::bytes(record->data());
  if (ZuUnlikely(!data)) return nullptr;
  return Zfb::GetRoot<T>(data.data());
}

} // namespace ZdbNet

#if 0
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
UpdateFn -> loads o from fbs
SaveFn -> saves o to fbs

push()
UpdateFn -> (void *) placement new O (passed to ZdbObject ctor, invoked by that to initialize contained T)
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
#endif

class ZdbEnv;				// database environment
class Zdb;				// individual database
class ZdbHost;				// host
struct ZdbAnyObject;			// object wrapper

namespace ZdbNet {
namespace Op {
  ZfbEnumValues(Op, Commit, Append, Delete, Purge);
}
}

namespace Zdb_ {

using namespace ZdbNet;

class Cxn;				// connection

using Offset = ZiFile::Offset;

// file index block cache

struct IndexBlk_ {
#pragma pack(push, 1)
  struct Index {
    uint64_t		offset;
    uint32_t		length;
  };
  struct Blk {
    Index		data[indexRecs()];	// RN -> {offset, length}
  };
#pragma pack(pop)

  IndexBlk_(unsigned id_, unsigned blkOffset_) :
      id{id_}, blkOffset{blkOffset_} {
    // leave blk uninitialized, it most likely will be read from disk
  }
  uint64_t	id = 0;			// (RN>>indexShift())
  uint64_t	offset = 0;		// offset of this index block
  Blk		blk;

  IndexBlk_() = delete;
  IndexBlk_(const IndexBlk_ &) = delete;
  IndexBlk_ &operator =(const IndexBlk_ &) = delete;
  IndexBlk_(IndexBlk_ &&) = delete;
  IndexBlk_ &operator =(IndexBlk_ &&) = delete;
};
struct IndexBlk_IDAxor {
  static auto get(const IndexBlk_ &index) { return index.id; }
};

using IndexBlkLRU =
  ZmList<IndexBlk_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using IndexBlkLRUNode = IndexBlkLRU::Node;

struct IndexBlkHeapID {
  static constexpr const char *id() { return "Zdb.IndexBlk"; }
};
using IndexBlkCache =
  ZmHash<IndexBlkLRUNode,
    ZmHashKey<IndexBlk_IDAxor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<IndexBlkHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;
using IndexBlk = IndexBlkCache::Node;

// file cache

// data file format:
// 0 +16 header (FileHdr) (magic:u32, version:u32, flags:u32, count:u32)
// 16 +1024 superblock (FileSuperBlk) (128 x u64)
// followed by sequences of 1K-sized index block (FileIndexBlk)
//   and records, appended as they are created
namespace FileFlags {
  enum {
    IOError	= 0x00000001,	// I/O error during open
    Clean	= 0x00000002	// closed cleanly
  };
};

#define ZdbCommitted 0xc001da7a	// "cool data"
#define ZdbDeleted   0xdeadda7a	// "dead data"

#pragma pack(push, 1)
using U32LE = ZuLittleEndian<uint32_t>;
using U64LE = ZuLittleEndian<uint64_t>;
using I64LE = ZuLittleEndian<int64_t>;
struct FileHdr {
  U32LE		magic;		// ZdbMagic
  U32LE		version;
  U32LE		flags;		// FileFlags
  U32LE		allocated;	// allocated record count
  U32LE		deleted;	// deleted record count
};
struct FileBitmap {	// bitmap
  U64LE		data[fileRecs()>>6];
};
struct FileSuperBlk {	// super block
  U64LE		data[fileIndices()];
};
struct FileIndex {
  U64LE		offset;
  U32LE		length;
};
struct FileIndexBlk {	// index block
  FileIndex	data[indexRecs()];
};
struct FileRecTrlr {	// record trailer
  U64LE		rn;
  U64LE		prevRN;
  U32LE		magic;	// ZdbCommitted + operation
};
#pragma pack(pop)

class ZdbAPI File_ : public ZiFile {
friend File_IDAxor;

  using Bitmap = ZuBitmap<fileRecs()>;
  struct SuperBlk {
    uint64_t	offset[fileIndices()];	// offsets to index blocks
  };

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  File_(Zdb *db, uint64_t id) : m_db{db}, m_id{id} { }

  uint64_t id() const { return m_id; }

  void alloc(unsigned i) {
    if (!m_bitmap[i]) alloc_(i);
  }
  void alloc_(unsigned i) {
    m_bitmap[i].set();
    ++m_allocated;
  }

  bool del(unsigned i) {
    if (m_bitmap[i]) del_(i);
    return m_deleted >= fileRecs();
  }
  void del_(unsigned i) {
    m_bitmap[i].clr();
    ++m_deleted;
  }

  bool exists(unsigned i) const { return m_bitmap[i]; }

  unsigned allocated() const { return m_allocated; }
  unsigned deleted() const { return m_deleted; }
  unsigned first() const { return m_bitmap.first(); }
  unsigned last() const { return m_bitmap.last(); }

  void checkpoint() { sync(); }

  uint64_t append(unsigned length) {
    uint64_t offset = m_offset;
    m_offset += length;
    return offset;
  }

private:
  void init();
  bool scan();
  bool sync();
  bool sync_();

  Zdb			*m_db = nullptr;
  uint64_t		m_id = 0;	// (RN>>fileShift())
  Lock			m_lock;
    uint32_t		  m_flags = 0;
    unsigned		  m_allocated = 0;
    unsigned		  m_deleted = 0;
    Bitmap		  m_bitmap;
    SuperBlk		  m_superBlk;
    uint64_t		  m_offset = 0;	// append offset
};
struct File_IDAxor {
  static auto get(const File_ &file) { return file.id(); }
};

using FileLRU =
  ZmList<File_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using FileLRUNode = FileLRU::Node;

struct FileHeapID {
  static constexpr const char *id() { return "Zdb.File"; }
};
using FileCache =
  ZmHash<FileLRUNode,
    ZmHashKey<File_IDAxor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<FileHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;
using File = FileCache::Node;

// pinned file/indexBlk + RN offset within index block
class FileRec {
public:
  FileRec() { }
  FileRec(ZmRef<File> file, ZmRef<IndexBlk> indexBlk, unsigned indexOff) :
    m_file{ZuMv(file)}, m_indexBlk{ZuMv(indexBlk)}, m_indexOff{indexOff} { }

  bool operator !() const { return !m_file; }
  ZuOpBool

  File *file() const { return m_file.ptr(); }
  IndexBlk *indexBlk() const { return m_indexBlk.ptr(); }
  unsigned indexOff() const { return m_indexOff; }

  ZdbRN rn() const {
    return ((m_indexBlk->id)<<indexShift()) | m_indexOff;
  }
  const IndexBlk::Blk &index() const {
    return const_cast<FileRec *>(this)->index();
  }
  IndexBlk::Blk &index() { return m_indexBlk->blk.data[m_indexOff]; }

private:
  ZmRef<File>		m_file = nullptr;
  ZmRef<IndexBlk>	m_indexBlk = nullptr;
  unsigned		m_indexOff = 0;
};

struct Buf_HeapID {
  static constexpr const char *id() { return "Buf"; }
};
class ZdbAPI Buf_ : public ZiIOBuf__<ZuGrow(0, 1), Buf_HeapID> {
  using Base = ZiIOBuf__<ZuGrow(0, 1), Buf_HeapID>;

  Buf_() = default;
  Buf_(Zdb *db) : Base(db) { }

  Zdb *db() const {
    return static_cast<Zdb *>(owner);
  }
  const Hdr *hdr() const {
    return reinterpret_cast<const ZdbNet::Hdr *>(data());
  }

friend Cxn;
friend ZdbAnyObject;

  void send(ZiIOContext &);
  void sent(ZiIOContext &);
};
struct Buf_RNAxor {
  static auto get(const Buf_ &buf) {
    using namespace ZdbNet;
    return record_(msg_(buf.hdr()))->rn();
  }
};
using BufCache =
  ZmHash<Buf_,
    ZmHashKey<Buf_RNAxor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<ZuNull,
	    ZmHashID<Buf_HeapID,
	      ZmHashLock<ZmNoLock> > > > > > >;
using Buf = BufCache::Node;

using LRU =
  ZmList<ZmObject,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using LRUNode = LRU::Node;

struct LRUNode_RNAxor {
  inline static ZdbRN get(const LRUNode &object);
};

struct Cache_ID {
  static constexpr const char *id() { return "Zdb.Cache"; }
};

using Cache =
  ZmHash<LRUNode,
    ZmHashKey<LRUNode_RNAxor,
      ZmHashObject<ZmPolymorph,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<ZuNull,
	    ZmHashID<Cache_ID,
	      ZmHashLock<ZmNoLock> > > > > > >;
using CacheNode = Cache::Node;

} // Zdb_

class ZdbAPI ZdbAnyObject : public Zdb_::CacheNode, public ZuPrintable {
  ZdbAnyObject() = delete;
  ZdbAnyObject(const ZdbAnyObject &) = delete;
  ZdbAnyObject &operator =(const ZdbAnyObject &) = delete;
  ZdbAnyObject(ZdbAnyObject &&) = delete;
  ZdbAnyObject &operator =(ZdbAnyObject &&) = delete;

  friend Zdb;

public:
  ZdbAnyObject(Zdb *db, ZdbRN rn) : m_db{db}, m_rn{rn} { }
  virtual ~ZdbAnyObject() { }

  Zdb *db() const { return m_db; }
  ZdbRN rn() const { return m_rn; }
  ZdbRN prevRN() const { return m_prevRN; }
  ZdbRN origRN() const { return m_origRN; }
  bool appended() const { return m_appended; }
  bool deleted() const { return m_deleted; }

  ZmRef<Buf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<ZdbAnyObject *>(this)->ptr_(); }

  void append() { m_appended = true; }
  void del() { m_deleted = true; }

  template <typename S> void print(S &s) const {
    s << "rn=" << rn << " prevRN=" << prevRN << " origRN=" << origRN <<
      "appended=" << static_cast<unsigned>(appended) <<
      "deleted=" << static_cast<unsigned>(deleted);
  }

private:
  void prevRN(ZdbRN rn) { m_prevRN = rn; }
  void rn(ZdbRN rn_) { m_rn = rn_; }

  Zdb		*m_db;
  ZdbRN		m_rn;
  ZdbRN		m_prevRN = ZdbNullRN;
  ZdbRN		m_origRN = ZdbNullRN;	// RN backup before put() / putUpdate()
  bool		m_appended = false;
  bool		m_deleted = false;
};

namespace Zdb_ {
inline ZdbRN LRUNode_RNAxor::get(const LRUNode &node)
{
  return static_cast<const ZdbAnyObject &>(node).rn();
}
}

struct ZdbObject_HeapID { 
  static constexpr const char *id() { return "Zdb.Object"; }
};
template <typename T, typename Heap>
class ZdbObject_ : public Heap, public ZdbAnyObject {
  ZdbObject_() = delete;
  ZdbObject_(const ZdbObject_ &) = delete;
  ZdbObject_ &operator =(const ZdbObject_ &) = delete;
  ZdbObject_(ZdbObject_ &&) = delete;
  ZdbObject_ &operator =(ZdbObject_ &&) = delete;

public:
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

// CtorFn(db, rn) - construct new object from flatbuffer
typedef ZdbAnyObject *(*ZdbCtorFn)(Zdb *, ZdbRN);
// LoadFn(db, rn, data, length) - construct new object from flatbuffer
typedef ZdbAnyObject *(*ZdbLoadFn)(Zdb *, ZdbRN, const uint8_t *, unsigned);
// UpdateFn(object, data, length) - load flatbuffer into object
typedef ZdbAnyObject *(*ZdbUpdateFn)(ZdbAnyObject *, const uint8_t *, unsigned);
// SaveFn(fbb, ptr) - save *ptr into flatbuffer fbb
typedef void (*ZdbSaveFn)(Zfb::Builder &, const void *);
// RecoverFn(object, buf) - object recovered (added, updated, deleted)
typedef void (*ZdbRecoverFn)(ZdbAnyObject *, const Buf *buf);

struct ZdbHandler {
  ZdbCtorFn		ctorFn =
    [](Zdb *db, ZdbRN rn) ->
	ZdbAnyObject * { return new ZdbAnyObject{db, rn}; };
  ZdbLoadFn		loadFn =
    [](Zdb *db, ZdbRN rn, const uint8_t *, unsigned) ->
	ZdbAnyObject * { return new ZdbAnyObject{db, rn}; };
  ZdbUpdateFn		updateFn = nullptr;
  ZdbSaveFn		saveFn = nullptr;
  ZdbRecoverFn		recoverFn = nullptr;

  template <typename T>
  static ZdbHandler bind() {
    return ZdbHandler{
      .ctorFn = [](Zdb *db, ZdbRN rn) -> ZdbAnyObject * {
	return new ZdbObject<T>{db, rn, [](void *ptr) { new (ptr) T{}; }};
      },
      .loadFn = [](Zdb *db, ZdbRN rn, const uint8_t *data, unsigned len) ->
	  ZdbAnyObject * {
	auto fbObject = Zfb::verify<T>(data, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	return new ZdbObject<T>{db, rn, [fbObject](void *ptr) {
	  Zfb::ctor<T>(ptr, fbObject);
	}};
      },
      .updateFn =
	[](ZdbAnyObject *object, const uint8_t *data, unsigned len) ->
	  ZdbAnyObject * {
	auto fbObject = Zfb::verify<T>(data, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	auto &data = static_cast<ZdbObject<T> *>(object)->data();
	Zfb::load<T>(data, fbObject);
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
  int			cacheMode = ZdbCacheMode::Normal;
  bool			warmUp = false;	// pre-write initial DB file
  uint8_t		repMode = 0;	// 0 - deferred, 1 - in put()

  ZdbCf() = default;
  ZdbCf(ZuString id_) : id{id_} { }
  ZdbCf(ZuString id_, const ZvCf *cf) : id{id_} {
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", false, ZdbCacheMode::Normal);
    warmUp = cf->getInt("warmUp", 0, 1, false, 0);
    repMode = cf->getInt("repMode", 0, 1, false, 0);
  }

  struct IDAxor { static ZuID get(const ZdbCf &cf) { return cf.id; } };
};

namespace Zdb_ {

struct Cfs_HeapID {
  static constexpr const char *id() { return "ZdbEnv.DBCfs"; }
};
using Cfs =
  ZmRBTree<ZdbCf,
    ZmRBTreeKey<ZdbCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZdbCfs_HeapID> > > >;

struct Deletes_HeapID {
  static constexpr const char *id() { return "Zdb.Deletes"; }
};
struct DeleteOp {
  ZdbRN	rn;
  int	op;	// Op
};
using Deletes =
  ZmRBTreeKV<ZdbRN, DeleteOp,
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<Deletes_HeapID> > >;

} // Zdb_

class ZdbAPI Zdb : public ZmPolymorph {
friend ZdbEnv;
friend ZdbAnyObject;

  using Cache = Zdb_::Cache;
  using BufCache = Zdb_::BufCache;
  using Deletes = Zdb_::Deletes;
  using File = Zdb_::File;
  using FileRec = Zdb_::FileRec;
  using FileLRU = Zdb_::FileLRU;
  using FileCache = Zdb_::FileCache;
  using IndexBlkCache = Zdb_::IndexBlkCache;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

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
  unsigned fileCacheSize() { return m_fileCacheSize; }
  unsigned indexBlkCacheSize() { return m_indexBlkCacheSize; }

  // first RN that is committed (will be ZdbMaxRN if DB is empty)
  ZdbRN minRN() { ReadGuard guard(m_lock); return m_minRN; }
  // next RN that will be allocated
  ZdbRN nextRN() { ReadGuard guard(m_lock); return m_nextRN; }

  // create new placeholder record (null RN, in-memory only, never in DB)
  ZmRef<ZdbAnyObject> placeholder();

  // create new record
  ZmRef<ZdbAnyObject> push();
  // create new record (idempotent)
  ZmRef<ZdbAnyObject> push(ZdbRN rn);
  // allocate RN only for new record, for later use with push(rn)
  ZdbRN pushRN();
  // commit push - causes replication / write
  void put(ZdbAnyObject *);

  // get record
  ZmRef<ZdbAnyObject> get(ZdbRN rn);	// use for read-only queries
  ZmRef<ZdbAnyObject> get_(ZdbRN rn);	// use for RMW - does not update cache

  // update record
  void update(ZdbAnyObject *object);
  // update record (idempotent) - returns true if update should proceed
  bool update(ZdbAnyObject *object, ZdbRN rn);
  // update record (with prevRN, without object)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN);
  // update record (idempotent) (with prevRN, without prev POD)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN, ZdbRN rn);
  // commit update
  void putUpdate(ZdbAnyObject *);

  // abort push() / update()
  void abort(ZdbAnyObject *);

  using Telemetry = ZvTelemetry::DB;

  void telemetry(Telemetry &data) const;

private:
  // push initial record
  ZmRef<ZdbAnyObject> push_();
  // idempotent push
  ZmRef<ZdbAnyObject> push_(ZdbRN rn);

  // low-level get, does not filter deleted records
  ZmRef<ZdbAnyObject> get__(ZdbRN rn);
  // low-level existence check
  bool exists(ZdbRN rn);

  // load object from buffer
  ZmRef<ZdbAnyObject> load(const fbs::Record *fbRecord);
  // save object to buffer
  void save(Zfb::Builder &fbb, ZdbAnyObject *object);

  // replication data rcvd (copy/commit, called while env is unlocked)
  void replicated(const ZmRef<Buf> &buf, const fbs::Record *fbRecord);

  // forward replication data
  ZmRef<Buf> replicateFwd(const ZmRef<Buf> &buf);
  // prepare recovery data for sending
  ZmRef<Buf> recovery(ZdbRN rn);
  ZmRef<Buf> gap(ZdbRN rn, uint64_t count);

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

  FileRec rn2file(ZdbRN rn, bool write);

  ZmRef<File> getFile(unsigned i, bool create);
  ZmRef<File> openFile(unsigned i, bool create);
  void delFile(File *file);
  void recover(File *file);
  bool recover(const FileRec &rec);
  void scan(File *file);

  ZmRef<ZdbAnyObject> read_(const FileRec &);

  void write2(ZmRef<Buf> buf);
  void write_(const Buf *buf);
  bool ack(ZdbRN rn);
  void vacuum();
  void standalone();
  void del_(ZdbRN rn);
  ZdbRN del__(ZdbRN rn);

  void fileRdError_(File *, uint64_t, int, ZeError e);
  void fileWrError_(File *, uint64_t, ZeError e);

  void cache(ZdbAnyObject *object);
  void cache_(ZdbAnyObject *object);
  ZmRef<ZdbAnyObject> cacheDel_(ZdbRN rn);
  void cacheDel_(ZdbAnyObject *object);

  ZdbEnv		*m_env;
  const ZdbCf		*m_cf = nullptr;
  ZuID			m_id = 0;
  ZdbHandler		m_handler;
  Lock			m_lock;
    ZdbRN		  m_minRN = ZdbMaxRN;
    ZdbRN		  m_nextRN = 0;
    LRU		  	  m_lru;
    ZmRef<Cache>	  m_cache;
    unsigned		  m_cacheSize = 0;
    uint64_t		  m_cacheLoads = 0;
    uint64_t		  m_cacheMisses = 0;
    ZmRef<BufCache>	  m_bufCache;
    Deletes		  m_deletes;
    ZdbRN		  m_vacuumRN = ZdbNullRN;
    FileLRU		  m_fileLRU;
    ZmRef<FileCache>	  m_files;
    unsigned		  m_fileCacheSize = 0;
    uint64_t		  m_lastFile = 0;
    uint64_t		  m_lastIndexBlk = 0;
    uint64_t		  m_fileLoads = 0;
    uint64_t		  m_fileMisses = 0;
    IndexBlkLRU	 	  m_indexBlkLRU;
    ZmRef<IndexBlkCache>  m_indexBlks;
    unsigned		  m_indexBlkCacheSize = 0;
    uint64_t		  m_indexBlkLoads = 0;
    uint64_t		  m_indexBlkMisses = 0;
};
struct Zdbs_HeapID {
  static constexpr const char *id() { return "ZdbEnv.DBs"; }
};
using Zdbs =
  ZmRBTree<Zdb,
    ZmRBTreeKey<Zdb::IDAxor,
      ZmRBTreeNodeDerive<true,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<Zdbs_HeapID> > > > >;

namespace Zdb_ {

using DBState_ = ZmLHashKV<ZuID, ZdbRN, ZmLHashLocal<>>;
struct DBState : public DBState_ {
  DBState(unsigned size) : DBState_{ZmHashParams{size}} { }

  DBState(Zfb::Vector<const ZdbNet::fbs::DBState *> *envState) :
      DBState_{ZmHashParams{envState->size()}} {
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

  bool update(ZuID id, ZdbRN rn) {
    auto node = find(id);
    if (!node) {
      add(id, rn);
      return true;
    }
    auto &thisRN = const_cast<Node *>(node)->val();
    if (thisRN < rn) {
      thisRN = rn;
      return true;
    }
    return false;
  }
  DBState &operator |=(const DBState &r) {
    if (ZuLikely(this != &r)) {
      auto i = r.readIterator();
      while (auto rnode = i.iterate()) update(rnode->key(), rnode->val());
    }
    return *this;
  }
  DBState &operator =(const DBState &r) {
    if (ZuLikely(this != &r)) {
      clean();
      this->operator |=(r);
    }
    return *this;
  }

  int cmp(const DBState &r) {
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
struct DBState_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, const DBState &a) {
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
DBState_Print ZuPrintType(DBState *);

} // Zdb_

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

class ZdbAPI ZdbHost_ {
friend ZdbEnv;

  using Cxn = Zdb_::Cxn;
friend Cxn;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  ZdbHost_(ZdbEnv *env, const ZdbHostCf *config);

public:
  struct IDAxor { static ZuID get(const ZdbHost_ &h) { return h.id(); } };

  const ZdbHostCf &config() const { return *m_cf; }

  ZuID id() const { return m_cf->id; }
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
  friend ZuPrintFn ZuPrintType(ZdbHost_ *);

private:
  ZmRef<Cxn> cxn() const { return m_cxn; }

  void state(int s) { m_state = s; }

  const DBState &dbState() const { return m_dbState; }
  DBState &dbState() { return m_dbState; }

  bool active() const {
    using namespace ZdbHostState;
    switch (m_state) {
      case Activating:
      case Active:
	return true;
    }
    return false;
  }

  int cmp(const ZdbHost_ *host) const {
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
  void associate(Cxn *cxn);
  void disconnected();

  void reactivate();

  ZdbEnv		*m_env;
  const ZdbHostCf	*m_cf;
  ZiMultiplex		*m_mx;

  Lock			m_lock;
    ZmRef<Cxn>		  m_cxn;

  ZmScheduler::Timer	m_connectTimer;

  // guarded by ZdbEnv

  int			m_state = ZdbHostState::Instantiated;
  DBState		m_dbState;
  bool			m_voted = false;
};
struct ZdbHosts_HeapID {
  static constexpr const char *id() { return "ZdbEnv.Hosts"; }
};
using ZdbHosts =
  ZmRBTree<ZdbHost_,
    ZmRBTreeKey<ZdbHost_::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeNodeDerive<true,
	  ZmRBTreeHeapID<ZdbHosts_HeapID> > > > >;
using ZdbHost = ZdbHosts::Node;

#if 0
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
#endif

namespace Zdb_ {

class Cxn : public ZiConnection {
  Cxn(const Cxn &);
  Cxn &operator =(const Cxn &);	// prevent mis-use

friend ZdbEnv;
friend ZdbHost;

  Cxn(ZdbEnv *env, ZdbHost *host, const ZiCxnInfo &ci);

  ZdbEnv *env() const { return m_env; }
  void host(ZdbHost *host) { m_host = host; }
  ZdbHost *host() const { return m_host; }

  void connected(ZiIOContext &);
  void disconnected();

  void msgRead(ZiIOContext &);
  bool msgRcvd(ZiIOContext &);

  bool hbRcvd(ZiIOContext &);
  void hbTimeout();
  void hbSend();

  template <typename Body>
  Zdb *repRcvd_(const Body *body) {
    if (!m_host) {
      ZeLOG(Fatal, "Zdb received replication message before heartbeat");
      return nullptr;
    }
    auto dbID = Zfb::Load::id(body->db())
    Zdb *db = m_env->db_(dbID, ZdbHandler{});
    if (!db) {
      ZeLOG(Fatal, ZtString{} <<
	  "Zdb unknown remote DBID " << dbID << " received");
      return nullptr;
    }
    return db;
  }
  bool repRcvd(Zdb *db, const ZmRef<ZdbBuf> &buf, const fbs::Record *record);
  bool repRcvd(Zdb *db, const ZmRef<ZdbBuf> &buf, const fbs::Gap *gap);

  void repSend(ZmRef<Buf> buf);

  void ackRcvd();
  void ackSend(int type, ZdbAnyObject *o);

  ZdbEnv		*m_env;
  ZdbHost		*m_host;	// 0 if not yet associated

  ZmRef<Buf>		m_rxBuf;

  ZmScheduler::Timer	m_hbTimer;
};

} // Zdb_

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
friend ZdbAnyObject;

  using Cxn = Zdb_::Cxn;
friend Zdb_::Cxn;

  struct CxnHash_HeapID {
    static constexpr const char *id() { return "ZdbEnv.CxnHash"; }
  };
  using CxnHash =
    ZmHash<ZmRef<Cxn>,
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
  ZdbHost *host(ZuID id) const { return m_hosts.find(id); }
  template <typename L> bool allHosts(L l) const {
    auto i = m_hosts.readIterator();
    while (auto node = i.iterate())
      if (!l(node)) return false;
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
  void connected(Cxn *cxn);
  void associate(Cxn *cxn, int hostID);
  void associate(Cxn *cxn, ZdbHost *host);
  void disconnected(Cxn *cxn);

  void hbRcvd(ZdbHost *host, const ZdbNet::fbs::Msg_HB *hb);
  void vote(ZdbHost *host);

  void hbStart();
  void hbSend();		// send heartbeat and reschedule self
  void hbSend_(Cxn *cxn);	// send heartbeat (once)

  void dbStateRefresh();	// refresh m_self->dbState()

  ZdbHost *setMaster();	// returns old master
  void setNext(ZdbHost *host);
  void setNext();

  void startReplication();
  void stopReplication();

  void replicated(ZdbHost *host, const ZdbNet::fbs::Record *record);

  void repSend(ZmRef<Buf> buf);
  void recSend();

  void ackRcvd(ZdbHost *host, bool positive, ZuID db, ZdbRN rn);

  void write(ZmRef<Buf> buf);

  // write thread
  void standalone();
  void replicating();
  bool isStandalone() { return m_standalone; }

  ZdbEnvCf		m_cf;
  ZiMultiplex		*m_mx;

  ZmFn<>		m_activeFn;
  ZmFn<>		m_inactiveFn;

  Lock			m_lock;
    StateCond		  m_stateCond;
    bool		  m_appActive;
    ZdbHost		  *m_self;
    ZdbHost		  *m_master;	// == m_self if Active
    ZdbHost		  *m_prev;	// previous-ranked host
    ZdbHost		  *m_next;	// next-ranked host
    ZmRef<Cxn>		  m_nextCxn;	// replica peer's cxn
    bool		  m_recovering;	// recovering next-ranked host
    DBState		  m_recover;	// recovery state
    DBState		  m_recoverEnd;	// recovery end
    int			  m_nPeers;	// # up to date peers
					// # votes received (Electing)
					// # pending disconnects (Stopping)
    ZmTime		  m_hbSendTime;
    Zdbs		  m_dbs;
    ZdbHosts		  m_hosts;

  // write thread
  bool			m_standalone = false;

  ZmScheduler::Timer	m_hbSendTimer;
  ZmScheduler::Timer	m_electTimer;

  ZmRef<CxnHash>	m_cxns;
};

#endif /* Zdb_HPP */
