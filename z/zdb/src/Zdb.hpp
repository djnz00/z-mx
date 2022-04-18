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
#include <zlib/ZuBitmap.hpp>

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

#define ZdbMaxDelBatch	1000		// maximum #deletions in a single batch

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

#define ZdbDeleted (~static_cast<uint64_t>(0))	// offset sentinel

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

#include <zlib/zdb__fbs.h>

namespace Zdb_ {

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
template <typename Buf>
inline int loadHdr(const Buf *buf) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Buf, typename Fn>
inline int verifyHdr(const Buf *buf, Fn fn) {
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
	(!Zfb::Verifier{data, hdr->length}.VerifyBuffer<fbs::Msg>())))
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
  return msg->body_type() == fbs::Body_Rec;
}
inline bool recovery_(const fbs::Msg *msg) {
  return msg->body_type() == fbs::Body_Rec;
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
  if (ZuUnlikely((
	!Zfb::Verifier{data.data(), data.length()}.VerifyBuffer<T>())))
    return nullptr;
  return Zfb::GetRoot<T>(data.data());
}
template <typename T>
inline const T *data_(const fbs::Record *record) {
  auto data = Zfb::Load::bytes(record->data());
  if (ZuUnlikely(!data)) return nullptr;
  return Zfb::GetRoot<T>(data.data());
}

} // namespace Zdb_

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
class ZdbAnyObject;			// object wrapper

namespace Zdb_ {

using Magic = uint32_t;

namespace Op {
  ZtEnumValues("Zdb_::Op",
    Put = 0,	// add this record, delete prevRN (and predecessors)
    Append,	// add this record, preserve prevRN
    Delete,	// add this tombstone, delete prevRN (no data)
    Purge	// add this purge instruction, delete all < prevRN (no data)
  );
}

using SeqLen = uint32_t;

namespace SeqLenOp {
  inline SeqLen mk(SeqLen length, SeqLen op) {
    return (length<<2) | (op & 3);
  }
  inline SeqLen seqLen(SeqLen seqLenOp) { return seqLenOp>>2; }
  inline SeqLen op(SeqLen seqLenOp) { return seqLenOp & 3; }
  inline constexpr SeqLen maxSeqLen() { return 0x3fffffffU; }
}

class Cxn;				// connection

// using Offset = ZiFile::Offset;

// file index block cache

using IndexBlkLRU =
  ZmList<ZmObject,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;

struct IndexBlk_IDAxor {
  static uint64_t get(const ZmObject &index);
};
struct IndexBlkHeapID {
  static constexpr const char *id() { return "Zdb.IndexBlk"; }
};
using IndexBlkCache =
  ZmHash<IndexBlkLRU::Node,
    ZmHashKey<IndexBlk_IDAxor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<IndexBlkHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;

struct IndexBlk : public IndexBlkCache::Node {
#pragma pack(push, 1)
  struct Index {
    uint64_t		offset;
    uint32_t		length;
  };
  struct Blk {
    Index		data[indexRecs()];	// RN -> {offset, length}
  };
#pragma pack(pop)

  IndexBlk(uint64_t id_, uint64_t offset_) : id{id_}, offset{offset_} {
    // leave blk uninitialized, it most likely will be read from disk
  }
  uint64_t	id = 0;			// (RN>>indexShift())
  uint64_t	offset = 0;		// offset of this index block
  Blk		blk;

  IndexBlk() = delete;
  IndexBlk(const IndexBlk &) = delete;
  IndexBlk &operator =(const IndexBlk &) = delete;
  IndexBlk(IndexBlk &&) = delete;
  IndexBlk &operator =(IndexBlk &&) = delete;
};
inline uint64_t IndexBlk_IDAxor::get(const ZmObject &index)
{
  return static_cast<const IndexBlk &>(index).id;
}

// file cache

// data file format:
// 0 +16 header (FileHdr) (magic:u32, version:u32, flags:u32, count:u32)
// 16 +1024 superblock (FileSuperBlk) (128 x u64)
// followed by sequences of 1K-sized index block (FileIndexBlk)
//   and records, appended as they are created
namespace FileFlags {
  enum {
    IOError	= 0x00000001,	// I/O error during open
    Clean	= 0x00000002,	// closed cleanly
    Append	= 0x00000004
  };
};

#define ZdbCommitted 0xc001da7a	// "cool data"

#pragma pack(push, 1)
using U32LE = ZuLittleEndian<uint32_t>;
using U64LE = ZuLittleEndian<uint64_t>;
using I64LE = ZuLittleEndian<int64_t>;

using RNLE = ZuLittleEndian<ZdbRN>;
using MagicLE = ZuLittleEndian<Magic>;
using SeqLenLE = ZuLittleEndian<SeqLen>;

struct FileHdr {
  MagicLE	magic;		// ZdbMagic
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
  RNLE		rn;
  RNLE		prevRN;
  SeqLenLE	seqLenOp;// chain length + op
  MagicLE	magic;	// ZdbCommitted
};
#pragma pack(pop)

using FileLRU =
  ZmList<ZmPolymorph,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;

struct File_IDAxor {
  static uint64_t get(const ZmPolymorph &file);
};
struct FileHeapID {
  static constexpr const char *id() { return "Zdb.File"; }
};
using FileCache =
  ZmHash<FileLRU::Node,
    ZmHashKey<File_IDAxor,
      ZmHashObject<ZmPolymorph,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<FileHeapID,
	    ZmHashLock<ZmNoLock> > > > > >;

class ZdbAPI File : public FileCache::Node, public ZiFile {
  using Bitmap = ZuBitmap<fileRecs()>;
  struct SuperBlk {
    uint64_t	data[fileIndices()];	// offsets to index blocks
  };

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

friend Zdb;

  File(Zdb *db, uint64_t id) : m_db{db}, m_id{id} { }

public:
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

  uint64_t append(unsigned length) {
    uint64_t offset = m_offset;
    m_offset += length;
    return offset;
  }

  ZmRef<IndexBlk> readIndexBlk(uint64_t id);
  ZmRef<IndexBlk> writeIndexBlk(uint64_t id);

  bool readIndexBlk_(IndexBlk *);
  bool writeIndexBlk_(IndexBlk *);

  bool scan();
  bool sync();
  bool sync_();

private:
  void reset();

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
inline uint64_t File_IDAxor::get(const ZmPolymorph &file)
{
  return static_cast<const File &>(file).id();
}

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
  const IndexBlk::Index &index() const {
    return const_cast<FileRec *>(this)->index();
  }
  IndexBlk::Index &index() { return m_indexBlk->blk.data[m_indexOff]; }

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

public:
  Buf_() = default;
  Buf_(Zdb *db) : Base(db) { }

  Zdb *db() const {
    return static_cast<Zdb *>(owner);
  }
  const Hdr *hdr() const {
    return reinterpret_cast<const Zdb_::Hdr *>(data());
  }

friend Cxn;
friend ZdbAnyObject;

  void send(ZiIOContext &);
  void sent(ZiIOContext &);
};
struct Buf_RNAxor {
  static auto get(const Buf_ &buf) {
    using namespace Zdb_;
    return record_(msg_(buf.hdr()))->rn();
  }
};
using BufCache =
  ZmHash<Buf_,
    ZmHashKey<Buf_RNAxor,
      ZmHashObject<ZmPolymorph,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<ZuNull,
	    ZmHashID<Buf_HeapID,
	      ZmHashLock<ZmNoLock> > > > > > >;
struct Buf : public BufCache::Node {
  template <typename ...Args>
  Buf(Args &&... args) : BufCache::Node{ZuFwd<Args>(args)...} { }

  using Buf_::data;

  Buf() = default;
  Buf(const Buf &) = default;
  Buf &operator =(const Buf &) = default;
  Buf(Buf &&) = default;
  Buf &operator =(Buf &&) = default;
  ~Buf() = default;
};

using LRU =
  ZmList<ZmPolymorph,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using LRUNode = LRU::Node;

struct LRUNode_RNAxor {
  static ZdbRN get(const LRUNode &object);
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

  using Buf = Zdb_::Buf;
  using SeqLen = Zdb_::SeqLen;

public:
  ZdbAnyObject(Zdb *db) : m_db{db} { }
  virtual ~ZdbAnyObject() { }

  Zdb *db() const { return m_db; }
  ZdbRN rn() const { return m_rn; }
  ZdbRN prevRN() const { return m_prevRN; }
  ZdbRN origRN() const { return m_origRN; }
  SeqLen seqLen() const { return m_seqLen; }
  bool deleted() const { return m_deleted; }

  ZmRef<Buf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<ZdbAnyObject *>(this)->ptr_(); }

  void del() { m_deleted = true; }
  void undel() { m_deleted = false; }

  template <typename S> void print(S &s) const {
    s << "rn=" << m_rn << " prevRN=" << m_prevRN << " origRN=" << m_origRN <<
      " seqLen=" << m_seqLen <<
      "deleted=" << static_cast<unsigned>(m_deleted);
  }

private:
  void init(ZdbRN rn) { m_rn = rn; }
  void load(ZdbRN rn, ZdbRN prevRN, SeqLen seqLenOp) {
    using namespace Zdb_;
    m_rn = rn;
    m_prevRN = prevRN;
    m_seqLen = SeqLenOp::seqLen(seqLenOp);
  }

  void pushRN(ZdbRN rn) { m_origRN = m_rn; m_rn = rn; }
  void putRN() { m_prevRN = m_origRN; m_origRN = ZdbNullRN; ++m_seqLen; }
  void appendRN() { m_prevRN = m_origRN; m_origRN = ZdbNullRN; ++m_seqLen; }
  void abortRN() { m_rn = m_origRN; m_origRN = ZdbNullRN; }

  Zdb		*m_db;
  ZdbRN		m_rn = ZdbNullRN;
  ZdbRN		m_prevRN = ZdbNullRN;
  ZdbRN		m_origRN = ZdbNullRN;	// RN backup before put()
  SeqLen	m_seqLen = 0;
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
  ZdbObject_(Zdb *db_, L l) : ZdbAnyObject{db_} {
    l(static_cast<void *>(&m_data[0]));
  }

  void *ptr_() { return &m_data[0]; }
  const void *ptr_() const { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~ZdbObject_() { ptr()->~T(); }

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

// CtorFn(db) - construct new object from flatbuffer
typedef ZdbAnyObject *(*ZdbCtorFn)(Zdb *);
// LoadFn(db, data, length) - construct new object from flatbuffer
typedef ZdbAnyObject *(*ZdbLoadFn)(Zdb *, const uint8_t *, unsigned);
// UpdateFn(object, data, length) - load flatbuffer into object
typedef ZdbAnyObject *(*ZdbUpdateFn)(ZdbAnyObject *, const uint8_t *, unsigned);
// SaveFn(fbb, ptr) - save *ptr into flatbuffer fbb
typedef void (*ZdbSaveFn)(Zfb::Builder &, const void *);
// RecoverFn(object, buf) - object recovered (added, updated, deleted)
typedef void (*ZdbRecoverFn)(ZdbAnyObject *, const Zdb_::Buf *buf);

struct ZdbHandler {
  ZdbCtorFn		ctorFn =
    [](Zdb *db) -> ZdbAnyObject * { return new ZdbAnyObject{db}; };
  ZdbLoadFn		loadFn =
    [](Zdb *db, const uint8_t *, unsigned) ->
	ZdbAnyObject * { return new ZdbAnyObject{db}; };
  ZdbUpdateFn		updateFn = nullptr;
  ZdbSaveFn		saveFn = nullptr;
  ZdbRecoverFn		recoverFn = nullptr;

  template <typename T>
  static ZdbHandler bind() {
    return ZdbHandler{
      .ctorFn = [](Zdb *db) -> ZdbAnyObject * {
	return new ZdbObject<T>{db, [](void *ptr) { new (ptr) T{}; }};
      },
      .loadFn = [](Zdb *db, const uint8_t *data, unsigned len) ->
	  ZdbAnyObject * {
	auto fbObject = ZfbField::verify<T>(data, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	return new ZdbObject<T>{db, [fbObject](void *ptr) {
	  ZfbField::ctor<T>(ptr, fbObject);
	}};
      },
      .updateFn =
	[](ZdbAnyObject *object, const uint8_t *data_, unsigned len) ->
	  ZdbAnyObject * {
	auto fbObject = ZfbField::verify<T>(data_, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	auto &data = static_cast<ZdbObject<T> *>(object)->data();
	ZfbField::load<T>(data, fbObject);
	return object;
      },
      .saveFn = [](Zfb::Builder &fbb, const void *ptr) {
	ZfbField::save<T>(fbb, *reinterpret_cast<const T *>(ptr));
      }
    };
  }
};

namespace ZdbCacheMode {
  using namespace ZvTelemetry::DBCacheMode;
}

// FIXME - append should be compile-time fixed per-db, not configured
struct ZdbCf {
  ZuID			id;
  int			cacheMode = ZdbCacheMode::Normal;
  bool			append = false;	// append records to chain
  bool			warmUp = false;	// pre-write initial DB file
  uint8_t		repMode = 0;	// 0 - deferred, 1 - in put()

  ZdbCf() = default;
  ZdbCf(ZuString id_) : id{id_} { }
  ZdbCf(ZuString id_, const ZvCf *cf) : id{id_} {
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", false, ZdbCacheMode::Normal);
    append = cf->getInt("append", 0, 1, false, 0);
    warmUp = cf->getInt("warmUp", 0, 1, false, 0);
    repMode = cf->getInt("repMode", 0, 1, false, 0);
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

namespace Zdb_ {

struct Deletes_HeapID {
  static constexpr const char *id() { return "Zdb.Deletes"; }
};
struct DeleteOp {
  ZdbRN		rn;
  SeqLen	seqLenOp;
};
using Deletes =
  ZmRBTreeKV<ZdbRN, DeleteOp,
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<Deletes_HeapID> > >;

} // Zdb_

class ZdbAPI Zdb : public ZmPolymorph {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using SeqLen = Zdb_::SeqLen;
  using Cxn = Zdb_::Cxn;
  using Cache = Zdb_::Cache;
  using BufCache = Zdb_::BufCache;
  using Buf = Zdb_::Buf;
  using LRU = Zdb_::LRU;
  using Deletes = Zdb_::Deletes;
  using File = Zdb_::File;
  using FileRec = Zdb_::FileRec;
  using FileLRU = Zdb_::FileLRU;
  using FileCache = Zdb_::FileCache;
  using IndexBlk = Zdb_::IndexBlk;
  using IndexBlkLRU = Zdb_::IndexBlkLRU;
  using IndexBlkCache = Zdb_::IndexBlkCache;

friend Cxn;
friend File;
friend ZdbAnyObject;
friend ZdbEnv;

protected:
  Zdb(ZdbEnv *env, ZdbCf *cf);

public:
  ~Zdb();

private:
  void init(ZdbHandler);
  void final();

  bool open();
  void close();

  bool recover();
  void checkpoint();
  bool checkpoint_();

public:
  struct IDAxor {
    static ZuID get(const Zdb &db) { return db.m_id; }
  };

  const ZdbCf &config() const { return *m_cf; }

  ZdbEnv *env() const { return m_env; }
  ZuID id() const { return m_id; }
  unsigned cacheSize() const { return m_cacheSize; } // unclean read
  unsigned fileCacheSize() const { return m_fileCacheSize; }
  unsigned indexBlkCacheSize() const { return m_indexBlkCacheSize; }

  // first RN that is committed (will be ZdbMaxRN if DB is empty)
  ZdbRN minRN() const { ReadGuard guard(m_lock); return m_minRN; }
  // next RN that will be allocated
  ZdbRN nextRN() const { ReadGuard guard(m_lock); return m_nextRN; }

  // create new placeholder record (null RN, in-memory only, never in DB)
  ZmRef<ZdbAnyObject> placeholder();

  // create new record
  ZmRef<ZdbAnyObject> push();
  // create new record (idempotent)
  ZmRef<ZdbAnyObject> push(ZdbRN rn);

  // get record
  ZmRef<ZdbAnyObject> get(ZdbRN rn);	// use for read-only queries
  ZmRef<ZdbAnyObject> get_(ZdbRN rn);	// use for RMW - does not update cache

  // update record
  bool update(ZdbAnyObject *object);
  // update record (idempotent) - returns true if update should proceed
  bool update(ZdbAnyObject *object, ZdbRN rn);
  // update record (with prevRN, without object)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN);
  // update record (idempotent) (with prevRN, without prev POD)
  ZmRef<ZdbAnyObject> update_(ZdbRN prevRN, ZdbRN rn);

  // commit push() /update() - causes replication / write
  void put(ZdbAnyObject *);
  // abort push() / update()
  void abort(ZdbAnyObject *);

  // purge all records < minRN
  void purge(ZdbRN minRN);

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
  ZmRef<ZdbAnyObject> load(const Zdb_::fbs::Record *record);
  // save object to buffer
  void save(Zfb::Builder &fbb, ZdbAnyObject *object);

  // replication data rcvd (copy/commit, called while env is unlocked)
  void replicated(const Zdb_::fbs::Record *record, const Buf *buf);
  void replicated(const Zdb_::fbs::Gap *gap, const Buf *buf);

  // forward replication data
  ZmRef<Buf> replicateFwd(const Buf *buf);
  // prepare recovery data for sending
  ZmRef<Buf> recovery(ZdbRN rn);
  ZmRef<Buf> gap(ZdbRN rn, uint64_t count);

  ZiFile::Path dirName(uint64_t id) const;
  ZiFile::Path fileName(ZiFile::Path dir, uint64_t id) const {
    return ZiFile::append(dir, ZuStringN<12>() <<
	ZuBox<unsigned>{id & 0xfffffU}.hex<false, ZuFmt::Right<5>>() << ".zdb");
  }
  ZiFile::Path fileName(uint64_t id) const {
    return fileName(dirName(id), id);
  }

  FileRec rn2file(ZdbRN rn, bool write);

  ZmRef<File> getFile(uint64_t id, bool create, bool cache);
  ZmRef<File> openFile(uint64_t id, bool create);
  ZmRef<File> openFile_(const ZiFile::Path &name, uint64_t id, bool create);
  void delFile(File *file);
  void recover(File *file);
  void recover(const FileRec &rec);
  void recover_(const Zdb_::fbs::Record *record, ZmRef<Buf> buf);
  void scan(File *file);
  ZmRef<IndexBlk> getIndexBlk(File *, uint64_t id, bool create, bool cache);
  ZmRef<Buf> read_(const FileRec &);

  void write2(ZmRef<Buf> buf);
  void write_(const Buf *buf);
  bool ack(ZdbRN rn);
  void vacuum();
  void standalone();
  void del_(ZdbRN rn, SeqLen seqLenOp);
  ZdbRN del_prevRN(ZdbRN rn);
  void del__(ZdbRN rn);

  void fileRdError_(File *, ZiFile::Offset, int, ZeError e);
  void fileWrError_(File *, ZiFile::Offset, ZeError e);

  void cache(ZdbAnyObject *object);
  void cache_(ZdbAnyObject *object);
  ZmRef<ZdbAnyObject> cacheDel_(ZdbRN rn);
  void cacheDel_(ZdbAnyObject *object);

  ZdbEnv		*m_env;
  const ZdbCf		*m_cf = nullptr;
  ZuID			m_id = 0;
  ZdbHandler		m_handler;
  ZtString		m_path;
  Lock			m_lock;
    ZdbRN		  m_minRN = ZdbMaxRN;
    ZdbRN		  m_nextRN = 0;
    LRU		  	  m_lru;
    ZmRef<Cache>	  m_cache;
    unsigned		  m_cacheSize = 0;
    uint64_t		  m_cacheLoads = 0;
    uint64_t		  m_cacheMisses = 0;
    ZmRef<BufCache>	  m_writeCache;
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

  DBState(Zfb::Vector<const Zdb_::fbs::DBState *> *envState) :
      DBState_{ZmHashParams{envState->size()}} {
    using namespace Zdb_;
    using namespace Zfb::Load;
    all(envState, [this](unsigned, const fbs::DBState *dbState) {
      add(id(&(dbState->db())), dbState->rn());
    });
  }
  Zfb::Offset<Zfb::Vector<const Zdb_::fbs::DBState *>>
  save(Zfb::Builder &fbb) const {
    using namespace Zdb_;
    using namespace Zfb::Save;
    auto i = readIterator();
    return structVecIter<Zdb_::fbs::DBState>(
	fbb, i.count(), [&i](Zdb_::fbs::DBState *ptr, unsigned) {
      if (auto state = i.iterate())
	new (ptr)
	  fbs::DBState{id(state->template p<0>()), state->template p<1>()};
      else
	new (ptr) fbs::DBState{}; // unused
    });
  }

  bool update(ZuID id, ZdbRN rn) {
    auto state = find(id);
    if (!state) {
      add(id, rn);
      return true;
    }
    auto &thisRN = const_cast<T *>(state)->template p<1>();
    if (thisRN < rn) {
      thisRN = rn;
      return true;
    }
    return false;
  }
  DBState &operator |=(const DBState &r) {
    if (ZuLikely(this != &r)) {
      auto i = r.readIterator();
      while (auto rstate = i.iterate())
	update(rstate->template p<0>(), rstate->template p<1>());
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

  int cmp(const DBState &r) const {
    bool inconsistent;
    {
      unsigned n = count_();
      inconsistent = n != r.count_();
      if (!n && !inconsistent) return 0;
    }
    int diff, ret = 0;
    {
      auto i = readIterator();
      while (auto state = i.iterate()) {
	auto rstate = r.find(state->template p<0>());
	if (!rstate)
	  diff = !!state->template p<1>();
	else
	  diff = ZuCmp<ZdbRN>::cmp(
	      state->template p<0>(), rstate->template p<1>());
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
      while (auto rstate = i.iterate()) {
	auto state = find(rstate->template p<0>());
	if (!state)
	  diff = -!!rstate->template p<1>();
	else
	  diff = ZuCmp<ZdbRN>::cmp(
	      state->template p<1>(), rstate->template p<1>());
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
    unsigned n = a.count_();
    if (ZuUnlikely(!n)) return;
    unsigned j = 0;
    auto i = a.readIterator();
    while (auto state = i.iterate()) {
      if (j++) s << ',';
      s << '{' << state->template p<0>() << ',' <<
	ZuBoxed(state->template p<1>()) << '}';
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

class ZdbAPI ZdbHost {
friend ZdbEnv;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using Cxn = Zdb_::Cxn;
  using DBState = Zdb_::DBState;

friend Cxn;
friend ZdbEnv;

protected:
  ZdbHost(ZdbEnv *env, const ZdbHostCf *config);

public:
  struct IDAxor { static ZuID get(const ZdbHost &h) { return h.id(); } };

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
  friend ZuPrintFn ZuPrintType(ZdbHost *);

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

  int cmp(const ZdbHost *host) const {
    int i;
    if (i = m_dbState.cmp(host->m_dbState)) return i;
    if (i = ZuCmp<bool>::cmp(active(), host->active())) return i;
    return ZuCmp<int>::cmp(priority(), host->priority());
  }

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
using ZdbHostPtr = ZdbHost *;
struct ZdbHostPtr_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, ZdbHostPtr v) {
    if (!v)
      s << "(null)";
    else
      s << *v;
  }
};
ZdbHostPtr_Print ZuPrintType(ZdbHostPtr *);

struct ZdbHosts_HeapID {
  static constexpr const char *id() { return "ZdbEnv.Hosts"; }
};
using ZdbHosts =
  ZmRBTree<ZdbHost,
    ZmRBTreeKey<ZdbHost::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeNodeDerive<true,
	  ZmRBTreeHeapID<ZdbHosts_HeapID> > > > >;

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
  int msgRead2(const Buf *);
  bool msgRcvd(ZiIOContext &);

  bool hbRcvd(const Zdb_::fbs::Heartbeat *);
  void hbTimeout();
  void hbSend();

  template <typename Body>
  Zdb *repRcvd_(const Body *body) const;
  bool repRcvd(Zdb *db, const Zdb_::fbs::Record *record, const Buf *buf);
  bool repRcvd(Zdb *db, const Zdb_::fbs::Gap *gap, const Buf *buf);

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
	hostCfs.addNode(new ZdbHostCfs::Node{key, hostCf});
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

  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  using StateCond = ZmCondition<Lock>;

  using Buf = Zdb_::Buf;
  using DBState = Zdb_::DBState;
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
    return allDBs_(ZuMv(l));
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
  unsigned dbCount() { return m_dbs.count_(); }

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

  void hbRcvd(ZdbHost *host, const Zdb_::fbs::Heartbeat *hb);
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

  void replicated(Zdb *db, ZdbHost *host, const Zdb_::fbs::Record *record);
  void replicated(Zdb *db, ZdbHost *host, const Zdb_::fbs::Gap *gap);
  void replicated_(ZdbHost *host, ZuID id, ZdbRN rn);

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

inline ZiFile::Path Zdb::dirName(uint64_t id) const
{
  return ZiFile::append(m_env->config().path, ZuStringN<8>() <<
      ZuBox<unsigned>{id>>20}.hex<false, ZuFmt::Right<5>>());
}

namespace Zdb_ {

template <typename Body>
inline Zdb *Cxn::repRcvd_(const Body *body) const {
  if (!m_host) {
    ZeLOG(Fatal, "Zdb received replication message before heartbeat");
    return nullptr;
  }
  auto dbID = Zfb::Load::id(body->db());
  Zdb *db = m_env->db_(dbID, ZdbHandler{});
  if (!db) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb unknown remote DBID " << dbID << " received");
    return nullptr;
  }
  return db;
}

} // Zdb_

#endif /* Zdb_HPP */
