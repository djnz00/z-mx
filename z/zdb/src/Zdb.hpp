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
#include <zlib/ZmEngine.hpp>

#include <zlib/ZtString.hpp>
#include <zlib/ZtEnum.hpp>

#include <zlib/ZePlatform.hpp>
#include <zlib/ZeLog.hpp>

#include <zlib/ZiFile.hpp>
#include <zlib/ZiMultiplex.hpp>
#include <zlib/ZiIOBuf.hpp>

#include <zlib/Zfb.hpp>
#include <zlib/ZfbField.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvTelemetry.hpp>
#include <zlib/ZvTelServer.hpp>

#define ZdbMagic	0x0db3a61c	// file magic number
#define ZdbVersion	1		// file format version

//  host state		engine state
//  ==========		============
//  Instantiated	Stopped
//  Initialized		Stopped
//  Opening		Starting | StopPending
//  Closing		Stopping | StartPending
//  Stopped		Stopped
//  Electing		!Stopped
//  Activating		!Stopped
//  Active		!Stopped
//  Deactivating	!Stopped
//  Inactive		!Stopped
//  Stopping		Stopping | StartPending

// FIXME - NEW threading model
//
// env has thread
// each db has a thread, which put, get etc. run on (and all caching etc.)
// each db has a file thread, which serializes all reads/writes
// get/put/etc. interface moved to async, using db thread
// cache eviction enqueues to file thread
// the pending write buffer cache and index block cache are the only
//   contended data structures (shared between db thread and file thread)
// cache miss enqueues read to file thread, avoiding need for pinning, BUT
//   we have a parallel write blk cache for pending writes that elides
//   waiting on the write to complete
//   individual recs are only written once (they are immutable), so no need
//     to provide for multiple pending writes to same rec, so blk cache
//     indexed by RN works (since RN is also an update number)
// index blocks are mutable, work a little differently, there could be
//   multiple pending writes, so index block eviction/write is just a move of
//   the index block in-memory from the main cache to the write cache;
//   any fallthrough read from the write cache moves the index block back
//   into the main cache, incrementing the update count above 1, effectively
//   aborting the earlier pending writes; enqueued stale writes are elided
//   by decrementing the update count and only writing if returned to zero;
//   new index blocks created in memory are always initialized with update
//   count to 1 so that, if evicted and written, the write will be processed
// replication processing at the env/host level occurs in the env thread
// replication processing at the db level occurs in the db thread, enqueuing
//   as needed to the file thread per above
//
//   -------

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
#define ZdbDEBUG(env, e) (void())
#endif

// new file structure with variable-length flatbuffer-format records

// each DB file contains up to 16K records, organized as a 1Kb superblock
// that indexes up to 128 index blocks, each 1.5Kb index block indexing
// to 128 individual records with {offset, length} tuples
// super-block is immediately followed by first index-block

// LRU write-through cache of files is maintained
// each cached file containes a write-through cache of the superblock
// a global LRU write-through cache of index blocks is maintained

// file hdr, bitmap, superblock are written on file eviction
// index blocks are written on index block eviction
// on close, all files and index blocks are both written and evicted

// each record on disk is {data, trailer}

#include <zlib/zdb__fbs.h>

namespace Zdb_ {

using RN = uint64_t;		// record ID

inline constexpr uint64_t maxRN() { return ~static_cast<uint64_t>(0); }
inline constexpr uint64_t nullRN() { return ZuCmp<RN>::null(); }

using Offset = uint64_t;
inline constexpr uint64_t deleted() { return ~static_cast<uint64_t>(0); }

inline constexpr unsigned fileShift()	{ return 14; }
inline constexpr unsigned fileMask()	{ return 0x7fU; }
inline constexpr unsigned fileIndices()	{ return 128; }
inline constexpr unsigned fileRecs()	{ return 16384; }
inline constexpr unsigned fileRecMask()	{ return 0x3fff; }
inline constexpr unsigned indexShift()	{ return 7; }
inline constexpr unsigned indexMask()	{ return 0x7fU; }
inline constexpr unsigned indexRecs()	{ return 128; }

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
  auto hdr = buf->hdr();
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Buf, typename Fn>
inline int verifyHdr(const Buf *buf, Fn fn) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = buf->hdr();
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  int i = fn(hdr, buf);
  if (i < 0) return i;
  return sizeof(Hdr) + i;
}

inline const fbs::Msg *msg(const Hdr *hdr) {
  if (ZuUnlikely(!hdr)) return nullptr;
  auto data = hdr->data();
  if (ZuUnlikely((!Zfb::Verifier{data, hdr->length}.VerifyBuffer<fbs::Msg>())))
    return nullptr;
  return Zfb::GetRoot<fbs::Msg>(data);
}
inline const fbs::Msg *msg_(const Hdr *hdr) {
  return Zfb::GetRoot<fbs::Msg>(hdr->data());
}
inline const fbs::Heartbeat *hb(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  switch (static_cast<int>(msg->body_type())) {
    default:
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
    default:
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
  inline SeqLen mk(SeqLen length, int op) {
    return (length<<2) | op;
  }
  inline SeqLen seqLen(SeqLen seqLenOp) { return seqLenOp>>2; }
  inline int op(SeqLen seqLenOp) {
    return !seqLenOp ? -1 : static_cast<int>(seqLenOp & 3);
  }
  inline constexpr SeqLen maxSeqLen() { return 0x3fffffffU; }
}

namespace HostState {
  using namespace ZvTelemetry::DBHostState;
}

class AnyObject;
class DB;
class Env;
class Host;
class Cxn;

// using Offset = ZiFile::Offset;

// file index block cache

struct IndexBlk_ : public ZmPolymorph {
#pragma pack(push, 1)
  struct Index {
    uint64_t		offset;
    uint32_t		length;
  };
  struct Blk {
    Index		data[indexRecs()];	// RN -> {offset, length}
  };
#pragma pack(pop)

  IndexBlk_(uint64_t id_, uint64_t offset_) : id{id_}, offset{offset_} {
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
using IndexBlkLRU =
  ZmList<IndexBlk_,
    ZmListShadow<true,
      ZmListNode<ZmPolymorph>>>;
using IndexBlkLRUNode = IndexBlkLRU::Node;

uint64_t IndexBlkIDAxor(const IndexBlk_ &blk);
inline const char *IndexBlkHeapID() { return "Zdb.IndexBlk"; }
using IndexBlkCache =
  ZmHash<IndexBlkLRU::Node,
    ZmHashNode<IndexBlkLRU::Node,
      ZmHashKey<IndexBlkIDAxor,
	ZmHashHeapID<IndexBlkHeapID>>>>;

inline uint64_t IndexBlkIDAxor(const IndexBlk_ &blk) {
  return static_cast<const IndexBlk_ &>(blk).id;
}

// file cache

// data file format:
// 0 +20 header (magic:u32, version:u32, flags:u32, allocated:u32, deleted:u32)
// 20 +1024 superblock (FileSuperBlk) (128 x u64)
// followed by sequences of 1.5K-sized index blocks (FileIndexBlk)
//   and records, appended as they are created
namespace FileFlags {
  enum {
    IOError	= 0x00000001,	// I/O error during open
    Clean	= 0x00000002	// closed cleanly
  };
};

#define ZdbCommitted 0xc001da7a	// "cool data"

using Magic = uint32_t;
using SeqLen = uint32_t;

#pragma pack(push, 1)
using U32LE = ZuLittleEndian<uint32_t>;
using U64LE = ZuLittleEndian<uint64_t>;
using I64LE = ZuLittleEndian<int64_t>;

using RNLE = ZuLittleEndian<RN>;
using MagicLE = ZuLittleEndian<Magic>;
using SeqLenLE = ZuLittleEndian<SeqLen>;

struct FileHdr {
  MagicLE	magic;		// ZdbMagic
  U32LE		version;
  U32LE		flags;		// FileFlags
  U32LE		allocated;	// allocated record count
  U32LE		deleted;	// deleted record count
};
struct FileBitmap {		// bitmap
  U64LE		data[fileRecs()>>6];
};
struct FileSuperBlk {		// super block
  U64LE		data[fileIndices()];
};
struct FileIndex {
  U64LE		offset;
  U32LE		length;
};
struct FileIndexBlk {		// index block
  FileIndex	data[indexRecs()];
};
struct FileRecTrlr {		// record trailer
  RNLE		rn;
  RNLE		prevRN;
  SeqLenLE	seqLenOp;	// chain length + op
  MagicLE	magic;		// ZdbCommitted
};
#pragma pack(pop)

// file cache
class ZdbAPI File_ : public ZiFile {
  File_() = delete;
  File_(const File_ &) = delete;
  File_(File_ &&) = delete;
  File_ &operator =(const File_ &) = delete;
  File_ &operator =(File_ &&) = delete;

  using Bitmap = ZuBitmap<fileRecs()>;
  struct SuperBlk {
    uint64_t	data[fileIndices()];	// offsets to index blocks
  };

friend DB;

  File_(DB *db, uint64_t id) : m_db{db}, m_id{id} { }

public:
  uint64_t id() const { return m_id; }
  static uint64_t IDAxor(const File &file) { return file.id(); }

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

  bool readIndexBlk(IndexBlk *);
  bool writeIndexBlk(IndexBlk *);

  bool scan();
  bool sync();
  bool sync_();

private:
  void reset();

  DB			*m_db = nullptr;
  uint64_t		m_id = 0;	// (RN>>fileShift())

  uint64_t		m_offset = 0;	// append offset
  uint32_t		m_flags = 0;
  unsigned		m_allocated = 0;
  unsigned		m_deleted = 0;
  Bitmap		m_bitmap;
  SuperBlk		m_superBlk;
};

using FileLRU =
  ZmList<File_,
    ZmListNode<File_,
      ZmListShadow<true>>>;

inline const char *FileHeapID() { return "Zdb.File"; }
using FileCache =
  ZmHash<FileLRU::Node,
    ZmHashNode<FileLRU::Node,
      ZmHashKey<File_::IDAxor,
	ZmHashHeapID<FileHeapID>>>>;

using File = FileCache::Node;

// file, indexBlk, RN offset within index block
class FileRec {
public:
  FileRec() { }
  FileRec(File *file, IndexBlk *indexBlk, unsigned indexOff) :
    m_file{file}, m_indexBlk{indexBlk}, m_indexOff{indexOff} { }

  bool operator !() const { return !m_file; }
  ZuOpBool

  File *file() const { return m_file; }
  IndexBlk *indexBlk() const { return m_indexBlk; }
  unsigned indexOff() const { return m_indexOff; }

  RN rn() const {
    return ((m_indexBlk->id)<<indexShift()) | m_indexOff;
  }
  const IndexBlk::Index &index() const {
    return const_cast<FileRec *>(this)->index();
  }
  IndexBlk::Index &index() { return m_indexBlk->blk.data[m_indexOff]; }

private:
  File		*m_file = nullptr;
  IndexBlk	*m_indexBlk = nullptr;
  unsigned	m_indexOff = 0;
};

// buffer cache
inline const char *Buf_HeapID() { return "Zdb::Buf"; }
using Buf_ = ZiIOVBuf<ZuGrow(0, 1), Buf_HeapID>;
RN Buf_RNAxor(const Buf_ &buf);
using BufCache =
  ZmHash<Buf_,
    ZmHashNode<Buf_,
      ZmHashKey<Buf_RNAxor,
	ZmHashHeapID<ZmHeapDisable(),
	  ZmHashID<Buf_HeapID>>>>>;

class ZdbAPI Buf : public BufCache::Node {
public:
  Buf() = default;
  Buf(DB *db) : BufCache::Node{db} { }

  DB *db() const {
    return static_cast<DB *>(owner);
  }
  using Buf_::data;
  const Hdr *hdr() const {
    return reinterpret_cast<const Hdr *>(data());
  }

friend Cxn;
friend AnyObject;

  void send(ZiIOContext &);
  void sent(ZiIOContext &);

  ZmRef<Buf> replicate() {
    ZmRef<Buf> buf = new Buf{db()};
    auto data = buf->ensure(length);
    if (!data) return nullptr;
    memcpy(data, this->data(), length);
    return buf;
  }

  template <typename S> void print(S &s) const;

  friend ZuPrintFn ZuPrintType(Buf *);
};
inline RN Buf_RNAxor(const Buf_ &buf) {
  return record_(msg_(static_cast<const Buf &>(buf).hdr()))->rn();
}

using DBState_ = ZmLHashKV<ZuID, RN, ZmLHashLocal<>>;
struct DBState : public DBState_ {
  DBState() = delete;

  DBState(unsigned size) : DBState_{ZmHashParams{size}} { }

  DBState(Zfb::Vector<const fbs::DBState *> *envState) :
      DBState_{ZmHashParams{envState->size()}} {
    using namespace Zdb_;
    using namespace Zfb::Load;
    all(envState, [this](unsigned, const fbs::DBState *dbState) {
      add(id(&(dbState->db())), dbState->rn());
    });
  }
  void load(Zfb::Vector<const fbs::DBState *> *envState) {
    using namespace Zdb_;
    using namespace Zfb::Load;
    all(envState, [this](unsigned, const fbs::DBState *dbState) {
      update(id(&(dbState->db())), dbState->rn());
    });
  }
  Zfb::Offset<Zfb::Vector<const fbs::DBState *>>
  save(Zfb::Builder &fbb) const {
    using namespace Zdb_;
    using namespace Zfb::Save;
    auto i = readIterator();
    return structVecIter<fbs::DBState>(
	fbb, i.count(), [&i](fbs::DBState *ptr, unsigned) {
      if (auto state = i.iterate())
	new (ptr)
	  fbs::DBState{id(state->template p<0>()), state->template p<1>()};
      else
	new (ptr) fbs::DBState{}; // unused
    });
  }

  bool update(ZuID id, RN rn) {
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
	  diff = ZuCmp<RN>::cmp(
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
	  diff = ZuCmp<RN>::cmp(
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

template <typename S>
void Buf::print(S &s) const
{
  auto msg = Zdb_::msg(hdr());
  if (!msg) {
    s << "corrupt{}";
    return;
  }
  if (auto hb = Zdb_::hb(msg)) {
    auto id = Zfb::Load::id(hb->host());
    s << "heartbeat{host=" << id <<
      " state=" << HostState::name(hb->state()) <<
      " dbState=" << DBState{hb->envState()} << "}";
    return;
  }
  if (auto record = Zdb_::record(msg)) {
    auto id = Zfb::Load::id(record->db());
    auto seqLenOp = record->seqLenOp();
    auto data = Zfb::Load::bytes(record->data());
    s << "record{db=" << id <<
      " RN=" << record->rn() <<
      " prevRN=" << record->prevRN() <<
      " seqLen=" << SeqLenOp::seqLen(seqLenOp) <<
      " op=" << Op::name(SeqLenOp::op(seqLenOp)) <<
      ZtHexDump("}\n", data.data(), data.length());
    return;
  }
  if (auto gap = Zdb_::gap(msg)) {
    auto id = Zfb::Load::id(gap->db());
    s << "gap{db=" << id <<
      " RN=" << gap->rn() <<
      " count=" << gap->count() << "}";
    return;
  }
  s << "unknown{}";
}

// object cache
using LRU =
  ZmList<ZmPolymorph,
    ZmListNode<ZmPolymorph,
      ZmListShadow<true>>>;
using LRUNode = LRU::Node;

RN LRUNode_RNAxor(const LRUNode &object);

inline const char *CacheHeapID() { return "Zdb.Cache"; }
using Cache =
  ZmHash<LRUNode,
    ZmHashNode<LRUNode,
      ZmHashKey<LRUNode_RNAxor,
	ZmHashHeapID<ZmHeapDisable(),
	  ZmHashID<CacheHeapID>>>>>;
using CacheNode = Cache::Node;

class ZdbAPI AnyObject : public CacheNode, public ZuPrintable {
  AnyObject() = delete;
  AnyObject(const AnyObject &) = delete;
  AnyObject &operator =(const AnyObject &) = delete;
  AnyObject(AnyObject &&) = delete;
  AnyObject &operator =(AnyObject &&) = delete;

  friend DB;

public:
  AnyObject(DB *db) : m_db{db} { }
  virtual ~AnyObject() { }

  DB *db() const { return m_db; }
  RN rn() const { return m_rn; }
  RN prevRN() const { return m_prevRN; }
  RN origRN() const { return m_origRN; }
  SeqLen seqLen() const { return SeqLenOp::seqLen(m_seqLenOp); }
  int op() const { return SeqLenOp::op(m_seqLenOp); }
  bool deleted() const { return op() == Op::Delete; }

  ZmRef<Buf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<AnyObject *>(this)->ptr_(); }

  template <typename S> void print(S &s) const {
    using namespace Zdb_;
    s << "rn=" << m_rn << " prevRN=" << m_prevRN << " origRN=" << m_origRN <<
      " seqLen=" << SeqLenOp::seqLen(m_seqLenOp) <<
      " op=" << Op::name(SeqLenOp::op(m_seqLenOp));
  }

private:
  void init(RN rn) { m_rn = rn; }
  void load(RN rn, RN prevRN, SeqLen seqLenOp) {
    using namespace Zdb_;
    m_rn = rn;
    m_prevRN = prevRN;
    m_seqLenOp = seqLenOp;
  }

  void push(RN rn) { m_origRN = m_rn; m_rn = rn; }
  void commit(int op) {
    using namespace Zdb_;
    m_prevRN = m_origRN;
    m_origRN = nullRN();
    m_seqLenOp = SeqLenOp::mk(seqLen() + 1, op);
  }
  void abort() { m_rn = m_origRN; m_origRN = nullRN(); }

  void put() {
    using namespace Zdb_;
    m_seqLenOp = SeqLenOp::mk(1, Op::Put);
  }

  DB		*m_db;
  RN		m_rn = nullRN();
  RN		m_prevRN = nullRN();
  RN		m_origRN = nullRN();	// RN backup before put()
  SeqLen	m_seqLenOp = 0;
};

inline RN LRUNode_RNAxor(const LRUNode &node) {
  return static_cast<const AnyObject &>(node).rn();
}

const char *ObjectHeapID() { return "Zdb.Object"; }
template <typename T, typename Heap>
class Object_ : public Heap, public AnyObject {
  Object_() = delete;
  Object_(const Object_ &) = delete;
  Object_ &operator =(const Object_ &) = delete;
  Object_(Object_ &&) = delete;
  Object_ &operator =(Object_ &&) = delete;

public:
  template <typename L>
  Object_(DB *db_, L l) : AnyObject{db_} {
    l(static_cast<void *>(&m_data[0]));
  }

  void *ptr_() { return &m_data[0]; }
  const void *ptr_() const { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~Object_() { ptr()->~T(); }

  const T &data() const & { return *ptr(); }
  T &data() & { return *ptr(); }
  T &&data() && { return ZuMv(*ptr()); }

  template <typename S> void print(S &s) const {
    AnyObject::print(s);
    s << ' ' << this->data();
  }

private:
  uint8_t	m_data[sizeof(T)];
};
template <typename T>
using ObjectHeap = ZmHeap<ObjectHeapID, sizeof(Object_<T, ZuNull>)>;
template <typename T>
using Object = Object_<T, ObjectHeap<T>>;

// CtorFn(db) - construct new object from flatbuffer
typedef AnyObject *(*CtorFn)(DB *);
// LoadFn(db, data, length) - construct new object from flatbuffer
typedef AnyObject *(*LoadFn)(DB *, const uint8_t *, unsigned);
// UpdateFn(object, data, length) - load flatbuffer into object
typedef AnyObject *(*UpdateFn)(AnyObject *, const uint8_t *, unsigned);
// SaveFn(fbb, ptr) - save *ptr into flatbuffer fbb
typedef void (*SaveFn)(Zfb::Builder &, const void *);
// RecoverFn(object, buf) - object recovered (added, updated, deleted)
typedef void (*RecoverFn)(AnyObject *, const Buf *buf);

struct DBHandler {
  CtorFn	ctorFn =
    [](DB *db) -> AnyObject * { return new AnyObject{db}; };
  LoadFn	loadFn =
    [](DB *db, const uint8_t *, unsigned) ->
	AnyObject * { return new AnyObject{db}; };
  UpdateFn	updateFn = nullptr;
  SaveFn	saveFn = nullptr;
  RecoverFn	recoverFn = nullptr;

  template <typename T>
  static DBHandler bind() {
    return DBHandler{
      .ctorFn = [](DB *db) -> AnyObject * {
	return new Object<T>{db, [](void *ptr) { new (ptr) T{}; }};
      },
      .loadFn = [](DB *db, const uint8_t *data, unsigned len) ->
	  AnyObject * {
	auto fbObject = ZfbField::verify<T>(data, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	return new Object<T>{db, [fbObject](void *ptr) {
	  ZfbField::ctor<T>(ptr, fbObject);
	}};
      },
      .updateFn =
	[](AnyObject *object, const uint8_t *data_, unsigned len) ->
	  AnyObject * {
	auto fbObject = ZfbField::verify<T>(data_, len);
	if (ZuUnlikely(!fbObject)) return nullptr;
	auto &data = static_cast<Object<T> *>(object)->data();
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

struct DBCf {
  ZuID			id;
  ZmThreadName		thread;		// in-memory thread
  mutable unsigned	sid = 0;	// thread slot ID
  ZmThreadName		fileThread;	// file I/O thread
  mutable unsigned	fileSID = 0;
  int			cacheMode = ZdbCacheMode::Normal;
  unsigned		vacuumBatch = 1000;
  bool			warmUp = false;	// pre-write initial DB file
  uint8_t		repMode = 0;	// 0 - deferred, 1 - in put()

  DBCf() = default;
  DBCf(ZuString id_) : id{id_} { }
  DBCf(ZuString id_, const ZvCf *cf) : id{id_} {
    thread = cf->get("thread");
    fileThread = cf->get("fileThread");
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", false, ZdbCacheMode::Normal);
    vacuumBatch = cf->getInt("vacuumBatch", 1, 100000, false, 1000);
    warmUp = cf->getInt("warmUp", 0, 1, false, 0);
    repMode = cf->getInt("repMode", 0, 1, false, 0);
  }

  static ZuID IDAxor(const DBCf &cf) { return cf.id; }
};

inline const char *DeletesHeapID() { return "Zdb.Deletes"; }
struct DeleteOp {
  RN		rn = nullRN();
  SeqLen	seqLenOp = 0;
};
using Deletes =
  ZmRBTreeKV<RN, DeleteOp,
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<DeletesHeapID>>>;

class ZdbAPI DB : public ZmPolymorph {
friend Cxn;
friend File;
friend AnyObject;
friend Env;

protected:
  DB(Env *env, DBCf *cf);

public:
  ~DB();

private:
  void init(DBHandler);
  void final();

  bool open();
  void close();

  bool recover();
  void checkpoint();
  bool checkpoint_();

public:
  Env *env() const { return m_env; }
  const DBCf &config() const { return *m_cf; }
  unsigned cacheSize() const { return m_cacheSize; } // unclean read
  unsigned fileCacheSize() const { return m_fileCacheSize; }
  unsigned indexBlkCacheSize() const { return m_indexBlkCacheSize; }

  // first RN that is committed (will be ZdbMaxRN if DB is empty)
  RN minRN() const { ReadGuard guard(m_pushLock); return m_minRN; }
  // next RN that will be allocated
  RN nextRN() const { ReadGuard guard(m_pushLock); return m_nextRN; }

  // create new placeholder record (null RN, in-memory only, never in DB)
  ZmRef<AnyObject> placeholder();

  // create new record
  ZmRef<AnyObject> push();
  // create new record (idempotent)
  ZmRef<AnyObject> push(RN rn);

  // get record
  ZmRef<AnyObject> get(RN rn);		// read-only query
  ZmRef<AnyObject> getUpdate(RN rn);	// potential subsequent update

  // update record - returns true if update can proceed
  bool update(AnyObject *object);
  // update record (idempotent) - returns true if update can proceed
  bool update(AnyObject *object, RN rn);
  // update record (with prevRN, without object)
  ZmRef<AnyObject> update(RN prevRN);
  // update record (idempotent) (with prevRN, without object)
  ZmRef<AnyObject> update(RN prevRN, RN rn);

  // commit push() /update() - causes replication / write
  void put(AnyObject *);
  // commit appended update() - causes replication / write
  void append(AnyObject *);
  // commits delete following push() / update()
  void del(AnyObject *);
  // abort push() / update()
  void abort(AnyObject *);

  // purge all records < minRN
  void purge(RN minRN);

  using Telemetry = ZvTelemetry::DB;

  void telemetry(Telemetry &data) const;

  static ZuID IDAxor(const DB &db) { return db.config().id; }

private:
  // push initial record
  ZmRef<AnyObject> push_();
  // idempotent push
  ZmRef<AnyObject> push_(RN rn);

  // low-level get, does not filter deleted records
  ZmRef<AnyObject> get__(RN rn);

  // load object from buffer
  ZmRef<AnyObject> load(const fbs::Record *record);
  // save object to buffer
  void save(Zfb::Builder &fbb, AnyObject *object);

  // replication data rcvd (copy/commit, called while env is unlocked)
  void replicated(const fbs::Record *record, const Buf *buf);
  void replicated(const fbs::Gap *gap, const Buf *buf);

  // forward replication data
  ZmRef<Buf> replicateFwd(const Buf *buf);
  // prepare recovery data for sending
  ZmRef<Buf> recovery(RN rn);
  ZmRef<Buf> gap(RN rn, uint64_t count);

  // transition to standalone, trigger vacuuming
  void standalone();

  ZiFile::Path dirName(uint64_t id) const;
  ZiFile::Path fileName(ZiFile::Path dir, uint64_t id) const {
    return ZiFile::append(dir, ZuStringN<12>() <<
	ZuBox<unsigned>{id & 0xfffffU}.hex<false, ZuFmt::Right<5>>() << ".zdb");
  }
  ZiFile::Path fileName(uint64_t id) const {
    return fileName(dirName(id), id);
  }

  FileRec rn2file(RN rn, bool write);

  File *getFile(uint64_t id, bool create);
  ZmRef<File> openFile(uint64_t id, bool create);
  ZmRef<File> openFile_(const ZiFile::Path &name, uint64_t id, bool create);
  void delFile(File *file);
  void recover(File *file);
  void recover(const FileRec &rec);
  void recover_(const fbs::Record *record, ZmRef<Buf> buf);
  void scan(File *file);
  IndexBlk *getIndexBlk(File *, uint64_t id, bool create);
  ZmRef<Buf> read_(const FileRec &);

  void write2(ZmRef<Buf> buf);
  bool write_(const Buf *buf);
  bool ack(RN rn);
  void vacuum();
  void vacuum_();
  ZuPair<int, RN> del_(const DeleteOp &, unsigned maxBatchSize);
  RN del_prevRN(RN rn);
  void del__(RN rn);

  void fileRdError_(File *, ZiFile::Offset, int, ZeError e);
  void fileWrError_(File *, ZiFile::Offset, ZeError e);

  void cache(AnyObject *object);
  void cache_(AnyObject *object);
  ZmRef<AnyObject> cacheDel_(RN rn);
  void cacheDel_(AnyObject *object);

  Env			*m_env;
  const DBCf		*m_cf;
  DBHandler		m_handler;
  ZtString		m_path;

  // FIXME - env has a single env thread (the dbTID)
  // FIXME - write thread performs all cache indexBlk/file cache eviction,
  // allowing FileRecs to be relied on even when the DB is unlocked,
  // and all disk I/O is performed blocking and unlocked, any eviction is
  // enqueued on run queue
  // FIXME - each DB can have it's own thread
  // FIXME - allDBs can run in parallel - use an async continuation
  // FIXME - remove m_standalone, standalone() can run in dbThread
  // FIXME - figure out DB thread vs write thread model

  Lock			m_lock;
  // RN allocator
    RN			  m_minRN = maxRN();
    RN			  m_nextRN = 0;

  // object cache
    LRU		  	  m_lru;
    ZmRef<Cache>	  m_cache;
    unsigned		  m_cacheSize = 0;
    uint64_t		  m_cacheLoads = 0;
    uint64_t		  m_cacheMisses = 0;

  // write cache
    ZmRef<BufCache>	  m_writeCache;
    Deletes		  m_deletes;
    RN			  m_vacuumRN = nullRN();
    uint64_t		  m_lastFile = 0;

  // index block cache
    IndexBlkLRU	 	  m_indexBlkLRU;
    ZmRef<IndexBlkCache>  m_indexBlks;
    unsigned		  m_indexBlkCacheSize = 0;
    uint64_t		  m_indexBlkLoads = 0;
    uint64_t		  m_indexBlkMisses = 0;

  // file cache
    FileLRU		  m_fileLRU;
    ZmRef<FileCache>	  m_files;
    unsigned		  m_fileCacheSize = 0;
    uint64_t		  m_fileLoads = 0;
    uint64_t		  m_fileMisses = 0;
};

inline const char *DBCfsHeapID() { return "Env.DBCfs"; }
using DBCfs =
  ZmRBTree<DBCf,
    ZmRBTreeKey<DBCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<DBCfsHeapID>>>>;

inline const char *DBsHeapID() { return "Env.DBs"; }
using DBs =
  ZmRBTree<DB,
    ZmRBTreeKey<DB::IDAxor,
      ZmRBTreeNode<DB,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<DBsHeapID>>>>>;

struct HostCf {
  ZuID		id;
  unsigned	priority = 0;
  ZiIP		ip;
  uint16_t	port = 0;
  ZtString	up;
  ZtString	down;

  HostCf(const ZtString &key, const ZvCf *cf) {
    id = cf->get("id", true);
    priority = cf->getInt("priority", 0, 1<<30, true);
    ip = cf->get("ip", true);
    port = cf->getInt("port", 1, (1<<16) - 1, true);
    up = cf->get("up");
    down = cf->get("down");
  }

  static ZuID IDAxor(const HostCf &cfg) { return cfg.id; }
};

inline const char *HostCfs_HeapID() { return "Env.HostCfs"; }
using HostCfs =
  ZmHash<HostCf,
    ZmHashKey<HostCf::IDAxor,
      ZmHashHeapID<HostCfs_HeapID>>>;

// FIXME
// - file reads and writes are serialized via filethread
// - allows eviction and write enqueue to be performed together (any read will follow write)
// - eliminates problem with unstable writes
// - removes need for write buf cache
// - careful - any index block read must NOT be performed immediately but scheduled via fileThread to ensure pending writes complete

// FIXME - all external interfaces become async, including start/stop/init etc.
// FIXME - all classes/structs in Zdb_ namespace, with external aliases
// used, e.g. using Host = Zdb_::Host

class ZdbAPI Host {
friend Cxn;
friend Env;

protected:
  Host(Env *env, const HostCf *config);

public:
  const HostCf &config() const { return *m_cf; }

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
  friend ZuPrintFn ZuPrintType(Host *);

  static ZuID IDAxor(const Host &h) { return h.id(); }

private:
  ZmRef<Cxn> cxn() const { return m_cxn; }

  void state(int s) { m_state = s; }

  const DBState &dbState() const { return m_dbState; }
  DBState &dbState() { return m_dbState; }

  bool active() const {
    using namespace HostState;
    switch (m_state) {
      case Activating:
      case Active:
	return true;
    }
    return false;
  }

  int cmp(const Host *host) const {
    if (ZuUnlikely(host == this)) return 0;
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

  Env			*m_env;
  const HostCf		*m_cf;
  ZiMultiplex		*m_mx;

  ZmScheduler::Timer	m_connectTimer;

  // guarded by Env

  ZmRef<Cxn>		m_cxn;
  int			m_state = HostState::Instantiated;
  DBState		m_dbState;
  bool			m_voted = false;
};
using HostPtr = Host *;
struct HostPtr_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, HostPtr v) {
    if (!v)
      s << "(null)";
    else
      s << *v;
  }
};
HostPtr_Print ZuPrintType(HostPtr *);

inline const char *Hosts_HeapID() { return "Env.Hosts"; }
using Hosts =
  ZmHash<Host,
    ZmHashNode<Host,
      ZmHashKey<Host::IDAxor,
	ZmHashHeapID<Hosts_HeapID>>>>;

class Cxn : public ZiConnection {
  Cxn(const Cxn &);
  Cxn &operator =(const Cxn &);	// prevent mis-use

friend Env;
friend Host;

  Cxn(Env *env, Host *host, const ZiCxnInfo &ci);

  Env *env() const { return m_env; }
  void host(Host *host) { m_host = host; }
  Host *host() const { return m_host; }

  void connected(ZiIOContext &);
  void disconnected();

  void msgRead(ZiIOContext &);
  int msgRead2(const Buf *);
  bool msgRcvd(ZiIOContext &);

  bool hbRcvd(const fbs::Heartbeat *);
  void hbTimeout();
  void hbSend();

  template <typename Body>
  DB *repRcvd_(const Body *body) const;
  bool repRcvd(DB *db, const fbs::Record *record, const Buf *buf);
  bool repRcvd(DB *db, const fbs::Gap *gap, const Buf *buf);

  void repSend(ZmRef<Buf> buf);

  void ackRcvd();
  void ackSend(int type, AnyObject *o);

  Env		*m_env;
  Host		*m_host;	// 0 if not yet associated

  ZmRef<Buf>		m_rxBuf;

  ZmScheduler::Timer	m_hbTimer;
};

// FIXME
// RunFn(ZmFn<> fn) - run fn on application thread
typedef uintptr_t *(*RunFn)(ZmFn<> fn);
// ActiveFn() - activate / inactivate
typedef void (*ActiveFn)(Env *);

struct EnvHandler {
  ActiveFn		active = [](Env *) { };
  ActiveFn		inactive = [](Env *) { };
};

// Env configuration
struct EnvCf {
  ZtString			path;
  ZmThreadName			thread;
  mutable unsigned		sid = 0;
  ZmThreadName			fileThread;
  mutable unsigned		fileSID = 0;
  DBCfs				dbCfs;
  HostCfs			hostCfs;
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

  EnvCf() = default;
  EnvCf(const ZvCf *cf) {
    path = cf->get("path", true);
    thread = cf->get("thread", true);
    fileThread = cf->get("fileThread");
    {
      ZvCf::Iterator i(cf->subset("dbs", true));
      ZuString key;
      while (ZmRef<ZvCf> dbCf = i.subset(key))
	dbCfs.addNode(new DBCfs::Node{key, dbCf});
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

class ZdbAPI Env : public ZmPolymorph, public ZmEngine<Env> {
  Env(const Env &);
  Env &operator =(const Env &);		// prevent mis-use

  using Engine = ZmEngine<Env>;

friend DB;
friend AnyObject;
friend Engine;
friend Host;

  using Engine::start;
  using Engine::stop;

  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

friend Cxn;

  static const char *CxnHash_HeapID() { return "Env.CxnHash"; }
  using CxnHash =
    ZmHash<ZmRef<Cxn>,
      ZmHashLock<ZmPLock,
	  ZmHashHeapID<CxnHash_HeapID>>>;

#ifdef ZdbRep_DEBUG
  bool debug() const { return m_cf.debug; }
#endif

public:
  Env() { }
  ~Env() { }

  void init(EnvCf config, ZiMultiplex *mx, EnvHandler handler);
  void final();

private:
  void start_();
  void stop_();
  template <typename L>
  bool spawn(L l) {
    if (!m_mx || !m_mx->running()) return false;
    m_mx->run(m_cf.sid, ZuMv(l));
    return true;
  }
  void wake();

  bool open();
  void close();

public:
  void checkpoint();

  const EnvCf &config() const { return m_cf; }
  ZiMultiplex *mx() const { return m_mx; }

  int state() const {
    return m_self ? m_self->state() : HostState::Instantiated;
  }
  void state(int n) {
    if (!m_self) {
      ZeLOG(Fatal, ZtString{} <<
	  "Env::state(" << ZuBoxed(n) << ") called out of order");
      return;
    }
    m_self->state(n);
  }
  bool running() {
    using namespace HostState;
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
    using namespace HostState;
    switch (state()) {
      case Activating:
      case Active:
	return true;
    }
    return false;
  }

  Host *self() const { return m_self; }
  Host *host(ZuID id) const { return m_hosts.find(id); }
  template <typename L> bool allHosts(L l) const {
    auto i = m_hosts.readIterator();
    while (auto node = i.iterate())
      if (!l(node)) return false;
    return true;
  }

private:
  DB *db_(ZuID id, DBHandler handler);
public:
  DB *db(ZuID id);
  template <typename T>
  DB *add(ZuID id) {
    return db_(id, DBHandler::bind<T>());
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
    // FIXME - invoke lambda within each DB's thread context
    return allDBs_(ZuMv(l));
  }

  using Telemetry = ZvTelemetry::DBEnv;

  void telemetry(Telemetry &data) const;

  ZvTelemetry::DBEnvFn telFn() {
    return ZvTelemetry::DBEnvFn{ZmMkRef(this), [](
	Env *dbEnv,
	ZmFn<const ZvTelemetry::DBEnv &> envFn,
	ZmFn<const ZvTelemetry::DBHost &> hostFn,
	ZmFn<const ZvTelemetry::DB &> dbFn) {
      {
	ZvTelemetry::DBEnv data;
	dbEnv->telemetry(data);
	envFn(data);
      }
      dbEnv->allHosts([&hostFn](const Host *host) {
	ZvTelemetry::DBHost data;
	host->telemetry(data);
	hostFn(data);
	return true;
      });
      dbEnv->allDBs([&dbFn](const DB *db) {
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
  void reactivate(Host *host);	// re-assert master

  ZiConnection *accepted(const ZiCxnInfo &ci);
  void connected(Cxn *cxn);
  void associate(Cxn *cxn, ZuID hostID);
  void associate(Cxn *cxn, Host *host);
  void disconnected(Cxn *cxn);
  void disconnected(Host *host);

  void hbRcvd(Host *host, const fbs::Heartbeat *hb);
  void vote(Host *host);

  void hbStart();
  void hbSend();		// send heartbeat and reschedule self
  void hbSend_();		// send heartbeat (once, broadcast)
  void hbSend_(Cxn *cxn);	// send heartbeat (once, directed)

  void dbStateRefresh();	// refresh m_self->dbState()

  Host *setMaster();	// returns old master
  void setNext(Host *host);
  void setNext();

  void startReplication();
  void stopReplication();

  void replicated(DB *db, Host *host, const fbs::Record *record);
  void replicated(DB *db, Host *host, const fbs::Gap *gap);
  void replicated_(Host *host, ZuID id, RN rn);

  void repSend(ZmRef<Buf> buf);
  void recSend();

  void ackRcvd(Host *host, bool positive, ZuID db, RN rn);

  void write(ZmRef<Buf> buf);

  // write thread
  void standalone();
  void replicating();
  bool isStandalone() { return m_standalone; }

  EnvCf		m_cf;
  ZiMultiplex		*m_mx = nullptr;

  EnvHandler		m_handler;

  ZmSemaphore		*m_stopping = nullptr;

  // specific
    bool		  m_appActive =false;
    Host		  *m_self = nullptr;
    Host		  *m_master = nullptr;	// == m_self if Active
    Host		  *m_prev = nullptr;	// previous-ranked host
    Host		  *m_next = nullptr;	// next-ranked host
    ZmRef<Cxn>		  m_nextCxn;		// replica peer's cxn
    bool		  m_recovering = false;	// recovering next-ranked host
    DBState		  m_recover{4};		// recovery state
    DBState		  m_recoverEnd{4};	// recovery end
    int			  m_nPeers = 0;	// # up to date peers
					// # votes received (Electing)
					// # pending disconnects (Stopping)
    ZmTime		  m_hbSendTime;
    DBs			  m_dbs;
    Hosts		  m_hosts;

  // write thread
  bool			m_standalone = false;

  ZmScheduler::Timer	m_hbSendTimer;
  ZmScheduler::Timer	m_electTimer;

  ZmRef<CxnHash>	m_cxns;
};

inline ZiFile::Path DB::dirName(uint64_t id) const
{
  return ZiFile::append(m_env->config().path, ZuStringN<8>() <<
      ZuBox<unsigned>{id>>20}.hex<false, ZuFmt::Right<5>>());
}

template <typename Body>
inline DB *Cxn::repRcvd_(const Body *body) const {
  if (!m_host) {
    ZeLOG(Fatal, "Zdb received replication message before heartbeat");
    return nullptr;
  }
  auto id = Zfb::Load::id(body->db());
  DB *db = m_env->db_(id, DBHandler{});
  if (!db) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb internal replication error - could not add DB " << id);
    return nullptr;
  }
  return db;
}

} // Zdb_

// external API
using ZdbRN = Zdb_::RN;
using ZdbAnyObject = Zdb_::AnyObject;
template <typename T> using ZdbObject = Zdb_::Object<T>;
using Zdb = Zdb_::DB;
using ZdbCf = Zdb_::DBCf;
using ZdbCtorFn = Zdb_::CtorFn;
using ZdbLoadFn = Zdb_::LoadFn;
using ZdbUpdateFn = Zdb_::UpdateFn;
using ZdbSaveFn = Zdb_::SaveFn;
using ZdbRecoverFn = Zdb_::RecoverFn;
using ZdbHandler = Zdb_::DBHandler;
using ZdbEnv = Zdb_::Env;
using ZdbEnvCf = Zdb_::EnvCf;
using ZdbRunFn = Zdb_::RunFn;
using ZdbActiveFn = Zdb_::ActiveFn;
using ZdbEnvHandler = Zdb_::EnvHandler;
using ZdbHost = Zdb_::Host;
namespace ZdbHostState { using namespace Zdb_::HostState; }
using ZdbBuf = Zdb_::Buf;

#endif /* Zdb_HPP */
