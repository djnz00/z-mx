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

// Z Database file format and I/O

#ifndef ZdbFile_HPP
#define ZdbFile_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <zlib/ZdbTypes.hpp>
#include <zlib/ZdbMsg.hpp>

#define ZdbFileVersion	1		// file format version

// -- file format
//
// new file structure with variable-length flatbuffer-format records

// each DB file contains up to 16K records, organized as a 1Kb superblock
// that indexes up to 128 index blocks, each a 1Kb index block indexing
// up to 128 individual records with {u64 offset} tuples
// super-block is immediately followed by first index-block

// LRU write-through cache of files is maintained
// each cached file containes a write-through cache of the superblock
// a global LRU write-through cache of index blocks is maintained

// file hdr, bitfield, superblock are written on file eviction
// index blocks are written on index block eviction
// on close, all files and index blocks are written and evicted

// each record on disk is {data, trailer}, where trailer is the on-disk
// equivalent of the network message header

// 0,+24 header (
//   magic:u32,
//   version:u32,
//   flags:u32,
//   allocated:u32,
//   deleted:u32,
//   excised:u32)
// 24,+512 bitfield (FileBitfield) (16384 x u2)
// 536,+1024 superblock (FileSuperBlk) (128 x u64)
// ... followed by extents - index blocks interleaved with records
// Offset,+8 extent header
// Offset+8,+1032 index block (FileIndexBlk) (129 x u64)
// Offset+1040,+12 index block trailer

namespace Zdb_ {

// file format constants
inline constexpr unsigned fileShift()	{ return 14; }
inline constexpr unsigned fileMask()	{ return 0x7fU; }
inline constexpr unsigned fileIndices()	{ return 128; }
inline constexpr unsigned fileRNMask()	{ return 0x3fff; }
inline constexpr unsigned fileRNs()	{ return 16384; }
inline constexpr unsigned indexShift()	{ return 7; }
inline constexpr unsigned indexMask()	{ return 0x7fU; }
inline constexpr unsigned indexRNs()	{ return 128; }

namespace Magic {	// magic numbers
  inline constexpr uint32_t file()	{ return 0x0db3a61c; }
  inline constexpr uint32_t committed()	{ return 0xc001da7a; }
}

using Magic = uint32_t;

#pragma pack(push, 1)
using U32LE = ZuLittleEndian<uint16_t>;
using U32LE = ZuLittleEndian<uint32_t>;
using U64LE = ZuLittleEndian<uint64_t>;
using I64LE = ZuLittleEndian<int64_t>;
using U128LE = ZuLittleEndian<uint128_t>;
using I128LE = ZuLittleEndian<int128_t>;

using UNLE = ZuLittleEndian<UN>;
using RNLE = ZuLittleEndian<RN>;
using MagicLE = ZuLittleEndian<Magic>;
using SeqLenLE = ZuLittleEndian<SeqLen>;

struct FileHdr {
  MagicLE	magic;		// Magic::file()
  U32LE		version;
  U64LE		clean;		// clean to this extent
  U64LE		rn;		// base RN
  U32LE		allocated;	// allocated record count
  U32LE		deleted;	// deleted record count
  U32LE		excised;	// excised record count
};
struct FileBitfield {		// bitfield - 2 bits per record
  U64LE		data[fileRNs()>>5];
};
struct FileSuperBlk {		// super block
  U64LE		data[fileIndices()];	// encoded type+offset - SuperElem
};
struct FileExtent {		// file extent header
  U64LE		value;			// encoded type+length - Extent
};
struct FileIndexBlk {		// index block
  U64LE		data[indexRNs() + 1];	// encoded type+offset - IndexElem
};
struct FileIdxBlkTrlr {		// index block trailer
  RNLE		rn;		// base RN
  MagicLE	magic;		// committed()
};
struct FileRecTrlr {		// record trailer
  UNLE		un;		// update number
  RNLE		rn;
  RNLE		prevRN;		// prevRN
  RNLE		opRN;		// opRN
  U16LE		op;		// operation
  U16LE		state;		// state (mutable)
  MagicLE	magic;		// committed()
};

inline constexpr unsigned fileMinSize()	{
  return sizeof(FileHdr) + sizeof(FileBitfield) + sizeof(FileSuperBlk);
}
inline constexpr unsigned fileIdxBlkSize() {
  return sizeof(FileExtent) + sizeof(FileIndexBlk) + sizeof(FileIdxBlkTrlr);
}
inline constexpr unsigned fileRecMinSize() {
  return sizeof(FileExtent) + sizeof(FileRecTrlr);
}
#pragma pack(pop)

// Note: all offsets in superblock and index block reference extent header

// RN states used in bitfield and index block
namespace RNState {
  enum {
    Uninitialized = 0,
    Allocated,
    Deleted,
    Excised
  };
}

// super block element - encodes type and offset for index block
struct SuperElem {
  uint64_t	value = 0;

  enum {
    Uninitialized = 0,
    Excised,
    IndexBlk			// must be last
  };

  ZuAssert(sizeof(FileSuperBlk) >= IndexBlk);

  int type() const { return value < IndexBlk ? value : IndexBlk; }

  // absolute offset of index block's FileExtent
  uint64_t offset() const {
    return value >= IndexBlk ? value : 0;
  }
};

// index block element - encodes RNState and record offset
struct IndexElem {
  uint64_t	value = 0;

  IndexElem(int state, uint64_t offset) : value{(offset<<2) | state} { }

  int state() const { return value & 0x3; }

  // absolute offset of record's FileExtent
  uint64_t offset() const { return value>>2; }
};

// extent header - encodes type and inclusive length for journal extent
struct Extent {
  uint64_t	value = 0;

  enum {
    Uninitialized = 0,
    IndexBlk,
    Record			// must be last
  };

  ZuAssert(sizeof(FileRecTrlr) >= Record);
  ZuAssert(sizeof(FileIndexBlk) + sizeof(FileIdxBlkTrlr) >= Record);

  int type() const { return value < Record ? value : Record; }

  // extent length exclusive of FileExtent header
  uint64_t length() const {
    if (value >= Record) return value;
    switch (value) {
      case IndexBlk:	return sizeof(FileIndexBlk) + sizeof(FileIdxBlkTrlr);
      default:		return 0;
    }
  }
};

// -- file index block cache

// trigger delete processing from FileMgr::write_()

// recover deletes in recover()
// - if a Delete is recovered, use temporary tree to prevent
// recovery of subsequent Deletes in the same sequence, by remembering
// the opRN as each is recovered
//
// schedule any deletion needed once recovery completes

// record lifecycle has following states
//
// Created -> Deleting -> Deleted // put/append
// Deleting -> Deleted // delete/purge
//
//   deleting count updates on initial delete request - count of records with state Deleting
//   deleted count updates on delete request completion - count of records with state Deleted that are candidates for defragging

// initial puts/deletes - write the RN, enqueue the pending delete with origRN=RN and RN=prevRN
// pending deletes keyed on RN
//   { RN, origRN }
//   - read the RN
//   - process the state transition diagram, using origRN as appropriate
//   - set origRN to RN and RN to prevRN/opRN before re-enqueuing
// initial purge - write the RN, set purgeRN to target minRN
// pending purges (one per DB, not a container)
//   purgeRN (nullRN means no purge in process)
//   - write Deleted to minRN
//   - ++minRN
//   - re-enqueue if minRN < purgeRN

// recovery prevents recovering more than the lowest-RN pending deletion
// in a single sequence via the opRN, using a set keyed on opRN -
// when tail is reached, set is cleared

// Note: *prevRN means
// prevRN != nullRN() && prevRN >= minRN && op == (Append | Delete)
// during recovery, recovery will rewrite the prevRN to null if prevRN
// is < minRN

// W(state) - write state
// WN(state) - write state, opRN=nextRN
// WM(state) - write state, opRN=minRN

//		Put		Append		Delete		Purge
//
// New		W(Created)	W(Created)	W(Deleting)	WM(Purging)
// New(*prevRN)	Delete(*prevRN)			Delete(*prevRN)	Purge(*minRN++)
//
// Created(*prevRN) -		-		WN(Deleting)	W(Deleted)
// 						Delete(*prevRN)	Purge(*minRN++)
// Created(!*prevRN) -		-		WN(Deleted)
// 						Delete(*opRN)
//
// Deleting	-		-		W(Deleted)	W(Deleted)
// 						Delete(*opRN)	
// 
// Deleted	-		-		Nop		Nop
//
// Purging	-		-		-		W(Purged)
// 								Purge(*minRN++)
//
// Purged	-		-		-		Nop
// 								Purge(*minRN++)

// 		Alloc8d	Deleted	Bitfld	Offset
//
// Null		N	N	0	0
// Created	Y	N	1	N
// Deleting	Y	N	1	N
// Deleted	Y	Y	2	N
// Excised	Y	Y	3	1

//
// Null
// Created		++allocated
// Deleted|Purged	++deleted
// Excised		++excised

// defragging migrates deleted/purged to unavailable

class File_;

struct Excision;

struct IndexBlk_ {
#pragma pack(push, 1)
  struct Blk {
    IndexElem	data[indexRNs() + 1];	// RN -> offset
  };
#pragma pack(pop)

  uint64_t	id = 0;		// (RN>>indexShift())
  uint64_t	offset = 0;	// absolute offset of this block's FileExtent
  Blk		blk;

  static uint64_t mkID(RN rn) { return rn>>indexShift(); }

  IndexBlk_() { }
  IndexBlk_(uint64_t id_, uint64_t offset_) : id{id_}, offset{offset_} {
    // shortcut endian conversion when initializing a blank index block
    memset(&blk, 0, sizeof(Blk));
  }

  ~IndexBlk_() { }

  static uint64_t IDAxor(const IndexBlk_ &blk) { return blk.id; }

  RN rn() const { return id<<indexShift(); }

  void reset() {
    ~IndexBlk_();
    new (this) IndexBlk_{};
  }
  void init(uint64_t id_, uint64_t offset_) {
    ~IndexBlk_();
    new (this) IndexBlk_{id_, offset_};
  }

  IndexElem elem(RN rn) const {
    ZmAssert((rn>>indexShift()) == m_id);

    unsigned i = rn & indexMask();

    return blk.data[i].state();
  }

  void alloc(RN rn, uint64_t offset, uint64_t length) {
    unsigned i = rn & indexMask();

    ZmAssert((rn>>indexShift()) == m_id);
    ZmAssert(!i || (
	  blk.data[i - 1].state() != RNState::Uninitialized &&
	  blk.data[i - 1].offset() >= fileMinSize() + fileIdxBlkSize() &&
	  blk.data[i - 1].offset() <= offset));
    ZmAssert(!i || (
	  blk.data[i].state() == RNState::Uninitialized &&
	  blk.data[i].offset() == offset));
    ZmAssert(
	  blk.data[i + 1].state() == RNState::Uninitialized &&
	  !blk.data[i + 1].offset());

    blk.data[i] = IndexElem{RNState::Allocated, offset};
    blk.data[i + 1] = IndexElem{RNState::Uninitialized, offset + length};
  }

  // rollback alloc()
  void free(RN rn) {
    unsigned i = rn & indexMask();

    ZmAssert((rn>>indexShift()) == m_id);

    blk.data[i] = IndexElem{RNState::Uninitialized, blk.data[i].offset()};
    blk.data[i + 1] = IndexElem{RNState::Uninitialized, 0};
  }

  void del(RN rn) {
    unsigned i = rn & indexMask();

    ZmAssert((rn>>indexShift()) == m_id);
    ZmAssert(
	blk.data[i].state() == RNState::Allocated &&
	blk.data[i].offset() >= fileMinSize() + fileIdxBlkSize());
    ZmAssert(blk.data[i + 1].offset() >=
	blk.data[i].offset() + sizeof(FileExtent) + sizeof(FileRecTrlr));

    blk.data[i] = IndexElem{RNState::Deleted, blk.data[i].offset()};
  }

  void skip(RN rn) {
    unsigned i = rn & indexMask();

    ZmAssert((rn>>indexShift()) == m_id);
    ZmAssert(!i || (
	  blk.data[i].state() == RNState::Uninitialized &&
	  blk.data[i].offset() == offset));
    ZmAssert(
	  blk.data[i + 1].state() == RNState::Uninitialized &&
	  !blk.data[i + 1].offset());

    blk.data[i] = IndexElem{RNState::Excised, offset};
    blk.data[i + 1] = IndexElem{RNState::Uninitialized, offset};
  }

  // void excise(Excision &excision);

  bool operator !() const { return !offset; }
  ZuOpBool

  IndexBlk_(const IndexBlk_ &) = delete;
  IndexBlk_ &operator =(const IndexBlk_ &) = delete;
  IndexBlk_(IndexBlk_ &&) = delete;
  IndexBlk_ &operator =(IndexBlk_ &&) = delete;
};
inline constexpr const char *IndexBlk_HeapID() { return "Zdb.IndexBlk"; }
using IndexBlkCache =
  ZmCache<IndexBlk_,
    ZmCacheNode<IndexBlk_,
      ZmCacheKey<IndexBlk_::IDAxor,
	ZmCacheHeapID<IndexBlk_HeapID>>>>;
using IndexBlk = IndexBlkCache::Node;

// -- file cache

class FileMgr;

class ZdbAPI File_ : public ZiFile {
  File_() = delete;
  File_(const File_ &) = delete;
  File_(File_ &&) = delete;
  File_ &operator =(const File_ &) = delete;
  File_ &operator =(File_ &&) = delete;

  using Bitfield = ZuBitfield<fileRNs(), 2>;
  struct SuperBlk {
    SuperElem	data[fileIndices()];	// offsets to index blocks
  };

friend FileMgr;

protected:
  File_(FileMgr *mgr, uint64_t id) : m_mgr{mgr}, m_id{id} { }

public:
  RN rn() const { return m_id<<fileShift(); }
  uint64_t id() const { return m_id; }

  static uint64_t IDAxor(const File_ &file) { return file.id(); }

  void alloc(RN rn) {
    ZmAssert((rn>>fileShift()) == m_id);
    ZmAssert(!(rn & fileRNMask()) ||
	m_bitfield[(rn - 1) & fileRNMask()] != RNState::Uninitialized);

    unsigned i = rn & fileRNMask();
    if (m_bitfield[i] == RNState::Uninitialized) alloc_(i);
  }
private:
  void alloc_(unsigned i) {
    m_bitfield[i] = RNState::Allocated;
    ++m_allocated;
  }
public:

  // rollback alloc()
  void free(RN rn) {
    ZmAssert((rn>>fileShift()) == m_id);

    unsigned i = rn & fileRNMask();
    m_bitfield[i] = RNState::Uninitialized;
    --m_allocated;
  }

  void del(RN rn) {
    ZmAssert((rn>>fileShift()) == m_id);
    ZmAssert(!(rn & fileRNMask()) ||
	m_bitfield[(rn - 1) & fileRNMask()] != RNState::Uninitialized);

    unsigned i = rn & fileRNMask();
    if (m_bitfield[i] == RNState::Allocated) del_(i);
  }
private:
  void del_(unsigned i) {
    m_bitfield[i] = RNState::Deleted;
    ++m_deleted;
  }
public:

  void excise(RN rn) {
    ZmAssert((rn>>fileShift()) == m_id);
    ZmAssert(!(rn & fileRNMask()) ||
	m_bitfield[(rn - 1) & fileRNMask()] != RNState::Uninitialized);

    unsigned i = rn & fileRNMask();
    if (m_bitfield[i] == RNState::Deleted) excise_(i);
  }
private:
  void excise_(unsigned i) {
    m_bitfield[i] = RNState::Excised;
    ++m_excised;
  }
public:

  int rnState(RN rn) const {
    ZmAssert((rn>>fileShift()) == m_id);

    return m_bitfield[rn & fileRNMask()];
  }

  unsigned allocated() const { return m_allocated; }
  unsigned deleted() const { return m_deleted; }
  unsigned excised() const { return m_excised; }

  void skip(RN rn) {
    ZmAssert((rn>>fileShift()) == m_id);
    ZmAssert(!(rn & fileRNMask()) ||
	m_bitfield[(rn - 1) & fileRNMask()] != RNState::Uninitialized);

    m_bitfield[rn & fileRNMask()] = RNState::Excised;
    ++m_allocated;
    ++m_deleted;
    ++m_excised;
  }

  void skipBlk(uint64_t blkID) {
    ZmAssert((blkID>>(fileShift() - indexShift())) == m_id);
    ZmAssert(!(blkID & fileMask()) ||
	m_superBlk.data[(blkID - 1) & fileMask()].type() !=
	  SuperElem::Uninitialized);

    m_superBlk.data[blkID & fileMask()] = {SuperElem::Excised};
  }

  uint64_t offset() const { return m_offset; }

  uint64_t append(unsigned length) {
    uint64_t offset = m_offset;
    m_offset += length;
    return offset;
  }

  uint64_t indexBlkOffset(uint64_t blkID);	// returns 0 if non-existent

  ZuPair<bool, IndexBlk *> readIndexBlk(uint64_t blkID);
  ZuPair<bool, IndexBlk *> writeIndexBlk(uint64_t blkID);

  bool readIndexBlk(IndexBlk *);
  bool writeIndexBlk(IndexBlk *);

  bool scan();
  bool sync();
  bool sync_();

private:
  void reset();

  FileMgr		*m_mgr = nullptr;
  uint64_t		m_id = 0;	// (RN>>fileShift())

  uint64_t		m_offset = 0;	// append offset
  uint32_t		m_flags = 0;
  unsigned		m_allocated = 0;
  unsigned		m_deleted = 0;
  unsigned		m_excised = 0;
  Bitfield		m_bitfield;
  SuperBlk		m_superBlk;
};
inline constexpr const char *File_HeapID() { return "Zdb.File"; }
using FileCache =
  ZmCache<File_,
    ZmCacheNode<File_,
      ZmCacheKey<File_::IDAxor,
	ZmCacheHeapID<File_HeapID>>>>;
using File = FileCache::Node;

struct Excision {
  IndexBlk_	*blk;		// current block
  RN		rn;		// current RN
  uint64_t	excised = 0;	// excised bytes
};

#if 0
inline void IndexBlk_::excise(Excision &excision) {
  unsigned i = excision.rn & indexMask();

  ZmAssert(this == excision.blk);
  ZmAssert(this->rn() == excision.rn & ~indexMask());
  ZmAssert(blk.data[i].state() == RNState::Deleted && blk.data[i].offset());

  auto origOff = blk.data[i].offset;

  ZmAssert(origOff > fileMinSize() + sizeof(FileIndexBlk) + excision.excised);
  ZmAssert(blk.data[i + 1].offset() >= origOffset);

  blk.data[i] = IndexElem{RNState::Excised, origOffset - excision.excised};
  excision.excised += (blk.data[i + 1].offset() - origOffset);
  ++excision.rn;
}
#endif

// -- pending deletes

// Deletes is a R/B tree of pending deletes, keyed by the deletion
// request RN - the value is the origRN of the previous deletion -
// deletion progresses backwards along the prevRN sequence marking all
// prior records within the sequence as pending deletion and
// updating the opRN to point forward, then moves forwards finalizing
// the deletions using the previously written opRN chain
inline constexpr const char *DeletesHeapID() { return "Zdb.Deletes"; }
using Deletes =
  ZmRBTreeKV<RN, RN,	// targetRN, origRN
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<DeletesHeapID>>>;

// As recovery progresses, RecDeletes follows recovered deletion chains
// that were previously recovered in order to prevent recovering multiple
// pending deletion operations for the same chain of records
inline constexpr const char *RecDeletesHeapID() { return "Zdb.RecDeletes"; }
using RecDeletes =
  ZmRBTree<RN,
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<RecDeletesHeapID>>>;

namespace FileFlags {
  enum {
    Exists	= 0x1,
    Active	= 0x2,
    Fragmented	= 0x4,
    Cached	= 0x8
  };
}
// file bitfield
inline constexpr const char *AllFilesHeapID() { return "Zdb.AllFiles"; }
class AllFiles {
  enum { Shift = 10 };
  enum { Size = (1<<Shift) };
  enum { Mask = (1<<Shift) - 1 };
  struct Bitfield : public ZuBitfield<Size, 4> {
    unsigned	count = 0;
  };
  using Tree =
    ZmRBTreeKV<uint64_t, Bitfield,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<AllFilesHeapID>>>;

public:
  AllFiles() = default;
  ~AllFiles() = default;
  AllFiles(const AllFiles &b) = delete;
  AllFiles &operator =(const AllFiles &b) = delete;
  AllFiles(AllFiles &&b) = delete;
  AllFiles &operator =(AllFiles &&b) = delete;

  // all(lambda) iterates over bitfields from minimum ID to maximum,
  // calling lambda for all files with non-zero bits
  //
  // lambda(uint64_t fileID, unsigned fileFlags) -> bool
  // - should return true to end iteration, or false to continue
  template <typename L>
  void all(L l) {
    auto i = m_tree.readIterator();
    while (auto node = i.iterate()) {
      uint64_t id = (node->key())<<Shift;
      auto &bitField = node->val();
      for (unsigned j = 0; j < Size; j++) {
	auto &field = bitField[j];
	if (flags && !l(id + j, field)) return;
      }
    }
  }

  // gets bits
  unsigned get(uint64_t fileID) {
    if (auto node = m_tree.find(static_cast<uint64_t>(fileID>>Shift)))
      return node->val().get(fileID & Mask);
    else
      return 0;
  }

  // sets bits
  void set(uint64_t fileID, unsigned v) {
    if (auto node = m_tree.find(static_cast<uint64_t>(fileID>>Shift))) {
      auto &bitfield = node->val();
      fileID &= Mask;
      unsigned w = bitfield.get(fileID);
      if (v &= ~w) {
	if (!w) ++bitfield.count;
	bitfield.set(fileID, w | v);
      }
    } else if (v) {
      auto node = new Tree::Node{};
      node->key() = fileID>>Shift;
      auto &bitfield = node->val();
      fileID &= Mask;
      bitfield.set(fileID, v);
      ++bitfield.count;
      m_tree.addNode(node);
    }
  }
  // clears bits
  void clr(uint64_t fileID, unsigned v) {
    if (auto node = m_tree.find(static_cast<uint64_t>(fileID>>Shift))) {
      auto &bitfield = node->val();
      fileID &= Mask;
      unsigned w = bitfield.get(fileID);
      if (v &= w) {
	if (v == w && !--bitfield.count) {
	  m_tree.delNode(node);
	  return;
	}
	bitfield.set(fileID, w & ~v);
      }
    }
  }

private:
  Tree	m_tree;
};

class ZdbAPI FileMgr {
friend File_;

protected:
  // open/close
  bool open_();
  void close_();

  // database ID (used for error logging)
  virtual ZuID id() = 0;

  // recovered - called from within open_()
  virtual void recovered(ZmRef<Buf> buf) = 0;

  // schedule vacuum
  virtual void scheduleVacuum() = 0;
  void vacuum(unsigned batchSize);

  void warmup_();

  ZmRef<Buf> read_(RN rn);
  bool write_(ZmRef<Buf> buf);

  void del_write(RN rn);
  RN del_prevRN(RN rn);

private:
  template <bool Write> ZuPair<File *, IndexBlk *> rn2file(RN rn);

  ZmRef<Buf> read_(File *, IndexBlk *, RN);

  template <bool Create> File *getFile(uint64_t id);
  template <bool Create> File *openFile(uint64_t id);
  template <bool Create> File *openFile_(const ZiFile::Path &, uint64_t id);

  template <bool Create> IndexBlk *getIndexBlk(File *, uint64_t id);

  void delFile(File *file);

  void fileRdError_(File *, ZiFile::Offset, int, ZeError e);
  void fileWrError_(File *, ZiFile::Offset, ZeError e);

  // immutable
  ZiFile::Path dirName(uint64_t id) const;
  ZiFile::Path fileName(ZiFile::Path dir, uint64_t id) const {
    return ZiFile::append(dir, ZuStringN<12>() <<
	ZuBox<unsigned>{id & 0xfffffU}.hex<false, ZuFmt::Right<5>>() << ".zdb");
  }
  ZiFile::Path fileName(uint64_t id) const {
    return fileName(dirName(id), id);
  }

private:
  RN			m_allocRN = 0;

  // index block cache
  IndexBlkCache		m_indexBlks;

  // file cache
  FileCache		m_files;

  // pending deletions
  Deletes		m_deletes;

  // recovered deletions
  RecDeletes		m_recDeletes;

  // pending purge
  RN			m_purgeRN = nullRN();
  RN			m_purgeOpRN = nullRN();

  // sparse bitfield of files
  AllFiles		m_allFiles;
  uint64_t		m_fragmented = 0;	// count of fragmented files
};

inline ZiFile::Path FileMgr::dirName(uint64_t id) const
{
  return ZiFile::append(m_env->config().path, ZuStringN<8>() <<
      ZuBox<unsigned>{id>>20}.hex<false, ZuFmt::Right<5>>());
}

} // Zdb_

#endif /* ZdbFile_HPP */
