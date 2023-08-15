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
// that indexes up to 128 index blocks, each a 1.5Kb index block indexing
// up to 128 individual records with {u64 offset, u32 length} tuples
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
//   expunged:u32)
// 24,+512 bitfield (FileBitfield) (16384 x u2)
// 536,+1024 superblock (FileSuperBlk) (128 x u64)
// ... followed by index blocks interleaved with variable length records
// Offset,+1536 index block (FileIndexBlk) (128 x {u64,u32})

namespace Zdb_ {

// file format constants
inline constexpr unsigned fileShift()	{ return 14; }
inline constexpr unsigned fileMask()	{ return 0x7fU; }
inline constexpr unsigned fileIndices()	{ return 128; }
inline constexpr unsigned fileRecs()	{ return 16384; }
inline constexpr unsigned fileRecMask()	{ return 0x3fff; }
inline constexpr unsigned indexShift()	{ return 7; }
inline constexpr unsigned indexMask()	{ return 0x7fU; }
inline constexpr unsigned indexRecs()	{ return 128; }

namespace FileFlags {
  enum {
    IOError	= 0x00000001,	// I/O error during open
    Clean	= 0x00000002	// closed cleanly
  };
}

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
  U32LE		expunged;	// expunged record count
};
struct FileBitfield {		// bitfield - 2 bits per record
  U64LE		data[fileRecs()>>5];
};
struct FileSuperBlk {		// super block
  U64LE		data[fileIndices()];	// encoded type+offset - IdxBlkOff
};
struct FileExtent {		// file extent header
  U64LE		value;			// encoded type+length - Extent
};
struct FileIndexBlk {		// index block
  U64LE		data[indexRecs()];	// encoded type+offset - RecOffset
};
struct FileIdxBlkTrlr {		// index block trailer
  RNLE		rn;		// base RN
  MagicLE	magic;		// committed()
};
struct FileRecTrlr {		// record trailer
  RNLE		rn;
  RNLE		prevRN;		// prevRN
  RNLE		opRN;		// opRN
  U16LE		op;		// operation
  U16LE		state;		// state (mutable)
  MagicLE	magic;		// committed()
};
#pragma pack(pop)

// Note: all offsets in superblock and index block reference extent header

// bitfield states
namespace BitField {
  enum {
    Uninitialized = 0,
    Created,
    Deleted,
    Expunged
  };
}

// extent header - encodes type and exclusive length for journal extent
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

// super block element - encodes type and offset for index block
struct IdxBlkOff {
  uint64_t	value = 0;

  enum {
    Uninitialized = 0,
    Expunged,
    IndexBlk			// must be last
  };

  ZuAssert(sizeof(FileSuperBlk) >= IndexBlk);

  int type() const { return value < IndexBlk ? value : IndexBlk; }

  // absolute offset of index block's FileExtent
  uint64_t offset() const {
    return value >= IndexBlk ? value : 0;
  }
};

// index block element - encodes type and offset for RN
struct RecOffset {
  uint64_t	value = 0;

  enum {
    Uninitialized = 0,
    Expunged,
    Record			// must be last
  };

  ZuAssert(sizeof(FileIndexBlk) >= Record);

  int type() const { return value < Record ? value : Record; }

  // relative offset (relative to offset of index block's FileExtent)
  uint64_t offset() const {
    return value >= Record ? value : 0;
  }
};

// FIXME - header is clean "as of" a file length, i.e. any append
// invalidates cleanliness

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
// Deleting -> Deleted // delete
// Purging -> Purged // purge
//
//   deleting count updates on initial delete request - count of records with state Deleting
//   deleted count updates on delete request completion - count of records with state Deleted that are candidates for compaction

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

// FIXME - use a write-through cache of mutable record trailers to elide
// file-write of interim recState=Deleting and opRN on short sequences;
// also need to flush this on fileMgr close

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
// Purging	Y	N	1	N
// Purged	Y	Y	2	N
// Expunged	Y	Y	3	1

//
// Null
// Created		++allocated
// Deleted|Purged	++deleted
// Expunged		++expunged

// compaction migrates deleted/purged to unavailable

class File_;
struct IndexBlk_ {
#pragma pack(push, 1)
  struct Blk {
    uint64_t	data[indexRecs()];	// RN -> offset
  };
#pragma pack(pop)

  uint64_t	id = 0;		// (RN>>indexShift())
  uint64_t	offset = 0;	// absolute offset of this block's FileExtent
  Blk		blk;

  IndexBlk_() { }
  IndexBlk_(uint64_t id_, uint64_t offset_) : id{id_}, offset{offset_} {
    // shortcut endian conversion when initializing a blank index block
    memset(&blk, 0, sizeof(Blk));
  }
  IndexBlk_(uint64_t id, uint64_t offset, File_ *, bool &ok); // read from file

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

  using Bitfield = ZuBitfield<fileRecs(), 2>;
  struct SuperBlk {
    uint64_t	data[fileIndices()];	// offsets to index blocks
  };

friend FileMgr;

protected:
  File_(FileMgr *mgr, uint64_t id) : m_mgr{mgr}, m_id{id} { }

public:
  RN rn() const { return m_id<<fileShift(); }
  uint64_t id() const { return m_id; }

  void alloc(unsigned i) { if (!m_bitfield[i]) alloc_(i); }
  void alloc_(unsigned i) { m_bitfield[i] = 1; ++m_allocated; }

  void del(unsigned i) { if (m_bitfield[i] == 1) del_(i); }
  void del_(unsigned i) { m_bitfield[i] = 2; ++m_deleted; }

  void expunge(unsigned i) { if (m_bitfield[i] == 2) expunge_(i); }
  void expunge_(unsigned i) { m_bitfield[i] = 3; ++m_expunged; }

  bool exists(unsigned i) const { return m_bitfield[i] == 1; }

  unsigned allocated() const { return m_allocated; }
  unsigned deleted() const { return m_deleted; }
  unsigned expunged() const { return m_expunged; }

  uint64_t append(unsigned length) {
    uint64_t offset = m_offset;
    m_offset += length;
    return offset;
  }

  IndexBlk *readIndexBlk(uint64_t id);
  IndexBlk *writeIndexBlk(uint64_t id);

  bool readIndexBlk(IndexBlk *);
  bool writeIndexBlk(IndexBlk *);

  bool scan();
  bool sync();
  bool sync_();

  static uint64_t IDAxor(const File_ &file) { return file.id(); }

private:
  void reset();

  FileMgr		*m_mgr = nullptr;
  uint64_t		m_id = 0;	// (RN>>fileShift())

  uint64_t		m_offset = 0;	// append offset
  uint32_t		m_flags = 0;
  unsigned		m_allocated = 0;
  unsigned		m_deleted = 0;
  unsigned		m_expunged = 0;
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

IndexBlk_::IndexBlk_(uint64_t id_, uint64_t offset_, File_ *file, bool &ok) :
  id{id_}, offset{offset_}
{
  ZuAssert(sizeof(FileIndexBlk) == sizeof(IndexBlk::Blk));
  ok = file->readIndexBlk(static_cast<IndexBlk *>(this));
}

// -- on-disk record - file, indexBlk, RN offset within index block

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
  // FIXME
  const IndexBlk::Index &index() const {
    return const_cast<FileRec *>(this)->index();
  }
  // FIXME
  IndexBlk::Index &index() { return m_indexBlk->blk.data[m_indexOff]; }

private:
  File		*m_file = nullptr;
  IndexBlk	*m_indexBlk = nullptr;
  unsigned	m_indexOff = 0;
};

// -- pending deletes

// Deletes is a R/B tree of pending deletes, keyed by the deletion
// request RN - the value is the origRN of the previous deletion -
// deletion progresses backwards along the linkRN sequence marking all
// prior records within the sequence as pending deletion and
// updating the opRN to point forward, then moves forwards finalizing
// the deletions using the previously written opRN chain
inline constexpr const char *DeletesHeapID() { return "Zdb.Deletes"; }
using Deletes =
  ZmRBTreeKV<RN, RN,	// rn, origRN
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

class ZdbAPI FileMgr {
friend File_;

protected:
  // open/close
  bool open_();
  void close_();

  // recovered - called from within open_()
  virtual void recovered(ZmRef<Buf> buf) = 0;

  // immutable
  ZiFile::Path dirName(uint64_t id) const;
  ZiFile::Path fileName(ZiFile::Path dir, uint64_t id) const {
    return ZiFile::append(dir, ZuStringN<12>() <<
	ZuBox<unsigned>{id & 0xfffffU}.hex<false, ZuFmt::Right<5>>() << ".zdb");
  }
  ZiFile::Path fileName(uint64_t id) const {
    return fileName(dirName(id), id);
  }

  void warmup_(ZdbRN nextRN);

  ZmRef<Buf> read_(ZdbRN rn);
  bool write_(ZmRef<Buf> buf);

  void del_write(RN rn);
  RN del_prevRN(RN rn);

private:
  template <bool Write> FileRec rn2file(RN rn);

  ZmRef<Buf> read_(const FileRec &);

  template <bool Create> File *getFile(uint64_t id);
  template <bool Create> File *openFile(uint64_t id);
  template <bool Create> File *openFile_(const ZiFile::Path &, uint64_t id);

  template <bool Create> IndexBlk *getIndexBlk(File *, uint64_t id);

  void delFile(File *file);

  void fileRdError_(File *, ZiFile::Offset, int, ZeError e);
  void fileWrError_(File *, ZiFile::Offset, ZeError e);

private:
  uint64_t		m_lastFile = 0;

  // index block cache
  IndexBlkCache		m_indexBlks;

  // file cache
  FileCache		m_files;

  // in-progress deletes
  Deletes		m_revDeletes;	// reversing back to head
  Deletes		m_fwdDeletes;	// going forward to tail

  // recovered deletions
  RecDeletes		m_recDeletes;

  // pending purge
  RN			m_purgeRN = nullRN();
  RN			m_purgeOpRN = nullRN();
};

inline ZiFile::Path FileMgr::dirName(uint64_t id) const
{
  return ZiFile::append(m_env->config().path, ZuStringN<8>() <<
      ZuBox<unsigned>{id>>20}.hex<false, ZuFmt::Right<5>>());
}

} // Zdb_

#endif /* ZdbFile_HPP */
