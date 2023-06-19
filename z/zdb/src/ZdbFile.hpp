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

// file hdr, bitmap, superblock are written on file eviction
// index blocks are written on index block eviction
// on close, all files and index blocks are written and evicted

// each record on disk is {data, trailer}, where trailer is the on-disk
// equivalent of the network message header

// 0,+20 header (magic:u32, version:u32, flags:u32, allocated:u32, deleted:u32)
// 20,+256 bitmap (FileBitmap) (16384 x u1)
// 276,+1024 superblock (FileSuperBlk) (128 x u64)
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
};

#define ZdbMagic	0x0db3a61c	// file magic number
#define ZdbCommitted	0xc001da7a	// "cool data"

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

// -- file index block cache

class File_;
struct IndexBlk_ {
#pragma pack(push, 1)
  struct Index {
    int64_t	offset;	// FIXME - use -ve for deleted, 
			// FIXME - make this relative to index block offset
			// FIXME - 
    uint32_t	length;
  };
  struct Blk {
    Index		data[indexRecs()];	// RN -> {offset, length}
  };
#pragma pack(pop)

  IndexBlk_(uint64_t id_, uint64_t offset_) : id{id_}, offset{offset_} {
    // shortcut endian conversion when initializing a blank index block
    memset(&blk, 0, sizeof(Blk));
  }
  IndexBlk_(uint64_t id, uint64_t offset, File_ *, bool &ok); // read from file

  uint64_t	id = 0;			// (RN>>indexShift())
  uint64_t	offset = 0;		// offset of this index block
  Blk		blk;

  static uint64_t IDAxor(const IndexBlk_ &blk) { return blk.id; }

  IndexBlk_() = delete;
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

  using Bitmap = ZuBitmap<fileRecs()>;
  struct SuperBlk {
    uint64_t	data[fileIndices()];	// offsets to index blocks
  };

friend FileMgr;

protected:
  File_(FileMgr *mgr, uint64_t id) : m_mgr{mgr}, m_id{id} { }

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
  Bitmap		m_bitmap;
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
  const IndexBlk::Index &index() const {
    return const_cast<FileRec *>(this)->index();
  }
  IndexBlk::Index &index() { return m_indexBlk->blk.data[m_indexOff]; }

private:
  File		*m_file = nullptr;
  IndexBlk	*m_indexBlk = nullptr;
  unsigned	m_indexOff = 0;
};

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
  bool write_(Buf *buf);

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
};

inline ZiFile::Path FileMgr::dirName(uint64_t id) const
{
  return ZiFile::append(m_env->config().path, ZuStringN<8>() <<
      ZuBox<unsigned>{id>>20}.hex<false, ZuFmt::Right<5>>());
}

} // Zdb_

#endif /* ZdbFile_HPP */
