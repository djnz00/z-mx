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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Z Database file format and I/O

#include <zlib/ZdbFile.hpp>

#include <zlib/ZiDir.hpp>

#define FileMgr_Assert(x, r) \
    ZeAssert(x, (dbID = this->id(), rn), "DB=" << dbID << " RN=" << rn, r)

namespace Zdb_ {

bool FileMgr::open_()
{
  ZeError e;
  ZiDir::Path subName;
  ZtBitWindow<1> subDirs;
  // main directory
  {
    ZiDir dir;
    if (dir.open(m_path) != Zi::OK) {
      if (ZiFile::mkdir(m_path, &e) != Zi::OK) {
	ZeLOG(Fatal, ([path = m_path, e](auto &s) { s << path << ": " << e; }));
	return false;
      }
      return true;
    }
    while (dir.read(subName) == Zi::OK) {
#ifdef _WIN32
      ZtString subName_{subName};
#else
      const auto &subName_ = subName;
#endif
      try {
	if (!ZtREGEX("^[0-9a-f]{5}$").m(subName_)) continue;
      } catch (const ZtRegexError &e) {
	ZeLOG(Error, ([e](auto &s) { s << e; }));
	continue;
      } catch (...) {
	continue;
      }
      ZuBox<unsigned> subIndex;
      subIndex.scan<ZuFmt::Hex<>>(subName_);
      subDirs.set(subIndex);
    }
    dir.close();
  }
  // subdirectories
  return subDirs.all([&](unsigned i, bool) {
#ifdef _WIN32
    ZtString subName_;
#else
    auto &subName_ = subName;
#endif
    subName_ = ZuBox<unsigned>{i}.hex<false, ZuFmt::Right<5>>();
    subName = ZiFile::append(m_path, subName_);
    ZiDir::Path fileName;
    ZtBitWindow<1> files;
    {
      ZiDir subDir;
      if (subDir.open(subName, &e) != Zi::OK) {
	ZeLOG(Error, ([subName, e](auto &s) { s << subName << ": " << e; }));
	return true;
      }
      while (subDir.read(fileName) == Zi::OK) {
#ifdef _WIN32
	ZtString fileName_{fileName};
#else
	auto &fileName_ = fileName;
#endif
	try {
	  if (!ZtREGEX("^[0-9a-f]{5}\.zdb$").m(fileName_)) continue;
	} catch (const ZtRegexError &e) {
	  ZeLOG(Error, ([e](auto &s) { s << e; }));
	  continue;
	} catch (...) {
	  continue;
	}
	ZuBox<unsigned> fileIndex;
	fileIndex.scan<ZuFmt::Hex<>>(fileName_);
	files.set(fileIndex);
      }
      subDir.close();
    }
    // data files
    return files.all([&](unsigned j, bool) {
#ifdef _WIN32
      ZtString fileName_;
#else
      auto &fileName_ = fileName;
#endif
      uint64_t id = (static_cast<uint64_t>(i)<<20) | j;
      if (File *file = openFile_<false>(fileName_, id))
	return recover(file);
      return false;
    });
  });
}

// file has been scanned by scan() prior to recover()
bool FileMgr::recover(File *file)
{
  bool ok = recover_(file);

  // FIXME - if !ok we should not add to allFiles, schedule vacuum, etc.

  // FIXME - check fragmentation
  // FIXME - set Fragmented flag, update fragmented count
  m_allFiles.set(id, FileFlags::Exists | FileFlags::Active | FileFlags::Cached);

  // schedule vacuum if needed
  m_recDeletes.clean();
  if (m_deletes.count()) scheduleVacuum();

  m_allocRN = rn;
  return ok;
}

bool FileMgr::recover_(File *file)
{
  if (!file->allocated()) return true;
  if (file->deleted() >= fileRNs()) { delFile(file); return true; }

  IndexBlk_ indexBlk;

  RN rn = file->rn();
  unsigned n = file->allocated();

  for (unsigned j = 0; j < n; j++, rn++) {
    {
      int rnState = file->rnState(j);
      if (rnState == RNState::Uninitialized) break;
      if (rnState == RNState::Excised) continue;
    }

    auto indexBlkID = rn>>indexShift();
    if (!indexBlk || indexBlk.id != indexBlkID) {
      auto offset = file->indexBlkOffset(indexBlkID);

      FileMgr_Assert(offset, false);

      indexBlk.init(indexBlkID, offset);
      bool readIndexBlkOK = file->readIndexBlk(indexBlk)

      FileMgr_Assert(readIndexBlkOK, false);
    }

    if (auto buf = read_(file, &indexBlk, rn)) {
      auto record = record_(msg_(buf->hdr()));
      // rebuild pending purge / deletions from record
      switch (static_cast<int>(record->state())) {
	case RecState::Created:
	  invoke([buf = ZuMv(buf)]() mutable { recovered(ZuMv(buf)); });
	  break;
	case RecState::Deleting:
	  if (record->op() == Op::Purge) {
	    m_purgeRN = record->rn();
	    m_purgeOpRN = record->opRN();
	  } else {
	    if (!m_recDeletes.del(record->rn())) {
	      ZmAssert(record->prevRN() != nullRN());
	      m_deletes.add(record->prevRN(), record->rn());
	    }
	    if (record->opRN() != nullRN())
	      m_recDeletes.add(record->opRN());
	  }
	  break;
	case RecState::Deleted:
	  if (record->op() != Op::Purge) {
	    m_deletes.del(record->rn());
	    if (record->opRN() != nullRN())
	      m_deletes.add(record->opRN(), record->rn());
	  }
	  break;
	default:
	  break;
      }
    }
  }

  return true;
}

void FileMgr::close_()
{
  // index blocks
  m_indexBlks.all<true>([this](IndexBlk *blk) {
    auto fileID = (blk->id)>>(fileShift() - indexShift());
    if (File *file = getFile<true>(fileID))
      file->writeIndexBlk(blk);
  });

  // files
  m_files.all<true>([this](File *file) {
    file->sync();
    m_allFiles.clr(file->id(), FileFlags::Cached);
  });
}

bool FileMgr::checkpoint()
{
  bool ok = true;
  m_indexBlks.all([this, &ok](IndexBlk *blk) mutable {
    auto fileID = (blk->id)>>(fileShift() - indexShift());
    if (File *file = getFile<true>(fileID))
      ok = ok && file->writeIndexBlk(blk);
    else
      ok = false;
  });
  m_files.all([&ok](File *file) mutable { ok = ok && file->sync_(); });
  return ok;
}

void File_::reset()
{
  m_flags = 0;
  m_allocated = m_deleted = m_excised = 0;
  m_bitfield.zero();
  memset(&m_superBlk.data[0], 0, sizeof(FileSuperBlk));
}

// FIXME compact will have a similar structure to scan

bool File_::scan()
{
  uint64_t size = this->size();
  // if file is truncated below minimum header size, reset/rewrite it
  if (size < sizeof(FileHdr) + sizeof(FileBitfield) + sizeof(FileSuperBlk)) {
    reset();
    return sync_();
  }
  FileHdr hdr;
  int r;
  ZeError e;
  // header
  {
    if (ZuUnlikely((r = pread(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(static_cast<File *>(this), 0, r, e);
      return false;
    }
    // validate header
    if (hdr.magic != Magic::file() ||
	hdr.version != ZdbVersion ||
	hdr.clean > size ||
	hdr.rn != rn()) return false;
  }
  // if clean { set record counts; read bitfield; read superblock; return; }
  if (hdr.clean == size) {
    // set record counts
    m_allocated = hdr.allocated;
    m_deleted = hdr.deleted;
    m_excised = hdr.excised;
    // read bitfield
    {
#if Zu_BIGENDIAN
      FileBitfield bitfield;
      auto bitfieldData = &(bitfield.data[0]);
#else
      auto bitfieldData = &(m_bitfield.data[0]);
#endif
      if (ZuUnlikely((r = pread(
		sizeof(FileHdr),
		&bitfieldData[0], sizeof(FileBitfield), &e)) != Zi::OK)) {
	m_mgr->fileRdError_(static_cast<File *>(this), sizeof(FileHdr), r, e);
	return false;
      }
#if Zu_BIGENDIAN
      for (unsigned i = 0; i < Bitfield::Words; i++)
	m_bitfield.data[i] = bitfieldData[i];
#endif
    }
    // read superblock
    {
#if Zu_BIGENDIAN
      FileSuperBlk superBlk;
      auto superData = &(superBlk.data[0]);
#else
      auto superData = &m_superBlk.data[0];
#endif
      if (ZuUnlikely((r = pread(
		sizeof(FileHdr) + sizeof(FileBitfield),
		&superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
	m_mgr->fileRdError_(static_cast<File *>(this),
	    sizeof(FileHdr) + sizeof(FileBitfield), r, e);
	return false;
      }
#if Zu_BIGENDIAN
      for (unsigned i = 0; i < fileIndices(); i++)
	m_superBlk.data[i] = superData[i];
#endif
    }
    m_offset = size();
    return true;
  }
  // scan file extents, rebuilding bitfield, counts, superblock and index blocks
  m_offset = fileMinSize();
  IndexBlk_ indexBlk; // current index block
  while (m_offset < size) {
    int type;
    uint64_t length;
    // read extent header
    {
      FileExtent extent_;
      if (ZuUnlikely((r = pread(m_offset,
		&extent_, sizeof(FileExtent), &e)) != Zi::OK)) {
	m_mgr->fileRdError_(static_cast<File *>(this), offset, r, e);
	return false;
      }
      Extent extent{extent_.value};
      type = extent.type();
      length = extent.length();
    }
    // validate extent header
    if (type == Extent::Uninitialized || !length || (m_offset + length) > size)
      goto eof;
    switch (type) {
      case Extent::IndexBlk: {
	// read index block trailer
	FileIdxBlkTrlr trlr;
	if (ZuUnlikely((r = pread(m_offset + length - sizeof(FileIdxBlkTrlr),
		  &trlr, sizeof(FileIdxBlkTrlr), &e)) != Zi::OK)) {
	  m_mgr->fileRdError_(static_cast<File *>(this), m_offset, r, e);
	  return false;
	}
	RN rn = trlr.rn;
	{
	  RN allocRN = rn() + m_allocated;
	  // write out index block if needed
	  if (indexBlk && indexBlk.id() != (allocRN>>indexShift())) {
	    if (!writeIndexBlk(&indexBlk)) return false;
	    indexBlk.reset();
	  }
	  // validate trailer
	  if (trlr.magic != ZdbCommitted ||
	      rn < allocRN ||
	      rn >= this->rn() + fileRNs() - indexRNs()) goto eof;
	  // gap-fill from last-allocated RN to this RN
	  while (allocRN < rn) {
	    skip(allocRN);
	    if (indexBlk) indexBlk.skip(allocRN);
	    ++allocRN;
	    if (!(allocRN & indexMask())) {
	      if (!indexBlk) {
		skipBlk(allocRN - (1<<indexShift()));
	      } else {
		if (!writeIndexBlk(&indexBlk)) return false;
		indexBlk.reset();
	      }
	    }
	  }
	}
	indexBlk.init(rn>>indexShift(), m_offset);
	m_superBlk.data[indexBlk.id & fileMask()] = indexBlk.offset;
	m_offset += length;
      } break;
      case Extent::Record: {
	if (!indexBlk) goto eof; // records must be preceded by index blocks
	// read record trailer
	FileRecTrlr trlr;
	if (ZuUnlikely((r = pread(m_offset + length - sizeof(FileRecTrlr),
		  &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	  m_mgr->fileRdError_(static_cast<File *>(this), m_offset, r, e);
	  return false;
	}
	RN rn = trlr.rn;
	{
	  RN allocRN = rn() + m_allocated;
	  // validate trailer
	  if (trlr.magic != ZdbCommitted ||
	      rn < allocRN ||
	      rn < indexBlk.rn() ||
	      rn >= indexBlk.rn() + indexRNs()) goto eof;
	  // gap-fill from last-allocated RN to this RN
	  while (allocRN < rn) {
	    skip(allocRN);
	    indexBlk.skip(allocRN);
	    ++allocRN;
	  }
	}
	// rebuild bitfield from record
	switch (static_cast<int>(trlr.state)) {
	  case RecState::Created:
	  case RecState::Deleting:
	    m_bitfield[rn & fileRNMask()] = RNState::Created;
	    ++m_allocated;
	    break;
	  case RecState::Deleted:
	    m_bitfield[rn & fileRNMask()] = RNState::Deleted;
	    ++m_allocated;
	    ++m_deleted;
	    break;
	}
	// rebuild index block from record
	indexBlk.blk[rn & indexMask()] = m_offset - indexBlk.offset;
      } break;
    }
  }
eof:
  if (m_offset < size) {
    if (truncate(m_offset) != Zi::OK) return false;
  }
  if (indexBlk) {
    if (!writeIndexBlk(&indexBlk)) return false;
    // indexBlk.reset();
  }
  return sync_();
}

bool File_::sync_()
{
  int r;
  ZeError e;
  // header
  {
    FileHdr hdr{
      .magic = Magic::file(),
      .version = ZdbVersion,
      .clean = m_offset,
      .rn = rn(),
      .allocated = m_allocated,
      .deleted = m_deleted,
      .excised = m_excised
    };
    if (ZuUnlikely((r = pwrite(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      return false;
    }
  }
  // bitfield
  {
#if Zu_BIGENDIAN
    FileBitfield bitfield;
    auto bitfieldData = &(bitfield.data[0]);
    for (unsigned i = 0; i < Bitfield::Words; i++)
      bitfieldData[i] = m_bitfield.data[i];
#else
    auto bitfieldData = &(m_bitfield.data[0]);
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr),
	      &bitfieldData[0], sizeof(FileBitfield), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      return false;
    }
  }
  // superblock
  {
#if Zu_BIGENDIAN
    FileSuperBlk superBlk;
    auto superData = &(super.data[0]);
    for (unsigned i = 0; i < fileIndices(); i++)
      superData[i] = m_superBlk.data[i];
#else
    auto superData = &m_superBlk.data[0];
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr) + sizeof(FileBitfield),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this),
	  sizeof(FileHdr) + sizeof(FileBitfield), e);
      return false;
    }
  }
  return true;
}

bool File_::sync()
{
  if (!sync_()) goto error;
  {
    ZeError e;
    if (ZiFile::sync(&e) != Zi::OK) {
      m_mgr->fileWrError_(static_cast<File *>(this), 0, e);
      goto error;
    }
  }
  return true;

error:
  FileHdr hdr{
    .magic = Magic::file(),
    .version = ZdbVersion,
    .clean = 0,			// force re-scan on next open
    .rn = rn(),
    .allocated = m_allocated,
    .deleted = m_deleted,
    .excised = m_excised
  };
  pwrite(0, &hdr, sizeof(FileHdr));
  return false;
}

template <bool Create>
File *FileMgr::getFile(uint64_t id)
{
  File *file = nullptr;
  m_files.find(id,
    [&file](File *file_) { file = file_; },
    [this]<typename L>(uint64_t id, L l) {
      File *file = openFile<Create>(id);
      if (ZuUnlikely(!file)) { l(nullptr); return; }
      l(file);
    }, [](File *file) {
      file->sync();
      m_allFiles.clr(file->id(), FileFlags::Cached);
    });
  return file;
}

template <bool Create>
File *FileMgr::openFile(uint64_t id)
{
  return openFile_<Create>(fileName(dirName(id), id), id);
}

template <bool Create>
File *FileMgr::openFile_(const ZiFile::Path &name, uint64_t id)
{
  ZuPtr<File> file = new File{this, id};
  if (file->open(name, ZiFile::GC, 0666) == Zi::OK) {
    if (!file->scan()) return nullptr;
    goto ret;
  }
  if constexpr (!Create) return nullptr;
  ZiFile::mkdir(dirName(id)); // pre-emptive idempotent
  ZeError e;
  auto fileSize = sizeof(FileHdr) + sizeof(FileBitfield) + sizeof(FileSuperBlk);
  if (file->open(
	name, ZiFile::Create | ZiFile::GC, 0666, fileSize, &e) != Zi::OK) {
    ZeLOG(Fatal, ([name, e](auto &s) {
      s << "Zdb could not open or create \"" << name << "\": " << e;
    }));
    return nullptr;
  }
  if (!file->sync_()) {
    file->close();
    ZiFile::remove(fileName(id));
    return nullptr;
  }
ret:
  return file.release();
}

template <bool Create>
IndexBlk *FileMgr::getIndexBlk(File *file, uint64_t id)
{
  ZmAssert(file->id() == (id>>(fileShift() - indexShift())));

  IndexBlk *blk;
  m_indexBlks.find(id,
    [&blk](IndexBlk *blk_) { blk = blk_; },
    [file]<typename L>(uint64_t id, L l) {
      if constexpr (Create) {
	auto [ok, indexBlk] = file->writeIndexBlk(id);
	l(ok ? indexBlk : nullptr);
      } else {
	auto [ok, indexBlk] = file->readIndexBlk(id);
	l(ok ? indexBlk : nullptr);
      }
    }, [this](IndexBlk *blk) {
      auto fileID = (blk->id)>>(fileShift() - indexShift());
      if (File *file = getFile<Create>(fileID))
	file->writeIndexBlk(blk);
    });
  return blk;
}

// creates/caches file and index block as needed
template <bool Write>
ZuPair<File *, IndexBlk *> FileMgr::rn2file(RN rn)
{
  File *file = getFile<Write>(rn>>fileShift());
  if (!file) return {};
  if constexpr (Write) {
    ZmAssert(file->rnState(rn) == RNState::Uninitialized);
  } else {
    if (file->rnState(rn) != RNState::Allocated) return {};
  }
  IndexBlk *indexBlk = getIndexBlk<Write>(file, rn>>indexShift());
  if (!indexBlk) return {};
#ifndef NDEBUG
  if constexpr (Write) {
    ZmAssert(indexBlk->elem(rn).state() == RNState::Uninitialized);
    ZmAssert(indexBlk->elem(rn).offset() == file->offset());
    ZmAssert(!(rn & indexMask()) ||
	indexBlk->elem(rn - 1).state != RNState::Unitialized);
  } else {
    ZmAssert(indexBlk->elem(rn).state() == RNState::Allocated);
    ZmAssert(indexBlk->elem(rn + 1).offset() > indexBlk->elem(rn).offset());
  }
#endif
  return {file, indexBlk};
}

uint64_t File_::indexBlkOffset(uint64_t blkID)
{
  const auto &superElem = m_superBlk.data[blkID & fileMask()];
  switch (static_cast<int>(superElem.type())) {
    case Uninitialized:
    case Excised:
      return 0;
  }
  return superElem.offset();
}

ZuPair<bool, IndexBlk *> File_::readIndexBlk(uint64_t blkID)
{
  ZuPtr<IndexBlk> indexBlk;

  // attempt read
  auto offset = indexBlkOffset(blkID);
  if (!offset) return {true, nullptr};
  indexBlk = new IndexBlk{blkID, offset};
  if (!readIndexBlk(*indexBlk)) return {false, nullptr};
  return {true, indexBlk.release()};
}

ZuPair<bool, IndexBlk *> File_::writeIndexBlk(uint64_t blkID)
{
  ZuPtr<IndexBlk> indexBlk;

  // attempt read
  if (auto offset = indexBlkOffset(blkID)) {
    indexBlk = new IndexBlk{blkID, offset};
    if (!readIndexBlk(*indexBlk)) return {false, nullptr};
    return {true, indexBlk.release()};
  }

  // index blk does not exist, ensure this is an append, attempt to write it
  unsigned super = blkID & fileMask();
  const auto &superElem = m_superBlk.data[super];

  // validate append
  ZmAssert(superElem.type() == SuperElem::Uninitialized);
  ZmAssert(!super ||
      m_superBlk.data[super - 1].type() != SuperElem::Uninitialized);

  // attempt write
  auto offset = append(fileIdxBlkSize());
  m_superBlk.data[super] = SuperElem{offset};
  indexBlk = new IndexBlk{blkID, offset};
  if (!writeIndexBlk(*indexBlk)) {
    m_offset -= fileIdxBlkSize();
    return {false, nullptr};
  }

  return {true, indexBlk.release()};
}

bool File_::readIndexBlk(IndexBlk_ &indexBlk)
{
  int r;
  ZeError e;
  auto offset = indexBlk.offset;
  // read extent header
  {
    FileExtent extent;
    if (ZuUnlikely((r = file->pread(
	      offset, &extent, sizeof(FileExtent), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(file, offset, e);
      return false;
    }
    if (ZuUnlikely(Extent{extent.value}.type() != Extent::IndexBlk))
      return false;
  }
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
#else
  auto indexData = &indexBlk.blk.data[0];
#endif
  if (ZuUnlikely((r = pread(indexBlk.offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_mgr->fileRdError_(static_cast<File *>(this), indexBlk.offset, r, e);
    return false;
  }
#if Zu_BIGENDIAN
  for (unsigned i = 0; i < indexRNs(); i++)
    indexBlk.blk.data[i] = { indexData[i].offset, indexData[i].length };
#endif
  {
    FileIdxBlkTrlr trlr;
    if (ZuUnlikely((r = file->pread(
	      offset, &trlr, sizeof(FileIdxBlkTrlr), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(file, offset, e);
      return false;
    }
    if (ZuUnlikely(
	  trlr.rn != indexBlk.rn() ||
	  trlr.magic != Magic::committed()))
      return false;
  }
  return true;
}

bool File_::writeIndexBlk(const IndexBlk_ &indexBlk)
{
  int r;
  ZeError e;
  auto offset = indexBlk.offset;
  // write extent header
  {
    FileExtent extent{Extent::IndexBlk};
    if (ZuUnlikely((r = file->pwrite(
	      offset, &extent, sizeof(FileExtent), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(file, offset, e);
      return false;
    }
    offset += sizeof(FileExtent);
  }
  {
#if Zu_BIGENDIAN
    FileIndexBlk fileIndexBlk;
    auto indexData = &(fileIndexBlk.data[0]);
    for (unsigned i = 0; i < indexRNs(); i++)
      indexData[i] = indexBlk.blk.data[i];
#else
    auto indexData = &indexBlk.blk.data[0];
#endif
    if (ZuUnlikely((r = pwrite(
	      offset, &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), indexBlk.offset, e);
      return false;
    }
    offset += sizeof(FileIndexBlk);
  }
  {
    FileIdxBlkTrlr trlr{
      .rn = indexBlk.rn(),
      .magic = Magic::committed()
    };
    if (ZuUnlikely((r = file->pwrite(
	      offset, &extent, sizeof(FileExtent), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(file, offset, e);
      return false;
    }
  }
  return true;
}

void FileMgr::warmup_()
{
  rn2file<true>(m_allocRN);
}

// read individual record from disk into buffer
ZmRef<Buf> FileMgr::read_(RN rn)
{
  auto [file, indexBlk] = rn2file<false>(rn);
  if (!file) return {};
  return read_(file, indexBlk, rn);
}

ZmRef<Buf> FileMgr::read_(File *file, IndexBlk *indexBlk, RN rn);
{
  uint64_t offset, length;
  {
    auto indexElem = indexBlk->elem(rn);

    FileMgr_Assert(indexElem.state() == RNState::Allocated, {});

    offset = indexElem.offset();
    length = indexBlk->elem(rn + 1).offset();

    FileMgr_Assert(length >= offset + fileRecMinSize(), {});

    length -= offset;
  }

  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  unsigned dataLen = 0;
  uint8_t *ptr = nullptr;

  if (length > fileRecMinSize()) {
    dataLen = length - fileRecMinSize();
    data = Zfb::Save::pvector_(fbb, dataLen, ptr);

    FileMgr_Assert(!data.IsNull() && ptr, {});
  }

  int r;
  ZeError e;
  ZiVec vecs[3];
  unsigned nVecs = 0;
  FileExtent extent;
  FileRecTrlr trlr;

  ZiVec_init(vecs[nVecs++], &extent, sizeof(FileExtent));
  if (dataLen) ZiVec_init(vecs[nVecs++], ptr, dataLen);
  ZiVec_init(vecs[nVecs++], &trlr, sizeof(FileRecTrlr));

  // read header, data, trailer
  if (ZuUnlikely((r = file->preadv(offset, vecs, nVecs, &e)) != Zi::OK)) {
    fileRdError_(file, offset, r, e);
    return {};
  }

  // check the trailer was committed
  if (ZuUnlikely(trlr.magic != ZdbCommitted)) return {};

#ifndef NDEBUG
  // validate the extent header
  {
    Extent extent_{extent.value};

    ZmAssert(extent_.type() == Extent::Record);
    ZmAssert(extent_.length() == length);
  }

  // validate the trailer
  ZmAssert(
      trlr.state == RecState::Created ||
      trlr.state == RecState::Deleting);
#endif

  auto id = Zfb::Save::id(config().id);
  auto un = Zfb::Save::zdb_un(trlr.un);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb, &id,
	&un, trlr.rn, trlr.prevRN, trlr.opRN,
	trlr.op, trlr.state, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
// - updates file bitfield, index block and appends the record on disk
bool FileMgr::write_(ZmRef<Buf> buf)
{
  auto record = record_(msg_(buf->hdr()));
  RN rn = record->rn();
  if (m_allocRN > rn) return true; // idempotent

  File *file = nullptr;
  IndexBlk *indexBlk = nullptr;

  // gap fill, skipping empty index blocks and files
  if (m_allocRN < rn) {
    if ((m_allocRN & fileRNMask()) ||
	(m_allocRN>>fileShift()) == (rn>>fileShift())) {
      file = getFile<true>(m_allocRN>>fileShift());
      if (!file) return false;
    }
    if (file) {
      if ((m_allocRN & indexMask()) ||
	  (m_allocRN>>indexShift()) == (rn>>indexShift())) {
	indexBlk = getIndexBlk<true>(file, m_allocRN>>indexShift());
	if (!indexBlk) return false;
      }
    }
    do {
      if (file) file->skip(m_allocRN);
      if (indexBlk) indexBlk->skip(m_allocRN);
      ++m_allocRN;
      if (!(m_allocRN & fileRNMask())) {
	indexBlk = nullptr;
	if ((m_allocRN>>fileShift()) != (rn>>fileShift())) {
	  file = nullptr;
	} else {
	  file = getFile<true>(m_allocRN>>fileShift());
	  if (!file) return false;
	}
      }
      if (file) {
	if (!(m_allocRN & indexMask())) {
	  if ((m_allocRN>>indexShift()) != (rn>>indexShift())) {
	    indexBlk = nullptr;
	    file->skipBlk(m_allocRN);
	  } else {
	    indexBlk = getIndexBlk<true>(file, m_allocRN>>indexShift());
	    if (!indexBlk) return false;
	  }
	}
      }
    } while (m_allocRN < rn);
  }

  // ensure file and index block are provisioned
  if (!file) {
    file = getFile<true>(rn>>fileShift());
    if (!file) return false;
  } else {
    ZmAssert(file->id() == (rn>>fileShift()));
  }
  if (!indexBlk) {
    indexBlk = getIndexBlk<true>(file, rn>>indexShift());
    if (!indexBlk) return false;
  } else {
    ZmAssert(indexBlk->id() == (rn>>indexShift()));
  }

  auto data = Zfb::Load::bytes(record->data());
  auto length = sizeof(FileExtent) + data.length() + sizeof(FileRecTrlr);
  auto offset = file->append(length);

  // update bitfield and index block
  file->alloc(rn);
  indexBlk->alloc(rn, offset, length);
  
  bool scheduleVacuum = false;

  switch (static_cast<int>(record->state())) {
    case RecState::Deleting:
      vacuum = true;
      // update pending purge / deletions
      switch (static_cast<int>(record->op())) {
	case Op::Purge:
	  m_purgeRN = record->rn();
	  m_purgeOpRN = record->opRN();
	  break;
	case Op::Delete:
	  m_deletes.add(record->prevRN(), record->rn());
	  break;
      }
      break;
    case RecState::Deleted:
    case RecState::Purged:
      file->del(rn);
      indexBlk->del(rn);
      break;
  }

  // prepare data for writing to disk
  int r;
  ZeError e;
  ZiVec vecs[3];
  unsigned nVecs = 0;
  FileExtent extent{length};
  FileRecTrlr trlr{
    .un = Zfb::Load::zdb_un(record->un()),
    .rn = rn,
    .prevRN = record->prevRN(),
    .opRN = record->opRN(),
    .op = record->op(),
    .state = record->state(),
    .magic = ZdbCommitted
  };

  ZiVec_init(vecs[nVecs++], &extent, sizeof(FileExtent));
  if (data) ZiVec_init(vecs[nVecs++], data.data(), data.length());
  ZiVec_init(vecs[nVecs++], &trlr, sizeof(FileRecTrlr));

  // write header, data, trailer
  if (ZuUnlikely((r = file->pwritev(offset, vecs, nVecs, &e)) != Zi::OK)) {
    file->free(rn);
    indexBlk->free(rn);
    m_mgr->fileWrError_(file, offset, e);
    return false;
  }

  if (vacuum) this->scheduleVacuum();

  return true;
}

// FIXME from here

// FIXME - in caller - need vacuum max CPU %
// - i.e. skip and reschedule (defer) if file thread is busier than threshold

// FIXME - when creating a new file - update allFiles

void FileMgr::vacuum(unsigned batchSize)
{
  bool defragged = false;
  m_allFiles.all(
      [&defragged]<template Field>(uint64_t fileID, Field &flags) -> bool {
    uint64_t flags_ = flags;
    if (!defragged) {
      if ((flags_ & (
	      FileFlags::Fragmented |
	      FileFlags::Active)) == FileFlags::Fragmented) {
	defrag(fileID); // FIXME - in defrag use Cached flag to find file in cache, or open directly if not cached; also decrement m_fragmented
	defragged = true;
      }
    }
    flags_ &= ~FileFlags::Active;
    return true;
  });

  // unmark all files as active
  //
  // if purge in progress, execute purge {
  //   read record trailer
  //   update to Deleted inc bitfield and indexBlk
  //   update file flags (mark as active, possibly fragmented)
  // } else if m_deletes.count() {
  //   get/del m_deletes.min()
  //   if value > key then moving backwards, otherwise forwards
  //   if (backwards) {
  //     // mark file as active, possibly fragmented
  //     read record trailer
  //     if prevRN {
  //       update to Deleting
  //       write opRN
  //       add new backwards move to m_deletes
  //     } else {
  //       update to Deleted (inc bitfield and indexBlk)
  //       write opRN
  //       add new forwards move to m_deletes
  //     }
  //   } else {
  //     // mark file as active, possibly fragmented
  //     read record trailer
  //     update to Deleted (inc bitfield and indexBlk)
  //     if opRN {
  //       add new forwards move to m_deletes
  //     }
  //   }
  // }
  //
  // reschedule vacuum as needed (if fragmented count remains +ve, etc.)
}

// obtain prevRN for a record pending deletion
RN FileMgr::del_prevRN(RN rn)
{
#if 0
  if (auto object = m_objCache.del(rn)) {
    if (object->committed()) return object->prevRN();
  }
#endif
  auto [file, indexBlk] = rn2file<false>(rn);
  if (!rec) return nullRN();
  const auto &index = rec.index();
  if (ZuUnlikely(index.offset <= 0)) {
    ZeLOG(Error, ([id = config().id, rn = rec.rn()](auto &s) {
      s << "Zdb internal error on DB " << id <<
	" bitfield inconsistent with index for RN " << rn;
    }));
    return nullRN();
  }
  auto file = rec.file();
  auto offset = rec.indexBlk()->offset + index.offset + index.length;
  FileRecTrlr trlr;
  int r;
  ZeError e;
  if (ZuUnlikely((r = file->pread(offset,
	    &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
    fileRdError_(file, offset, r, e);
    return nullRN();
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return nullRN();
  return trlr.prevRN;
}

// delete individual record
void FileMgr::del_write(RN rn)
{
  auto [file, indexBlk] = rn2file<false>(rn);
  if (!rec) return;
  rec.index().offset = -rec.index().offset;
  auto file = rec.file();
  if (file->del(rn & fileRNMask()))
    delFile(file);
  else {
    // FIXME - when compacting, superblock needs updating in memory
    // and writing to disk on completion (can be skipped on initial
    // write of compacted file, along with file header)
    //
    // FIXME - move this logic to post cache eviction
    // FIXME - only compact if file is fully allocated
    // FIXME - maintain container keyed by ID of pending compactions,
    // check that container in file cache loadFn
    // FIXME - incrementally compact in small batches from min to max ID
    // FIXME - enqueue compaction batches to file thread
    // FIXME - do not re-enqueue if compaction container is empty
    // FIXME - enqueue idempotently on initial compaction container add
    // on cache load, just remove from compaction container, copy
    // remainder of file, switch files then load into cache
    // FIXME - copy routine uses small buffer for locality, can be tuned
    // in configuration file, suggest 32K as default
    // FIXME - use sharded thread-local allocation for compaction buffer
    auto count = file->allocated() - file->deleted();
    if (count) {
      auto span = file->last() - file->first();
      // FIXME - maintain record of deleted bytes, not just deleted records
      // FIXME - also allocated bytes, in FileHdr (bytes being record data bytes, not including index blocks)
      // FIXME - also compacted bytes
      // FIXME - fragmentation % is then (deleted - compacted) / ((deleted - compacted) + allocated)
      // FIXME - compaction threshold is configurable, default 50%
      if (span > (count<<1)) { // >50% fragmented
      }
    }
  }
}

// compact should 

// -0  +1  -2  +3  -4  +5  -6/0+7/2-8/4

// file flags (4-bit bitfield):

// 1 exists		// file exists
// 2 active		// deletion occurred since last sweep
// 4 fragmented		// fragmentation > threshold
// 8 compacting		// undergoing compaction

// files maintain the following counts:
//    allocated - all allocated records
//    deleted - all deleted records
//    excised - all zero-length records
//
// fragmentation % calculation:
//    (deleted - excised) / ((deleted - excised) + allocated)
//
// 1] mark files as fragmented (or not) following each deletion
// 2] mark files as active following each deletion
// 3] unmark files as active during each vacuum, except currently appending file
// 4] only initiate compaction for files that are both inactive and fragmented
// 5] abort compaction if file becomes active during compaction
//    - remainder of file is copied to compacted file as is, compacted
//      file replaces previous
// 6] compaction simply removes the file if complete and 100% deleted

// parameters:
//   vacuum interval
//   vacuum batch size (number of records processed in each iteration)
//   vacuum max fragmentation %
//   vacuum max CPU %

// delete file - never called for active file (i.e. currently appending file)
void FileMgr::delFile(File *file)
{
  bool lastFile;
  uint64_t id = file->id();
  m_files.delNode(file);
  file->close();
  ZiFile::remove(fileName(id));
  // FIXME - update allFiles
}

// disk read error
void FileMgr::fileRdError_(File *file, ZiFile::Offset off, int r, ZeError e)
{
  if (r < 0) {
    ZeLOG(Error, ([this, id = file->id(), off, e](auto &s) { s <<
	"Zdb pread() failed on \"" << fileName(id) <<
	"\" at offset " << ZuBoxed(off) << ": " << e;
    }));
  } else {
    ZeLOG(Error, ([this, id = file->id(), off](auto &s) { s <<
	"Zdb pread() truncated on \"" << fileName(id) <<
	"\" at offset " << ZuBoxed(off);
    }));
  }
}

// disk write error
void FileMgr::fileWrError_(File *file, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ([this, id = file->id(), off, e](auto &s) { s <<
      "Zdb pwrite() failed on \"" << fileName(id) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e; }));
}

} // namespace Zdb_
