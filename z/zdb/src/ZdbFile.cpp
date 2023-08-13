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

bool FileMgr::recover(File *file)
{
  if (!file->allocated()) return true;
  if (file->deleted() >= fileRecs()) { delFile(file); return true; }
  // FIXME - compacted
  RN rn = file->rn();
  IndexBlk *blk = nullptr;
  int first = file->first(); // FIXME - don't rely on bitfield for this
  if (ZuUnlikely(first < 0)) return true;
  int last = file->last(); // FIXME - see above
  if (ZuUnlikely(last < 0)) return false; // file corrupt
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    if (!file->exists(j)) continue;
    auto blkID = rn>>indexShift();
    if (!blk || blk->id != blkID) blk = file->readIndexBlk(blkID);
    if (!blk) return false; // I/O error on file
    if (auto buf = read_(
	  FileRec{file, blk, static_cast<unsigned>(rn & indexMask())}))
      auto record = record_(msg_(buf->hdr()));
      if (record->state() != RecState::Created) continue;
      invoke([buf = ZuMv(buf)]() mutable { recovered(ZuMv(buf)); });
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
  m_files.all<true>([](File *file) { file->sync(); });
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
  m_allocated = m_deleted = m_expunged = 0;
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
    m_expunged = hdr.expunged;
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
  m_offset = sizeof(FileHdr) + sizeof(FileBitfield) + sizeof(FileSuperBlk);
  IndexBlk_ indexBlk;
  while (m_offset < size) {
    int type;
    uint64_t length;
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
    if (type == Extent::Uninitialized || !length ||
	(m_offset + sizeof(FileExtent) + length) > size) break;
    switch (type) {
      case Extent::IndexBlk: {
	FileIdxBlkTrlr trlr;
	if (ZuUnlikely((r = pread(
		m_offset + sizeof(FileExtent) + length - sizeof(FileIdxBlkTrlr),
		  &trlr, sizeof(FileIdxBlkTrlr), &e)) != Zi::OK)) {
	  m_mgr->fileRdError_(static_cast<File *>(this), m_offset, r, e);
	  return false;
	}
	RN rn = trlr.rn;
	{
	  RN allocRN = rn() + m_allocated;
	  if (trlr.magic != ZdbCommitted ||
	      rn < allocRN ||
	      rn >= this->rn() + fileRecs() - indexRecs()) break;
	  while (allocRN < rn) {
	    m_bitfield[allocRN & fileRecMask()] = BitField::Expunged;
	    if (indexBlk)
	      indexBlk.blk[allocRN & indexMask()] = RecOffset::Expunged;
	    else if (!(allocRN & indexMask()))
	      m_superBlk.data[(allocRN>>indexShift()) & fileMask()] =
		IdxBlkOff::Expunged;
	    ++m_allocated;
	    ++m_deleted;
	    ++m_expunged;
	    ++allocRN;
	    if (indexBlk) {
	      if (allocRN >= indexBlk.rn() + indexRecs()) {
		if (!writeIndexBlk(&indexBlk)) return false;
		indexBlk.reset();
	      }
	    }
	  }
	}
	indexBlk.init(rn>>indexShift(), m_offset);
	m_superBlk.data[indexBlk.id & fileMask()] = indexBlk.offset;
	m_offset += sizeof(FileExtent) + length;
      } break;
      case Extent::Record: {
	if (!indexBlk) break; // no preceding index block
	FileRecTrlr trlr;
	if (ZuUnlikely((r = pread(
		  m_offset + sizeof(FileExtent) + length - sizeof(FileRecTrlr),
		  &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	  m_mgr->fileRdError_(static_cast<File *>(this), m_offset, r, e);
	  return false;
	}
	RN rn = trlr.rn;
	{
	  RN allocRN = rn() + m_allocated;
	  if (trlr.magic != ZdbCommitted ||
	      rn < allocRN ||
	      rn < indexBlk.rn() ||
	      rn >= indexBlk.rn() + indexRecs()) break;
	  while (allocRN < rn) {
	    m_bitfield[allocRN & fileRecMask()] = BitField::Expunged;
	    indexBlk.blk[allocRN & indexMask()] = RecOffset::Expunged;
	    ++m_allocated;
	    ++m_deleted;
	    ++m_expunged;
	    ++allocRN;
	  }
	}
	switch (static_cast<int>(trlr.state)) {
	  case RecState::Created:
	  case RecState::Deleting:
	  case RecState::Purging:
	    m_bitfield[rn & fileRecMask()] = BitField::Created;
	    ++m_allocated;
	    break;
	  case RecState::Deleted:
	  case RecState::Purged:
	    m_bitfield[rn & fileRecMask()] = BitField::Deleted;
	    ++m_allocated;
	    ++m_deleted;
	    break;
	}
	indexBlk.blk[rn & indexMask()] = m_offset - indexBlk.offset;
	switch (static_cast<int>(trlr.state)) {
	  case RecState::Deleting:
	    if (!m_recDeletes.del(trlr.rn))
	      m_revDeletes.add(trlr.prevRN, trlr.rn);
	    if (trlr.opRN != nullRN())
	      m_recDeletes.add(trlr.opRN);
	    break;
	  case RecState::Deleted:
	    m_fwdDeletes.del(trlr.rn);
	    if (trlr.opRN != nullRN())
	      m_fwdDeletes.add(trlr.opRN, trlr.rn);
	    break;
	  case RecState::Purging:
	    m_purgeRN = trlr.rn;
	    m_purgeOpRN = trlr.opRN;
	    break;
	}
      } break;
    }
  }
  if (indexBlk) {
    if (!writeIndexBlk(&indexBlk)) return false;
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
      .expunged = m_expunged
    };
    if (ZuUnlikely((r = pwrite(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
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
      m_flags |= FileFlags::IOError;
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
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  return true;
}

bool File_::sync() // file thread
{
  m_flags |= FileFlags::Clean;
  if (!sync_()) goto error;
  {
    ZeError e;
    if (ZiFile::sync(&e) != Zi::OK) {
      m_mgr->fileWrError_(static_cast<File *>(this), 0, e);
      m_flags |= FileFlags::IOError;
      goto error;
    }
  }
  m_flags &= ~FileFlags::Clean;
  return true;
error:
  m_flags &= ~FileFlags::Clean;
  FileHdr hdr{
    .magic = Magic::file(),
    .version = ZdbVersion,
    .flags = m_flags,
    .allocated = m_allocated,
    .deleted = m_deleted
  };
  pwrite(0, &hdr, sizeof(FileHdr)); // best effort to clear Clean flag
  m_flags &= ~FileFlags::Clean;
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
      if (id > m_lastFile) m_lastFile = id;
      l(file);
    }, [](File *file) { file->sync(); });
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
    return file.release();
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
  file->sync_();
  return file.release();
}

template <bool Create>
IndexBlk *FileMgr::getIndexBlk(File *file, uint64_t id)
{
  IndexBlk *blk;
  m_indexBlks.find(id,
    [&blk](IndexBlk *blk_) { blk = blk_; },
    [file]<typename L>(uint64_t id, L l) {
      if constexpr (Create)
	l(file->writeIndexBlk(id));
      else
	l(file->readIndexBlk(id));
    }, [this](IndexBlk *blk) {
      auto fileID = (blk->id)>>(fileShift() - indexShift());
      if (File *file = getFile<Create>(fileID))
	file->writeIndexBlk(blk);
    });
  return blk;
}

// creates/caches file and index block as needed
template <bool Write>
FileRec FileMgr::rn2file(RN rn)
{
  uint64_t fileID = rn>>fileShift();
  File *file = getFile<Write>(fileID);
  if (!file) return {};
  if constexpr (!Write) if (!file->exists(rn & fileRecMask())) return {};
  uint64_t indexBlkID = rn>>indexShift();
  IndexBlk *indexBlk = getIndexBlk<Write>(file, indexBlkID);
  if (!indexBlk) return {};
  auto indexOff = static_cast<unsigned>(rn & indexMask());
  return {ZuMv(file), ZuMv(indexBlk), indexOff};
}

IndexBlk *File_::readIndexBlk(uint64_t id)
{
  ZuPtr<IndexBlk> indexBlk;
  if (auto offset = m_superBlk.data[id & indexMask()]) {
    bool ok;
    indexBlk = new IndexBlk{id, offset, this, ok}; // calls readIndexBlk
    if (!ok) return nullptr;
    return indexBlk.release();
  }
  return nullptr;
}

// FIXME
// offsets increase with RNs, index blocks are appended as needed
// offsets are relative - a compactor can iterate over the file, splicing out
// expunged records by setting their offset to a sentinel 1 and reducing
// subsequent offsets in both the superblock and all subsequent index blocks;
// ideally this would be run-length batched; note that offset reduction has to
// occur both on-disk and in-cache - file cache m_offset, superBlk, and
// index blk cache offset

IndexBlk *File_::writeIndexBlk(uint64_t id)
{
  ZuPtr<IndexBlk> indexBlk;
  if (indexBlk = readIndexBlk(id)) return indexBlk.release();
  // FIXME - disambiguate read I/O error from need to append
  // FIXME - check that id is an append of an index block
  auto offset = m_offset;
  // FIXME - write extent header
  m_superBlk.data[id & indexMask()] = offset;
  indexBlk = new IndexBlk{id, offset};
  // FIXME - call writeIndexBlk(indexBlk)
  m_offset = offset + sizeof(FileIndexBlk); // FIXME
  return indexBlk.release();
}

bool File_::readIndexBlk(IndexBlk_ *indexBlk)
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
#else
  auto indexData = &indexBlk->blk.data[0];
#endif
  if (ZuUnlikely((r = pread(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_mgr->fileRdError_(static_cast<File *>(this), indexBlk->offset, r, e);
    return false;
  }
#if Zu_BIGENDIAN
  for (unsigned i = 0; i < indexRecs(); i++)
    indexBlk->blk.data[i] = { indexData[i].offset, indexData[i].length };
#endif
  // FIXME - read and validate FileIdxBlkTrlr
  return true;
}

bool File_::writeIndexBlk(IndexBlk_ *indexBlk)
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
  for (unsigned i = 0; i < indexRecs(); i++)
    indexData[i] = indexBlk->blk.data[i];
#else
  auto indexData = &indexBlk->blk.data[0];
#endif
  if (ZuUnlikely((r = pwrite(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_mgr->fileWrError_(static_cast<File *>(this), indexBlk->offset, e);
    return false;
  }
  // FIXME - write FileIdxBlkTrlr
  return true;
}

void FileMgr::warmup_(ZdbRN rn)
{
  rn2file<true>(rn);
}

// read individual record from disk into buffer
ZmRef<Buf> FileMgr::read_(ZdbRN rn)
{
  if (FileRec rec = rn2file<false>(rn)) return read_(rec);
  return {};
}

ZmRef<Buf> FileMgr::read_(const FileRec &rec)
{
  // FIXME - read FileExtent header
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.length < sizeof(FileRecTrlr))) {
    ZeLOG(Error, ([id = config().id, rn = rec.rn()](auto &s) {
      s << "Zdb internal error on DB " << id <<
	" bitfield inconsistent with index for RN " << rn;
    }));
    return {};
  }
  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  unsigned dataLen = 0;
  uint8_t *ptr = nullptr;
  if (index.length > sizeof(FileRecTrlr)) {
    dataLen = index.length - sizeof(FileRecTrlr);
    data = Zfb::Save::pvector_(fbb, dataLen, ptr);
    if (data.IsNull() || !ptr) return {};
  }
  FileRecTrlr trlr;
  {
    auto file = rec.file();
    auto indexBlk = rec.indexBlk();
    auto offset = indexBlk->offset + index.offset;
    int r;
    ZeError e;
    if (dataLen) {
      // read record
      if (ZuUnlikely((r = file->pread(offset, ptr, dataLen, &e)) != Zi::OK)) {
	fileRdError_(file, offset, r, e);
	return {};
      }
      offset += dataLen;
    }
    // read trailer
    if (ZuUnlikely((r = file->pread(
	      offset, &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileRdError_(file, offset, r, e);
      return {};
    }
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return {};
  auto id = Zfb::Save::id(config().id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb, &id,
	trlr.rn, trlr.prevRN, trlr.opRN, trlr.op, trlr.state, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
// - updates the file bitfield, index block and appends the record on disk
bool FileMgr::write_(ZmRef<Buf> buf)
{
  // FIXME - write FileExtent header
  // FIXME - check for append, any gap needing to be filled, etc.
  // FIXME - DO NOT PERMIT out of order writes
  auto record = record_(msg_(buf->hdr()));
  RN rn = record->rn();
  auto data = Zfb::Load::bytes(record->data());
  {
    FileRec rec = rn2file<true>(rn);
    if (!rec) return false;
    auto file = rec.file();
    auto indexBlk = rec.indexBlk();
    auto &index = rec.index();
    file->alloc(rn & fileRecMask());
    switch (static_cast<int>(record->state())) {
      case RecState::Deleted:
      case RecState::Purged:
	file->del(rn & fileRecMask());
	break;
    }
    index.length = data.length() + sizeof(FileRecTrlr);
    auto offset = file->append(index.length);
    index.offset = offset - indexBlk->offset;
    FileRecTrlr trlr{
      .rn = rn,
      .prevRN = record->prevRN(),
      .opRN = record->opRN(),
      .op = record->op(),
      .state = record->state(),
      .magic = ZdbCommitted
    };
    int r;
    ZeError e;
    if (data) {
      // write record
      if (ZuUnlikely((r = file->pwrite(
		offset, data.data(), data.length(), &e)) != Zi::OK)) {
	fileWrError_(file, offset, e);
	index.offset = 0;
	index.length = 0;
	return false;
      }
      offset += data.length();
    }
    // write trailer
    if (ZuUnlikely((r = file->pwrite(
	      offset, &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileWrError_(file, offset, e);
      index.offset = 0;
      index.length = 0;
      return false;
    }
  }
  return true;
}

// obtain prevRN for a record pending deletion
RN FileMgr::del_prevRN(RN rn)
{
#if 0
  if (auto object = m_objCache.del(rn)) {
    if (object->committed()) return object->prevRN();
  }
#endif
  FileRec rec = rn2file<false>(rn);
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
  FileRec rec = rn2file<false>(rn);
  if (!rec) return;
  rec.index().offset = -rec.index().offset;
  auto file = rec.file();
  if (file->del(rn & fileRecMask()))
    delFile(file);
  else {
    // FIXME - index block offsets are relative to index block itself
    // (eases completion of compaction for remainder of file)
    //
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

compact should 

-0  +1  -2  +3  -4  +5  -6/0+7/2-8/4

// 1] file flags:
//    complete (no longer appending)
//    fragmented (fragmentation > threshold)
//    active (deletion occurred, implies complete) - cleared each sweep
//    compacting (implies inactive, fragmented and complete)
//    graveyard (entirely comprised of tombstones)
//    disused (will be deleted in next vacuum - implies graveyard)
//
// 2] files maintain the following counts:
//    allocated - all allocated records
//    deleted - all deleted records
//    tombstones - all zero-length records (deletions and previously compacted)
//
// 3] fragmentation calculation:
//    fragmentation % is (deleted - tombstones) / ((deleted - tombstones) + allocated)
//
// 4] mark files as fragmented (or not) following each deletion
// 5] mark files as active following each deletion
// 6] unmark files as active during each sweep
// 7] only initiate compaction for files that are inactive for an entire
//    interval and fragmented
// 8] abort compaction if file becomes active during compaction
//    - remainder of file is copied to compacted file as is, compacted
//      file replaces previous
// 9] dead files are also marked for deletion each sweep
// 10] dead files are deleted in order once marked in next sweep

// parameters:
//   vacuum interval
//   vacuum batch size (number of records processed in each iteration)
//   compaction threshold

// delete file
void FileMgr::delFile(File *file)
{
  bool lastFile;
  uint64_t id = file->id();
  m_files.delNode(file);
  lastFile = id == m_lastFile;
  if (ZuUnlikely(lastFile)) getFile<true>(id + 1); // ensure recovery of nextRN
  // FIXME - lastFile will not work, need to preserve headRN even if Deleted
  // in order to ensure RNs continuously increment
  file->close();
  ZiFile::remove(fileName(id));
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
