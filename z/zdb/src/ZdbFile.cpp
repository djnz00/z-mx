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
  RN rn = (file->id())<<fileShift();
  IndexBlk *blk = nullptr;
  int first = file->first();
  if (ZuUnlikely(first < 0)) return true;
  int last = file->last();
  if (ZuUnlikely(last < 0)) return false; // file corrupt
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    if (!file->exists(j)) continue;
    auto blkID = rn>>indexShift();
    if (!blk || blk->id != blkID) blk = file->readIndexBlk(blkID);
    if (!blk) return false; // I/O error on file
    if (auto buf = read_(
	FileRec{file, blk, static_cast<unsigned>(rn & indexMask())}))
      invoke([buf = ZuMv(buf)]() mutable {
	auto db = buf->db();
	recovered(ZuMv(buf));
      });
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
  m_allocated = m_deleted = 0;
  m_bitmap.zero();
  memset(&m_superBlk.data[0], 0, sizeof(FileSuperBlk));
}

// FIXME compact will have a similar structure to scan

bool File_::scan()
{
  // if file is truncated below minimum header size, reset/rewrite it
  if (size() < sizeof(FileHdr) + sizeof(FileBitmap) + sizeof(FileSuperBlk)) {
    reset();
    return sync_();
  }
  int r;
  ZeError e;
  // header
  {
    FileHdr hdr;
    if (ZuUnlikely((r = pread(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(static_cast<File *>(this), 0, r, e);
      return false;
    }
    if (hdr.magic != ZdbMagic) return false;
    if (hdr.version != ZdbVersion) return false;
    m_flags = hdr.flags;
    m_allocated = hdr.allocated;
    m_deleted = hdr.deleted;
  }
  // bitmap
  {
#if Zu_BIGENDIAN
    FileBitmap bitmap;
    auto bitmapData = &(bitmap.data[0]);
#else
    auto bitmapData = &(m_bitmap.data[0]);
#endif
    if (ZuUnlikely((r = pread(
	      sizeof(FileHdr),
	      &bitmapData[0], sizeof(FileBitmap), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(static_cast<File *>(this), sizeof(FileHdr), r, e);
      return false;
    }
#if Zu_BIGENDIAN
    for (unsigned i = 0; i < Bitmap::Words; i++)
      m_bitmap.data[i] = bitmapData[i];
#endif
  }
  // superblock
  {
#if Zu_BIGENDIAN
    FileSuperBlk superBlk;
    auto superData = &(superBlk.data[0]);
#else
    auto superData = &m_superBlk.data[0];
#endif
    if (ZuUnlikely((r = pread(
	      sizeof(FileHdr) + sizeof(FileBitmap),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(static_cast<File *>(this),
	  sizeof(FileHdr) + sizeof(FileBitmap), r, e);
      return false;
    }
#if Zu_BIGENDIAN
    for (unsigned i = 0; i < fileIndices(); i++)
      m_superBlk.data[i] = superData[i];
#endif
  }
  if (m_flags & FileFlags::Clean) {
    m_flags &= ~FileFlags::Clean;
    m_offset = size();
    return true;
  }
  // rebuild count and bitmap from index blocks
  m_allocated = m_deleted = 0;
  m_bitmap.zero();
  bool rewrite = false;
  for (unsigned i = 0; i < fileIndices(); i++) {
    uint64_t indexBlkOffset = m_superBlk.data[i];
    if (!indexBlkOffset) break;
    FileIndexBlk indexBlk;
    if (ZuUnlikely((r = pread(indexBlkOffset,
	      &indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
      m_mgr->fileRdError_(static_cast<File *>(this), indexBlkOffset, r, e);
      return false;
    }
    bool rewriteIndexBlk = false;
    for (unsigned j = 0; j < indexRecs(); j++) {
      uint64_t offset = indexBlk.data[j].offset;
      if (!offset) break;
      if (offset < 0) {
	++m_deleted;
      } else {
	offset += indexBlk->offset + indexBlk.data[j].length;
	FileRecTrlr trlr;
	if (ZuUnlikely((r = pread(offset,
		  &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	  m_mgr->fileRdError_(static_cast<File *>(this), offset, r, e);
	  return false;
	}
	if (trlr.rn != ((id()<<fileShift()) | (i<<indexShift()) | j) ||
	    trlr.magic != ZdbCommitted) {
	  indexBlk.data[j] = { 0, 0 };
	  rewriteIndexBlk = true;
	  break;
	}
	++m_allocated;
	m_bitmap[(i<<indexShift()) | j].set();
      }
    }
    if (rewriteIndexBlk) {
      rewrite = true;
      if (ZuUnlikely((r = pwrite(indexBlkOffset,
		&indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
	m_mgr->fileWrError_(static_cast<File *>(this), indexBlkOffset, e);
	return false;
      }
    }
  }
  m_offset = size();
  return !rewrite || sync_();
}

bool File_::sync_()
{
  int r;
  ZeError e;
  // header
  {
    FileHdr hdr{
      .magic = ZdbMagic,
      .version = ZdbVersion,
      .flags = m_flags,
      .allocated = m_allocated,
      .deleted = m_deleted
    };
    if (ZuUnlikely((r = pwrite(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  // bitmap
  {
#if Zu_BIGENDIAN
    FileBitmap bitmap;
    auto bitmapData = &(bitmap.data[0]);
    for (unsigned i = 0; i < Bitmap::Words; i++)
      bitmapData[i] = m_bitmap.data[i];
#else
    auto bitmapData = &(m_bitmap.data[0]);
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr),
	      &bitmapData[0], sizeof(FileBitmap), &e)) != Zi::OK)) {
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
	      sizeof(FileHdr) + sizeof(FileBitmap),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this),
	  sizeof(FileHdr) + sizeof(FileBitmap), e);
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
    .magic = ZdbMagic,
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
  auto fileSize = sizeof(FileHdr) + sizeof(FileBitmap) + sizeof(FileSuperBlk);
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
// if negative offset implies deleted (as opposed to a single sentinel
// value), then a compactor can iterate over the file, splicing out
// deleted records by reducing their length to 0 and reducing subsequent
// offsets in both the superblock and all subsequent index blocks; ideally
// this would be run-length batched; note that offset reduction has to
// occur both on-disk and in-cache - file cache m_offset, superBlk,
// index blk cache offset, blk

IndexBlk *File_::writeIndexBlk(uint64_t id)
{
  ZuPtr<IndexBlk> indexBlk;
  if (indexBlk = readIndexBlk(id)) return indexBlk.release();
  auto offset = m_offset;
  m_superBlk.data[id & indexMask()] = offset;
  indexBlk = new IndexBlk{id, offset};
  {
    int r;
    ZeError e;
    if (ZuUnlikely((r = pwrite(offset,
	      &indexBlk->blk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
      m_mgr->fileWrError_(static_cast<File *>(this), offset, e);
      return nullptr;
    }
  }
  m_offset = offset + sizeof(FileIndexBlk);
  return indexBlk.release();
}

bool File_::readIndexBlk(IndexBlk *indexBlk)
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
  return true;
}

bool File_::writeIndexBlk(IndexBlk *indexBlk)
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
  for (unsigned i = 0; i < indexRecs(); i++) {
    const auto &index = indexBlk->blk.data[i];
    indexData[i] = { index.offset, index.length };
  }
#else
  auto indexData = &indexBlk->blk.data[0];
#endif
  if (ZuUnlikely((r = pwrite(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_mgr->fileWrError_(static_cast<File *>(this), indexBlk->offset, e);
    return false;
  }
  return true;
}

void FileMgr::warmup(ZdbRN rn)
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
  const auto &index = rec.index();
  if (ZuUnlikely(index.offset <= 0)) {
    ZeLOG(Error, ([id = config().id, rn = rec.rn()](auto &s) {
      s << "Zdb internal error on DB " << id <<
	" bitmap inconsistent with index for RN " << rn;
    }));
    return nullptr;
  }
  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  uint8_t *ptr = nullptr;
  if (index.length) {
    data = Zfb::Save::pvector_(fbb, index.length, ptr);
    if (data.IsNull() || !ptr) return nullptr;
  }
  FileRecTrlr trlr;
  {
    auto file = rec.file();
    auto indexBlk = rec.indexBlk();
    int r;
    ZeError e;
    if (index.length) {
      // read record
      if (ZuUnlikely((r = file->pread(indexBlk->offset + index.offset,
		ptr, index.length, &e)) != Zi::OK)) {
	fileRdError_(file, index.offset, r, e);
	return nullptr;
      }
    }
    // read trailer
    if (ZuUnlikely((r = file->pread(
	      indexBlk->offset + index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileRdError_(file, index.offset + index.length, r, e);
      return nullptr;
    }
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return nullptr;
  auto id = Zfb::Save::id(config().id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb,
	&id, trlr.rn, trlr.prevRN, trlr.seqLenOp, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
// - updates the file bitmap, index block and appends the record on disk
bool FileMgr::write_(Buf *buf)
{
  ZuGuard guard([this, buf]() { m_repBufs->delNode(buf); });

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
    index.offset =
      file->append(data.length() + sizeof(FileRecTrlr)) - indexBlk->offset;
    index.length = data.length();
    FileRecTrlr trlr{
      .rn = rn,
      .prevRN = record->prevRN(),
      .seqLenOp = record->seqLenOp(),
      .magic = ZdbCommitted
    };
    int r;
    ZeError e;
    if (data) {
      // write record
      if (ZuUnlikely((r = file->pwrite(indexBlk->offset + index.offset,
		data.data(), data.length(), &e)) != Zi::OK)) {
	fileWrError_(file, index.offset, e);
	index.offset = 0;
	index.length = 0;
	return false;
      }
    }
    // write trailer
    if (ZuUnlikely((r = file->pwrite(
	      indexBlk->offset + index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileWrError_(file, index.offset + index.length, e);
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
	" bitmap inconsistent with index for RN " << rn;
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
    // FIXME - negative offsets in index block for deleted, not sentinel
    // FIXME - index block offsets are relative to index block itself
    // (eases completion of compaction for remainder of file)
    //
    // FIXME - when compacting, superblock needs updating in memory
    // and writing to disk on completion (can be skipped on initial
    // write of compacted file, along with file header)
    //
    // FIXME - move file I/O into ZdbFile.{hpp,cpp} with unit tests
    // for compaction etc.
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

// FIXME - NEW DESIGN

// record lifecycle:
//
// 0, 0 - uninitialized/unused (only possible if file is incomplete)
// offset, N - record data
// -offset, N - deletion tombstone (uncompacted)
// -offset, 0 - deletion tombstone (compacted)
// a file that is entirely comprised of tombstones is a candidate for removal

// 1] file flags:
//    complete (no longer appending)
//    fragmented (fragmentation > threshold)
//    active (deletion occurred, implies complete) - cleared each sweep
//    compacting (implies inactive, fragmented and complete)
//    tombstone (entirely comprised of tombstones)
//    zombie (will be deleted in next vacuum - implies tombstone)
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
  if (ZuUnlikely(lastFile)) getFile<true>(id + 1);
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
