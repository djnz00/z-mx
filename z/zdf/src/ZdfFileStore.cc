//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series File I/O

#include <zlib/ZdfFileStore.hh>

#include <zlib/ZtRegex.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiDir.hh>

using namespace ZdfFileStore;

void FileStore_::init(ZmScheduler *sched, const ZvCf *cf)
{
  FileStore_::Config config{cf};
  BufMgr::init(config.maxBufs);
  m_sched = sched;
  m_dir = config.dir;
  m_coldDir = config.coldDir;
  m_sid = sched->sid(config.thread);
  if (!m_sid || m_sid > sched->params().nThreads())
    throw ZtString{} <<
      "ZdfFileStore thread misconfigured: " <<
      config.thread;
  m_files = new FileHash{};
  m_maxFileSize = config.maxFileSize;
  m_maxOpenFiles = m_files->size();
}

void FileStore_::final()
{
  m_lru.clean();
  m_files->clean();
  Store::final();
}

void FileStore_::open(
    unsigned seriesID, ZuString parent, ZuString name, OpenFn openFn)
{
  ZiFile::Path path = ZiFile::append(m_dir, parent);
  if (m_series.length() <= seriesID) m_series.length(seriesID + 1);
  m_series[seriesID] = SeriesFile{
    .path = path,
    .name = name,
    .fileBlks = (m_maxFileSize > BufSize) ? (m_maxFileSize / BufSize) : 1
  };
  ZiDir dir;
  {
    ZeError e;
    if (dir.open(path, &e) != Zi::OK) {
      openFn(ZeMEVENT(Error, ([path, e](auto &s) {
	s << "ZiDir::open(\"" << path << "\") failed: " << e;
      })));
      return;
    }
  }
  ZiDir::Path fileName;
  unsigned minIndex = UINT_MAX;
  ZtString regex_{name};
  ZtREGEX("\\E").sg(regex_, ZuPP_Q("\\E"));
  regex_ = ZtString{} <<
    ZuPP_Q("^\Q") << regex_ << '_' << ZuPP_Q("\E[0-9a-f]{8}\.sdb$");
  ZtRegex regex{regex_};
  while (dir.read(fileName) == Zi::OK) {
#ifdef _WIN32
    ZtString fileName_{fileName};
#else
    auto &fileName_ = fileName;
#endif
    try {
      if (!regex.m(fileName_)) continue;
    } catch (const ZtRegexError &e) {
      ZeLOG(Error, ([e](auto &s) { s << e; }));
      continue;
    } catch (...) {
      continue;
    }
    ZuBox<unsigned> index{ZuFmt::Hex<>{}, fileName_};
    if (index < minIndex) minIndex = index;
  }
  if (minIndex == UINT_MAX) minIndex = 0;
  m_series[seriesID].minFileIndex = minIndex;
  unsigned blkOffset = minIndex * m_series[seriesID].fileBlks;
  openFn(OpenData{.blkOffset = blkOffset});
}

void FileStore_::close(unsigned seriesID, CloseFn closeFn)
{
  auto i = m_lru.iterator();
  ZmRef<File> fileRef; // need to keep ref count +ve during loop iteration
  while (auto file = static_cast<File *>(i.iterate())) {
    if (file->id.seriesID() == seriesID) {
      i.del();
      fileRef = m_files->del(file->id); // see above comment
    }
  }
  closeFn(CloseResult{});
}

ZmRef<File> FileStore_::getFile(const FileID &fileID, bool create)
{
  ++m_fileLoads;
  ZmRef<File> file;
  if (file = m_files->find(fileID)) {
    m_lru.pushNode(m_lru.delNode(file.ptr()));
    return file;
  }
  ++m_fileMisses;
  file = openFile(fileID, create);
  if (ZuUnlikely(!file)) return nullptr;
  while (m_lru.count_() >= m_maxOpenFiles) {
    auto node = m_lru.shift();
    m_files->del(static_cast<File *>(node)->id);
  }
  m_files->addNode(file);
  m_lru.pushNode(file);
  return file;
}

ZmRef<File> FileStore_::openFile(const FileID &fileID, bool create)
{
  ZmRef<File> file = new File{fileID};
  unsigned fileSize = m_series[fileID.seriesID()].fileSize();
  ZiFile::Path path = fileName(fileID);
  if (file->open(path, ZiFile::GC, 0666, fileSize, nullptr) == Zi::OK)
    return file;
  if (!create) return nullptr;
  bool retried = false;
  ZeError e;
retry:
  if (file->open(path, ZiFile::Create | ZiFile::GC,
	0666, fileSize, &e) != Zi::OK) {
    if (retried) {
      ZeLOG(Error, ([path, e](auto &s) {
	s << "ZdfFileStore could not open or create \""
	  << path << "\": " << e;
      }));
      return nullptr; 
    }
    ZiFile::Path dir = ZiFile::dirname(path);
    ZiFile::mkdir(ZiFile::dirname(dir));
    ZiFile::mkdir(ZuMv(dir));
    retried = true;
    goto retry;
  }
  return file;
}

void FileStore_::archiveFile(const FileID &fileID)
{
  ZiFile::Path name = fileName(fileID);
  ZiFile::Path coldName = ZiFile::append(m_coldDir, name);
  name = ZiFile::append(m_dir, name);
  ZeError e;
  if (ZiFile::rename(name, coldName, &e) != Zi::OK) {
    ZeLOG(Error, ([name, coldName, e](auto &s) {
      s << "ZdfFileStore could not rename \"" << name << "\" to \""
	<< coldName << "\": " << e;
    }));
  }
}

bool FileStore_::loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &hdr)
{
  int r;
  ZeError e;
  FilePos pos = this->pos(seriesID, blkIndex);
  FileID fileID{seriesID, pos.index()};
  ZmRef<File> file = getFile(fileID, false);
  if (!file) return false;
  if (ZuUnlikely((r = file->pread(
	    pos.offset(), &hdr, sizeof(Hdr), &e)) < (int)sizeof(Hdr))) {
    fileRdError_(fileID, pos.offset(), r, e);
    return false;
  }
  return true;
}

bool FileStore_::load(unsigned seriesID, unsigned blkIndex, void *buf)
{
  int r;
  ZeError e;
  FilePos pos = this->pos(seriesID, blkIndex);
  FileID fileID{seriesID, pos.index()};
  ZmRef<File> file = getFile(fileID, false);
  if (!file) return false;
  if (ZuUnlikely((r = file->pread(
	    pos.offset(), buf, BufSize, &e)) < (int)BufSize)) {
    fileRdError_(fileID, pos.offset(), r, e);
    return false;
  }
  return true;
}

void FileStore_::save(ZmRef<Buf> buf)
{
  auto buf_ = buf.ptr();
  buf_->save([buf = ZuMv(buf)]() {
    auto this_ = static_cast<FileStore *>(buf->mgr);
    this_->run([buf = ZuMv(buf)]() mutable {
      auto buf_ = buf.ptr();
      buf_->save_([buf = ZuMv(buf)]() {
	auto this_ = static_cast<FileStore *>(buf->mgr);
	this_->save_(buf->seriesID, buf->blkIndex, buf->data());
      });
    });
  });
}

void FileStore_::save_(unsigned seriesID, unsigned blkIndex, const void *buf)
{
  int r;
  ZeError e;
  FilePos pos = this->pos(seriesID, blkIndex);
  FileID fileID{seriesID, pos.index()};
  ZmRef<File> file = getFile(fileID, true);
  if (!file) return;
  if (ZuUnlikely((r = file->pwrite(
	    pos.offset(), buf, BufSize, &e)) != Zi::OK))
    fileWrError_(fileID, pos.offset(), e);
}

void FileStore_::purge(unsigned seriesID, unsigned blkIndex)
{
  BufMgr::purge(seriesID, blkIndex);
  FilePos pos = this->pos(seriesID, blkIndex);
  {
    auto i = m_lru.iterator();
    ZmRef<File> fileRef; // need to keep ref count +ve during loop iteration
    while (auto file = static_cast<File *>(i.iterate())) {
      if (file->id.seriesID() == seriesID &&
	  file->id.index() < pos.index()) {
	i.del();
	fileRef = m_files->del(file->id); // see above comment
      }
    }
  }
  for (unsigned i = m_series[seriesID].minFileIndex, n = pos.index();
      i < n; i++)
    archiveFile(FileID{seriesID, i});
  m_series[seriesID].minFileIndex = pos.index();
}

void FileStore_::loadDF(
  ZuString name_, Zfb::Load::LoadFn fbLoadFn,
  unsigned maxFileSize, LoadFn loadFn)
{
  ZiFile::Path name{name_};
  name += ZiFile::Path{".df"};
  ZiFile::Path path = ZiFile::append(m_dir, name);
  ZeError e;
  if (Zfb::Load::load(path, ZuMv(fbLoadFn), maxFileSize, &e) != Zi::OK) {
    if (e.errNo() == ZiENOENT)
      loadFn(LoadResult{});
    else
      loadFn(LoadResult{ZeMEVENT(Error, ([path, e](auto &s) {
	s << "Zfb::Load::load(\"" << path << "\") failed: " << e;
      }))});
  } else
    loadFn(LoadResult{LoadData{}});
}

void FileStore_::saveDF(ZuString name_, Zfb::Builder &fbb, SaveFn saveFn)
{
  ZiFile::Path name{name_};
  name += ZiFile::Path{".df"};
  ZiFile::Path path = ZiFile::append(m_dir, name);
  ZeError e;
  if (Zfb::Save::save(path, fbb, 0666, &e) != Zi::OK)
    saveFn(SaveResult{ZeMEVENT(Error, ([path, e](auto &s) {
      s << "Zfb::Save::save(\"" << path << "\") failed: " << e;
    }))});
  else
    saveFn(SaveResult{});
}

void FileStore_::fileRdError_(
  const FileID &fileID, ZiFile::Offset off, int r, ZeError e)
{
  if (r < 0) {
    ZeLOG(Error, ([name = fileName(fileID), off, e](auto &s) {
      s << "ZdfFileStore pread() failed on \"" << name
	<< "\" at offset " << ZuBoxed(off) <<  ": " << e;
    }));
  } else {
    ZeLOG(Error, ([name = fileName(fileID), off](auto &s) {
      s << "ZdfFileStore pread() truncated on \"" << name
	<< "\" at offset " << ZuBoxed(off);
    }));
  }
}

void FileStore_::fileWrError_(
  const FileID &fileID, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ([name = fileName(fileID), off, e](auto &s) {
    s << "ZdfFileStore pwrite() failed on \"" << name
      << "\" at offset " << ZuBoxed(off) <<  ": " << e;
  }));
}
