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

// Data Series File I/O

#include <zlib/ZdfFile.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiDir.hpp>

using namespace Zdf;

void FileMgr::init(ZmScheduler *sched, const ZvCf *cf)
{
  FileMgr::Config config{cf};
  BufMgr::init(config.maxBufs);
  m_sched = sched;
  m_dir = config.dir;
  m_coldDir = config.coldDir;
  m_writeThread = sched->index(config.writeThread);
  if (!m_writeThread ||
      m_writeThread > sched->params().nThreads())
    throw ZtString() <<
      "Zdf::FileMgr writeThread misconfigured: " <<
      config.writeThread;
  m_files = new FileHash{};
  m_maxFileSize = config.maxFileSize;
  m_maxOpenFiles = m_files->size();
}

void FileMgr::final()
{
  m_lru.clean();
  m_files->clean();
  Mgr::final();
}

bool FileMgr::open(
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
    if (dir.open(path, &e) != Zi::OK)
      return e.errNo() == ZiENOENT;
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
      ZeLOG(Error, ZtString() << e);
      continue;
    } catch (...) {
      continue;
    }
    ZuBox<unsigned> index{ZuFmt::Hex<>{}, fileName_};
    if (index < minIndex) minIndex = index;
  }
  if (minIndex == UINT_MAX) minIndex = 0;
  m_series[seriesID].minFileIndex = minIndex;
  openFn(minIndex * m_series[seriesID].fileBlks);
  return true;
}

void FileMgr::close(unsigned seriesID)
{
  auto i = m_lru.iterator();
  ZmRef<File> fileRef; // need to keep ref count +ve during loop iteration
  while (auto file_ = i.iterateNode()) {
    auto file = static_cast<File *>(file_);
    if (file->id.seriesID() == seriesID) {
      i.del();
      fileRef = m_files->del(file->id); // see above comment
    }
  }
}

ZmRef<File> FileMgr::getFile(const FileID &fileID, bool create)
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
    auto node = m_lru.shiftNode();
    m_files->del(static_cast<File *>(node)->id);
  }
  m_files->addNode(file);
  m_lru.pushNode(file);
  return file;
}

ZmRef<File> FileMgr::openFile(const FileID &fileID, bool create)
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
      ZeLOG(Error, ZtString() <<
	  "Zdf::FileMgr could not open or create \"" <<
	  path << "\": " << e);
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

void FileMgr::archiveFile(const FileID &fileID)
{
  ZiFile::Path name = fileName(fileID);
  ZiFile::Path coldName = ZiFile::append(m_coldDir, name);
  name = ZiFile::append(m_dir, name);
  ZeError e;
  if (ZiFile::rename(name, coldName, &e) != Zi::OK) {
    ZeLOG(Error, ZtString() <<
	"Zdf::FileMgr could not rename \"" << name << "\" to \"" <<
	coldName << "\": " << e);
  }
}

bool FileMgr::loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &hdr)
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

bool FileMgr::load(unsigned seriesID, unsigned blkIndex, void *buf)
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

void FileMgr::save(ZmRef<Buf> buf)
{
  buf->save([this, buf = ZuMv(buf)]() {
    m_sched->run(m_writeThread, ZmFn<>{ZuMv(buf), [](Buf *buf) {
      buf->save_([buf]() {
	static_cast<FileMgr *>(buf->mgr)->save_(
	    buf->seriesID, buf->blkIndex, buf->data());
      });
    }});
  });
}

void FileMgr::save_(unsigned seriesID, unsigned blkIndex, const void *buf)
{
  int r;
  ZeError e;
  FilePos pos = this->pos(seriesID, blkIndex);
  FileID fileID{seriesID, pos.index()};
  ZmRef<File> file = getFile(fileID, true);
  if (!file) return;
  if (ZuUnlikely((r = file->pwrite(
	    pos.offset(), buf, BufSize, &e)) != (int)BufSize))
    fileWrError_(fileID, pos.offset(), e);
}

void FileMgr::purge(unsigned seriesID, unsigned blkIndex)
{
  BufMgr::purge(seriesID, blkIndex);
  FilePos pos = this->pos(seriesID, blkIndex);
  {
    auto i = m_lru.iterator();
    ZmRef<File> fileRef; // need to keep ref count +ve during loop iteration
    while (auto file_ = i.iterateNode()) {
      auto file = static_cast<File *>(file_);
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

bool FileMgr::loadFile(
    ZuString name_, Zfb::Load::LoadFn loadFn,
    unsigned maxFileSize, ZeError *e)
{
  ZiFile::Path name{name_};
  name += ZiFile::Path{".df"};
  ZiFile::Path path = ZiFile::append(m_dir, name);
  return Zfb::Load::load(path, ZuMv(loadFn), maxFileSize, e) == Zi::OK;
}

bool FileMgr::saveFile(ZuString name_, Zfb::Builder &fbb, ZeError *e)
{
  ZiFile::Path name{name_};
  name += ZiFile::Path{".df"};
  ZiFile::Path path = ZiFile::append(m_dir, name);
  return Zfb::Save::save(path, fbb, 0666, e) == Zi::OK;
}

void FileMgr::fileRdError_(
    const FileID &fileID, ZiFile::Offset off, int r, ZeError e)
{
  if (r < 0) {
    ZeLOG(Error, ZtString() <<
	"Zdf::FileMgr pread() failed on \"" <<
	fileName(fileID) <<
	"\" at offset " << ZuBoxed(off) <<  ": " << e);
  } else {
    ZeLOG(Error, ZtString() <<
	"Zdf::FileMgr pread() truncated on \"" <<
	fileName(fileID) <<
	"\" at offset " << ZuBoxed(off));
  }
}

void FileMgr::fileWrError_(
    const FileID &fileID, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ZtString() <<
      "Zdf::FileMgr pwrite() failed on \"" <<
      fileName(fileID) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e);
}
