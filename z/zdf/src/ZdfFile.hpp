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

#ifndef ZdfFile_HPP
#define ZdfFile_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HPP
#include <zlib/ZdfLib.hpp>
#endif

#include <zlib/ZuObject.hpp>
#include <zlib/ZuPtr.hpp>

#include <zlib/ZmHeap.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmHash.hpp>
#include <zlib/ZmScheduler.hpp>

#include <zlib/ZtArray.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiFile.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/ZdfBuf.hpp>
#include <zlib/ZdfSeries.hpp>

namespace Zdf {

ZuDeclTuple(FileID, (unsigned, seriesID), (unsigned, index));
ZuDeclTuple(FilePos, (unsigned, index), (unsigned, offset));

struct File_ : public ZiFile {
  template <typename ...Args>
  File_(Args &&... args) : id{ZuFwd<Args>(args)...} { }

  FileID	id;
};
struct File_IDAccessor {
  static const FileID &get(const File_ &file) { return file.id; }
};

using FileLRU =
  ZmList<File_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using FileLRUNode = FileLRU::Node;

struct File_HeapID {
  static constexpr const char *id() { return "Zdf.File"; }
};
using FileHash =
  ZmHash<FileLRUNode,
    ZmHashKey<File_IDAccessor,
      ZmHashObject<ZmObject,
	ZmHashNodeDerive<true,
	  ZmHashHeapID<File_HeapID,
	    ZmHashLock<ZmNoLock> > > > > >;
using File = typename FileHash::Node;

class ZdfAPI FileMgr : public Mgr {
private:
  struct Config {
    Config(const Config &) = delete;
    Config &operator =(const Config &) = delete;
    Config() = default;
    Config(Config &&) = default;
    Config &operator =(Config &&) = default;

    Config(const ZvCf *cf) {
      dir = cf->get("dir", true);
      coldDir = cf->get("coldDir", true);
      writeThread = cf->get("writeThread", true);
      maxFileSize = cf->getInt("maxFileSize", 1, 1<<30, false, 10<<20);
      maxBufs = cf->getInt("maxBufs", 0, 1<<20, false, 1<<10);
    }

    ZiFile::Path	dir;
    ZiFile::Path	coldDir;
    ZmThreadName	writeThread;
    unsigned		maxFileSize = 0;
    unsigned		maxBufs = 0;
  };

public:
  void init(ZmScheduler *sched, const ZvCf *cf);
  void final();

  const ZiFile::Path &dir() const { return m_dir; }
  const ZiFile::Path &coldDir() const { return m_coldDir; }

  bool open(
      unsigned seriesID, ZuString parent, ZuString name, OpenFn openFn);
  void close(unsigned seriesID);

private:
  ZmRef<File> getFile(const FileID &fileID, bool create);
  ZmRef<File> openFile(const FileID &fileID, bool create);
  void archiveFile(const FileID &fileID);

public:
  void purge(unsigned seriesID, unsigned blkIndex);

  bool loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &hdr);
  bool load(unsigned seriesID, unsigned blkIndex, void *buf);
  void save(ZmRef<Buf> buf);

  bool loadFile(
      ZuString name, Zfb::Load::LoadFn,
      unsigned maxFileSize, ZeError *e = nullptr);
  bool saveFile(
      ZuString name, Zfb::Builder &fbb, ZeError *e = nullptr);

private:
  void save_(unsigned seriesID, unsigned blkIndex, const void *buf);

  struct SeriesFile {
    ZiFile::Path	path;
    ZiFile::Path	name;
    unsigned		minFileIndex = 0;	// earliest file index
    unsigned		fileBlks = 0;

    unsigned fileSize() const { return fileBlks * BufSize; }
  };

  ZiFile::Path fileName(const FileID &fileID) {
    const auto &series = m_series[fileID.seriesID()];
    return ZiFile::append(series.path,
	ZiFile::Path{} << series.name << '_' <<
	(ZuStringN<12>{} <<
	  ZuBox<unsigned>{fileID.index()}.hex<false, ZuFmt::Right<8>>()) <<
	".sdb");
  }
  FilePos pos(unsigned seriesID, unsigned blkIndex) {
    auto fileBlks = m_series[seriesID].fileBlks;
    return FilePos{blkIndex / fileBlks, (blkIndex % fileBlks) * BufSize};
  }

  void fileRdError_(const FileID &fileID, ZiFile::Offset, int, ZeError);
  void fileWrError_(const FileID &fileID, ZiFile::Offset, ZeError);

private:
  ZtArray<SeriesFile>	m_series;	// indexed by seriesID
  ZuPtr<FileHash>	m_files;
  FileLRU		m_lru;
  ZmScheduler		*m_sched = nullptr;
  ZiFile::Path		m_dir;
  ZiFile::Path		m_coldDir;
  unsigned		m_writeThread = 0;	// write thread index
  unsigned		m_maxFileSize;		// maximum file size
  unsigned		m_maxOpenFiles;		// maximum #files open
  unsigned		m_fileLoads = 0;
  unsigned		m_fileMisses = 0;
};

} // namespace Zdf

#endif /* ZdfFile_HPP */
