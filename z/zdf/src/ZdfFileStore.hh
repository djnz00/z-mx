//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series - raw file data store

#ifndef ZdfFileStore_HH
#define ZdfFileStore_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZuPtr.hh>

#include <zlib/ZmHeap.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmHash.hh>

#include <zlib/ZtArray.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZdfStore.hh>

namespace ZdfFileStore {

using namespace Zdf;
using namespace Zdf::Store_;

using Zdf::Store_::OpenData;
using Zdf::Store_::OpenResult;
using Zdf::Store_::OpenFn;
using Zdf::Store_::CloseResult;
using Zdf::Store_::CloseFn;

ZuDeclTuple(FileID, (unsigned, seriesID), (unsigned, index));
ZuDeclTuple(FilePos, (unsigned, index), (unsigned, offset));

struct File_ : public ZmObject, public ZiFile {
  template <typename ...Args>
  File_(Args &&... args) : id{ZuFwd<Args>(args)...} { }

  static const FileID &IDAxor(const File_ &file) { return file.id; }

  FileID	id;
};
using FileLRU =
  ZmList<File_,
    ZmListNode<File_,
      ZmListShadow<true>>>;
using FileLRUNode = typename FileLRU::Node;

inline constexpr const char *File_HeapID() { return "Zdf.File"; }
using FileHash =
  ZmHash<FileLRUNode,
    ZmHashNode<FileLRUNode,
      ZmHashKey<File_::IDAxor,
	ZmHashHeapID<File_HeapID>>>>;
using File = typename FileHash::Node;

class ZdfAPI FileStore_ : public Interface {
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
      thread = cf->get("thread", true);
      maxFileSize = cf->getInt("maxFileSize", 1, 1<<30, 10<<20);
      maxBufs = cf->getInt("maxBufs", 0, 1<<20, 1<<10);
    }

    ZiFile::Path	dir;
    ZiFile::Path	coldDir;
    ZmThreadName	thread;
    unsigned		maxFileSize = 0;
    unsigned		maxBufs = 0;
  };

public:
  void init(ZmScheduler *sched, const ZvCf *cf);
  void final();

  const ZiFile::Path &dir() const { return m_dir; }
  const ZiFile::Path &coldDir() const { return m_coldDir; }

  void open(unsigned seriesID, ZuString parent, ZuString name, OpenFn);
  void close(unsigned seriesID, CloseFn);

private:
  template <typename ...Args>
  void run(Args &&... args) const {
    m_sched->run(m_sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&... args) const {
    m_sched->invoke(m_sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_sched->invoked(m_sid); }

  ZmRef<File> getFile(const FileID &fileID, bool create);
  ZmRef<File> openFile(const FileID &fileID, bool create);
  void archiveFile(const FileID &fileID);

public:
  void purge(unsigned seriesID, unsigned blkIndex);

  bool loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &hdr);
  bool load(unsigned seriesID, unsigned blkIndex, void *buf);
  void save(ZmRef<Buf> buf);

  void loadDF(ZuString name, Zfb::Load::LoadFn, unsigned maxFileSize, LoadFn);
  void saveDF(ZuString name, Zfb::Builder &, SaveFn);

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
  unsigned		m_sid = 0;		// thread slot index
  unsigned		m_maxFileSize;		// maximum file size
  unsigned		m_maxOpenFiles;		// maximum #files open
  unsigned		m_fileLoads = 0;
  unsigned		m_fileMisses = 0;
};

} // namespace ZdfFileStore

namespace Zdf {
  using FileStore = ZdfFileStore::FileStore_;
}

#endif /* ZdfFileStore_HH */
