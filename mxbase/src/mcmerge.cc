//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// multicast capture file merge tool

#include <stdio.h>

#include <zlib/ZuTime.hh>
#include <zlib/ZmTrap.hh>
#include <zlib/ZmRBTree.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiFile.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxMCapHdr.hh>

void usage()
{
  std::cerr <<
    "Usage: mcmerge OUTFILE INFILE...\n"
    "\tOUTFILE\t- output capture file\n"
    "\tINFILE\t- input capture file\n\n"
    "Options:\n"
    << std::flush;
  Zm::exit(1);
}

class File : public ZuObject {
public:
  // UDP over Ethernet maximum payload is 1472 (without Jumbo frames)
  enum { MsgSize = 1472 };

  template <typename Path>
  File(const Path &path) : m_path(path) { }

  void open() {
    ZeError e;
    if (m_file.open(m_path, ZiFile::ReadOnly, 0, &e) != Zi::OK) {
      ZeLOG(Error, ([](auto &s) { s << '"' << m_path << "\": " << e; }));
      Zm::exit(1);
    }
  }
  void close() { m_file.close(); }
  ZuTime read() {
    ZeError e;
    int i;
    if ((i = m_file.read(&m_hdr, sizeof(MxMCapHdr), &e)) == Zi::IOError) {
      ZeLOG(Error, ([](auto &s) { s << '"' << m_path << "\": " << e; }));
      Zm::exit(1);
    }
    if (i == Zi::EndOfFile || (unsigned)i < sizeof(MxMCapHdr)) {
      close();
      return ZuTime();
    }
    if (m_hdr.len > MsgSize) {
      ZeLOG(Error, ([](auto &s) { s << "message length >" << ZuBoxed(MsgSize) <<
	  " at offset " << ZuBoxed(m_file.offset() - sizeof(MxMCapHdr)); }));
      Zm::exit(1);
    }
    if ((i = m_file.read(m_buf, m_hdr.len, &e)) == Zi::IOError) {
      ZeLOG(Error, ([](auto &s) { s << '"' << m_path << "\": " << e; }));
      Zm::exit(1);
    }
    if (i == Zi::EndOfFile || (unsigned)i < m_hdr.len) {
      close();
      return ZuTime();
    }
    return ZuTime((time_t)m_hdr.sec, (int32_t)m_hdr.nsec);
  }
  int write(ZiFile *out, ZeError *e) {
    int i;
    if ((i = out->write(&m_hdr, sizeof(MxMCapHdr), e)) < 0) return i;
    if ((i = out->write(m_buf, m_hdr.len, e)) < 0) return i;
    return Zi::OK;
  }

private:
  ZtString	m_path;
  ZiFile	m_file;
  MxMCapHdr	m_hdr;
  char		m_buf[MsgSize];
};

typedef ZmRBTree<ZuTime,
	  ZmRBTreeVal<ZmRef<File>,
	    ZmRBTreeLock<ZmNoLock> > > Files;

int main(int argc, const char *argv[])
{
  Files files;
  ZuCSpan outPath;

  {
    ZtArray<ZuCSpan> paths;

    for (int i = 1; i < argc; i++) {
      if (argv[i][0] != '-') {
	paths.push(argv[i]);
	continue;
      }
      switch (argv[i][1]) {
	default:
	  usage();
	  break;
      }
    }
    if (paths.length() < 2) usage();
    outPath = paths[0];
    
    ZeLog::init("mcmerge");
    ZeLog::level(0);
    ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
    ZeLog::start();

    for (int i = 1, n = paths.length(); i < n; i++) {
      ZmRef<File> file = new File(paths[i]);
      file->open();
      ZuTime t = file->read();
      if (t) files.add(t, file);
    }
  }

  ZiFile out;
  ZeError e;

  if (out.open(outPath, ZiFile::Create | ZiFile::Append, 0666, &e) < 0) {
    ZeLOG(Error, ([](auto &s) { s << '"' << outPath << "\": " << e; }));
    Zm::exit(1);
  }

  for (;;) {
    ZmRef<File> file;
    {
      auto i = files.iterator<ZmRBTreeGreaterEqual>();
      if (file = i.iterateVal()) i.del();
    }
    if (!file) break;
    if (file->write(&out, &e) != Zi::OK) {
      ZeLOG(Error, ([](auto &s) { s << '"' << outPath << "\": " << e; }));
      Zm::exit(1);
    }
    ZuTime t = file->read();
    if (t)
      files.add(t, file);
    else
      if (!files.count()) break;
  }

  ZeLog::stop();
  return 0;
}
