//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// file I/O

#ifndef ZiFile_HH
#define ZiFile_HH

#ifndef ZiLib_HH
#include <zlib/ZiLib.hh>
#endif

#include <zlib/ZmLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmAlloc.hh>

#include <zlib/ZePlatform.hh>

#include <zlib/ZiPlatform.hh>

#ifndef _WIN32
#include <sys/mman.h>
#endif

#ifndef _WIN32
#include <alloca.h>
#endif

class ZiAPI ZiFile {
public:
  using Handle = Zi::Handle;
  using Path = Zi::Path;
  using Offset = Zi::Offset;
  using MMapPtr = Zi::MMapPtr;

  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  enum Flags {
    ReadOnly	= 0x0001,
    WriteOnly	= 0x0002,
    Create	= 0x0004,
    Exclusive	= 0x0008,
    Truncate	= 0x0010,
    Append	= 0x0020,
    Direct	= 0x0040,// O_DIRECT (Unix) / FILE_FLAG_NO_BUFFERING  (Windows)
    Sync	= 0x0080,// O_DSYNC  (Unix) / FILE_FLAG_WRITE_THROUGH (Windows)
    GC		= 0x0100,// close() handle in destructor
    MMap	= 0x0200,// memory-mapped file (set internally by mmap())
    Shm		= 0x0400,// global named shared memory, not a real file
    ShmGC	= 0x0800,// remove shared memory on close()
    ShmMirror	= 0x1000,// map two adjacent copies of the same memory
    MMPopulate	= 0x2000,// MAP_POPULATE
    Shadow	= 0x4000 // shadow already opened file
  };

  // Note: Direct requires caller align all reads/writes to blkSize()

  ZiFile() { }

  ZiFile(const ZiFile &file) :
      m_handle{file.m_handle},
      m_flags{file.m_flags | Shadow},
      m_offset{file.m_offset},
      m_blkSize{file.m_blkSize},
      m_addr{file.m_addr},
      m_mmapLength{file.m_mmapLength},
#ifndef _WIN32
      m_shmName{file.m_shmName}
#else
      m_mmapHandle{file.m_mmapHandle}
#endif
      { }
  ZiFile &operator =(const ZiFile &file) {
    if (this != &file) {
      this->~ZiFile();
      new (this) ZiFile{file};
    }
    return *this;
  }

  ~ZiFile() { final(); }

  ZuInline Handle handle() const { return m_handle; }
  ZuInline void *addr() const { return m_addr; }
  ZuInline Offset mmapLength() const { return m_mmapLength; }

  ZuInline unsigned flags() { return m_flags; }
  void setFlags(int f) { Guard guard(m_lock); m_flags |= f; }
  void clrFlags(int f) { Guard guard(m_lock); m_flags &= ~f; }

  int init(Handle handle, unsigned flags, ZeError *e = nullptr);
  void final() {
    Guard guard(m_lock);

    if (m_flags & GC) {
      close();
    } else {
      m_handle = Zi::nullHandle();
#ifdef _WIN32
      m_mmapHandle = Zi::nullHandle();
#endif
    }
  }

  bool operator !() const {
    ReadGuard guard(m_lock);
    return Zi::nullHandle(m_handle);
  }
  ZuOpBool

  int open(const Path &name,
      unsigned flags, unsigned mode = 0666, ZeError *e = nullptr);
  int open(const Path &name,
      unsigned flags, unsigned mode, Offset length, ZeError *e = nullptr);
  int mmap(const Path &name,
      unsigned flags, Offset length, bool shared = true,
      int mmapFlags = 0, unsigned mode = 0666, ZeError *e = nullptr);
  void close();

  Offset size();
  int blkSize() { return m_blkSize; }

  Offset offset() { ReadGuard guard(m_lock); return m_offset; }
  void seek(Offset offset) { Guard guard(m_lock); m_offset = offset; }

  int sync(ZeError *e = nullptr);
  int msync(void *addr = 0, Offset length = 0, ZeError *e = nullptr);

  int read(void *ptr, unsigned len, ZeError *e = nullptr);
  int readv(const ZiVec *vecs, unsigned nVecs, ZeError *e = nullptr);

  int write(const void *ptr, unsigned len, ZeError *e = nullptr);
  int writev(const ZiVec *vecs, unsigned nVecs, ZeError *e = nullptr);

  int pread(Offset offset,
      void *ptr, unsigned len, ZeError *e = nullptr);
  int preadv(Offset offset,
      const ZiVec *vecs, unsigned nVecs, ZeError *e = nullptr);

  int pwrite(Offset offset,
      const void *ptr, unsigned len, ZeError *e = nullptr);
  int pwritev(Offset offset,
      const ZiVec *vecs, unsigned nVecs, ZeError *e = nullptr);

  int truncate(Offset offset, ZeError *e = nullptr);

  // Note: unbuffered!
  template <typename V> ZiFile &operator <<(V &&v) {
    append_(ZuFwd<V>(v));
    return *this;
  }

  static ZuTime mtime(const Path &name, ZeError *e = nullptr);
  static bool isdir(const Path &name, ZeError *e = nullptr);

  static int remove(const Path &name, ZeError *e = nullptr);
  static int rename(
      const Path &oldName, const Path &newName, ZeError *e = nullptr);
  static int copy(
      const Path &oldName, const Path &newName, ZeError *e = nullptr);
  static int mkdir(const Path &name, ZeError *e = nullptr);
  static int rmdir(const Path &name, ZeError *e = nullptr);

  static Path cwd();

  static bool absolute(const Path &name);

  static Path leafname(const Path &name);
  static Path dirname(const Path &name);
  static Path append(const Path &dir, const Path &name);

  static void age(const Path &name, unsigned max);

private:
  int open_(const Path &name,
      unsigned flags, unsigned mode, Offset length, ZeError *e);

  void init_(Handle handle, unsigned flags, int blkSize, Offset mmapLength = 0);

  template <typename U, typename R = void>
  using MatchPDelegate =
    ZuIfT<ZuPrint<U>::Delegate && !ZuTraits<U>::IsString, R>;
  template <typename U, typename R = void>
  using MatchPBuffer =
    ZuIfT<ZuPrint<U>::Buffer && !ZuTraits<U>::IsString, R>;

  template <typename S> ZuMatchString<S> append_(S &&s_) {
    ZuCSpan s(ZuFwd<S>(s_));
    if (ZuUnlikely(!s)) return;
    ZeError e;
    if (ZuUnlikely(write(s.data(), s.length(), &e) != Zi::OK))
      throw e;
  }
  template <typename P> MatchPDelegate<P> append_(P &&p) {
    ZuPrint<P>::print(*this, ZuFwd<P>(p));
  }
  template <typename P> MatchPBuffer<P> append_(const P &p) {
    unsigned len = ZuPrint<P>::length(p);
    auto buf = ZmAlloc(char, len);
    if (!buf) throw ZeError{ZiENOMEM};
    ZeError e;
    if (ZuUnlikely(write(buf, ZuPrint<P>::print(buf, len, p), e) != Zi::OK))
      throw e;
  }

  Lock		m_lock;
    Handle	  m_handle = Zi::nullHandle();
    unsigned	  m_flags = 0;
    Offset	  m_offset = 0;
    int		  m_blkSize = 0;
    void	  *m_addr = nullptr;
    Offset	  m_mmapLength = 0;
#ifndef _WIN32
    ZtString	  m_shmName;
#else
    Handle	  m_mmapHandle = Zi::nullHandle();
#endif
};

#endif /* ZiFile_HH */
