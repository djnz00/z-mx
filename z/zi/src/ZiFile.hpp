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

// file I/O

#ifndef ZiFile_HPP
#define ZiFile_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>

#ifndef _WIN32
#include <sys/mman.h>
#endif

#ifndef _WIN32
#include <alloca.h>
#endif

class ZiAPI ZiFile {
  ZiFile(const ZiFile &) = delete;
  ZiFile &operator =(const ZiFile &) = delete;	// prevent mis-use

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

  ZuInline bool operator !() const {
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
  int shadow(const ZiFile &file, ZeError *e = nullptr);
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

  // Note: unbuffered!
  template <typename V> ZiFile &operator <<(V &&v) {
    append_(ZuFwd<V>(v));
    return *this;
  }

  static ZmTime mtime(const Path &name, ZeError *e = nullptr);
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

  template <typename S> ZuIsString<S> append_(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
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
    auto buf = ZuAlloc(char, len);
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

#endif /* ZiFile_HPP */
