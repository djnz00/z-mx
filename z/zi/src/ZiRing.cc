//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// shared memory IPC ring buffer

#include <zlib/ZiRing.hh>

#include <zlib/ZmTime.hh>

#include <zlib/ZeLog.hh>

#ifdef linux

#include <sys/syscall.h>
#include <linux/futex.h>

#endif

namespace ZiRing_ {

#ifdef linux

Blocker::Blocker() { }
Blocker::Blocker(const Blocker &blocker) { }
Blocker::~Blocker() { }

bool Blocker::open(bool, const Params &) { return true; }
void Blocker::close() { }

int Blocker::wait(
    ZmAtomic<uint32_t> &addr, uint32_t val,
    const Params &params)
{
  if (addr.cmpXch(val | Waiting32(), val) != val) return Zu::OK;
  val |= Waiting32();
  if (ZuUnlikely(params.timeout)) {
    ZuTime out = Zm::now(params.timeout);
    unsigned i = 0;
    do {
      if (ZuUnlikely(i >= params.spin)) {
	if (syscall(SYS_futex, reinterpret_cast<volatile int *>(&addr),
	      FUTEX_WAIT | FUTEX_CLOCK_REALTIME,
	      static_cast<int>(val), &out, 0, 0) < 0) {
	  if (errno == ETIMEDOUT) return Zu::NotReady;
	  if (errno == EAGAIN) return Zu::OK;
	}
	i = 0;
      } else
	++i;
    } while (addr == val);
  } else {
    unsigned i = 0;
    do {
      if (ZuUnlikely(i >= params.spin)) {
	syscall(SYS_futex, reinterpret_cast<volatile int *>(&addr),
	    FUTEX_WAIT, static_cast<int>(val), 0, 0, 0);
	i = 0;
      } else
	++i;
    } while (addr == val);
  }
  return Zu::OK;
}

void Blocker::wake(ZmAtomic<uint32_t> &addr)
{
  syscall(SYS_futex, reinterpret_cast<volatile int *>(&addr),
      FUTEX_WAKE, INT_MAX, 0, 0, 0);
}

#endif /* linux */

#ifdef _WIN32

Blocker::Blocker() { }

Blocker::Blocker(const Blocker &blocker) {
  DuplicateHandle(
    GetCurrentProcess(),
    blocker.m_sem,
    GetCurrentProcess(),
    &m_sem,
    0,
    FALSE,
    DUPLICATE_SAME_ACCESS);
}

Blocker::~Blocker() { close(); }

bool Blocker::open(bool head, const Params &params)
{
  if (m_sem) return true;
  Zi::Path path(params.name.length() + 21);
  path << L"Global\\" << params.name;
  if (head)
    path << L"_head";
  else
    path << L"_tail";
  path << L".sem";
  m_sem = CreateSemaphore(0, 0, LONG_MAX, path);
  if (m_sem == INVALID_HANDLE_VALUE) m_sem = 0;
  if (!m_sem) {
    ZeLOG(Error, ([path, e = ZeLastError](auto &s) {
      s << "ZiRing::Blocker::open() CreateSemaphore("
	<< path << ") failed: " << e;
    }));
    return false;
  }
  return true;
}

void Blocker::close()
{
  if (m_sem) { CloseHandle(m_sem); m_sem = 0; }
}

int Blocker::wait(
    ZmAtomic<uint32_t> &addr, uint32_t val,
    const Params &params)
{
  if (addr.cmpXch(val | Waiting32(), val) != val) return Zu::OK;
  val |= Waiting32();
  DWORD timeout = params.timeout ? params.timeout * 1000 : INFINITE;
  unsigned i = 0;
  while (i < params.spin) {
    if (addr != val) return Zu::OK;
    ++i;
  }
  while (addr == val) {
    if (ZuUnlikely(!m_sem)) return Zu::IOError;
    DWORD r = WaitForSingleObject(m_sem, timeout);
    switch (int(r)) {
      case WAIT_OBJECT_0:	return Zu::OK;
      case WAIT_TIMEOUT:	return Zu::NotReady;
    }
    return Zu::IOError;
  }
  return Zu::OK;
}

void Blocker::wake(ZmAtomic<uint32_t> &addr)
{
  if (ZuUnlikely(!m_sem)) return;
  LONG prev = 0;
  while (ReleaseSemaphore(m_sem, 1, &prev) && prev > 1);
}

#endif /* _WIN32 */

bool CtrlMem::open(unsigned size, const Params &params)
{
  if (m_file) return true;
  int r;
  int mmapFlags =
#ifdef linux
    MAP_POPULATE;
#else
    0;
#endif
  Zi::Path path(params.name.length() + 6);
  path << params.name << ".ctrl";
  ZeError e;
  if ((r = m_file.mmap(path,
	  ZiFile::Create | ZiFile::Shm, size,
	  true, mmapFlags, 0666, &e)) != Zi::OK) {
    ZeLOG(Error, ([path, e](auto &s) {
      s << "ZiRing::CtrlMem::open() mmap(" << path  << ") failed: " << e;
    }));
    return false;
  }
  if (params.cpuset)
    hwloc_set_area_membind(
	ZmTopology::hwloc(), m_file.addr(), m_file.mmapLength(),
	params.cpuset, HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
  return true;
}

void CtrlMem::close() { m_file.close(); }

bool DataMem::open(unsigned size, const Params &params)
{
  if (m_file) return true;
  int r;
  int mmapFlags =
#ifdef linux
    params.ll ? MAP_POPULATE : 0;
#else
    0;
#endif
  Zi::Path path(params.name.length() + 6);
  path << params.name << ".data";
  ZeError e;
  if ((r = m_file.mmap(path,
	  ZiFile::Create | ZiFile::Shm, size,
	  true, mmapFlags, 0666, &e)) != Zi::OK) {
    ZeLOG(Error, ([path, e](auto &s) {
      s << "ZiRing::DataMem::open() mmap(" << path  << ") failed: " << e;
    }));
    return false;
  }
  if (params.cpuset)
    hwloc_set_area_membind(
	ZmTopology::hwloc(), m_file.addr(), m_file.mmapLength(),
	params.cpuset, HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
  return true;
}

void DataMem::close() { m_file.close(); }

bool MirrorMem::open(unsigned size, const Params &params)
{
  if (m_file) return true;
  int r;
  int mmapFlags =
#ifdef linux
    params.ll ? MAP_POPULATE : 0;
#else
    0;
#endif
  Zi::Path path(params.name.length() + 6);
  path << params.name << ".data";
  ZeError e;
  if ((r = m_file.mmap(path,
	  ZiFile::Create | ZiFile::Shm | ZiFile::ShmMirror, size,
	  true, mmapFlags, 0666, &e)) != Zi::OK) {
    ZeLOG(Error, ([path, e](auto &s) {
      s << "ZiRing::MirrorMem::open() mmap(" << path  << ") failed: " << e;
    }));
    return false;
  }
  if (params.cpuset)
    hwloc_set_area_membind(
	ZmTopology::hwloc(), m_file.addr(), (m_file.mmapLength())<<1,
	params.cpuset, HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
  return true;
}

void MirrorMem::close() { m_file.close(); }

void RingExt_::getpinfo(uint32_t &pid, ZuTime &start)
{
#ifdef linux
  pid = getpid();
  ZuCArray<20> path; path << "/proc/" << ZuBox<uint32_t>(pid);
  struct stat s;
  start = (::stat(path, &s) < 0) ? ZuTime{} : ZuTime{s.st_ctim};
#endif
#ifdef _WIN32
  pid = GetCurrentProcessId();
  FILETIME creation, exit, kernel, user;
  start = !GetProcessTimes(
      GetCurrentProcess(), &creation, &exit, &kernel, &user) ?
    ZuTime{} : ZuTime{creation};
#endif
}

bool RingExt_::alive(uint32_t pid, ZuTime start)
{
  if (!pid) return false;
#ifdef linux
  ZuCArray<20> path; path << "/proc/" << ZuBox<uint32_t>(pid);
  struct stat s;
  if (::stat(path, &s) < 0) return false;
  return !start || ZuTime{s.st_ctim} == start;
#endif
#ifdef _WIN32
  HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION | SYNCHRONIZE, FALSE, pid);
  if (!h || h == INVALID_HANDLE_VALUE) return false;
  DWORD r = WaitForSingleObject(h, 0);
  if (r != WAIT_TIMEOUT) { CloseHandle(h); return false; }
  FILETIME creation, exit, kernel, user;
  if (!GetProcessTimes(h, &creation, &exit, &kernel, &user))
    { CloseHandle(h); return false; }
  return !start || ZuTime{creation} == start;
  CloseHandle(h);
#endif
}

bool RingExt_::kill(uint32_t pid, bool coredump)
{
  if (!pid) return false;
#ifdef linux
  return ::kill(pid, coredump ? SIGQUIT : SIGKILL) >= 0;
#endif
#ifdef _WIN32
  HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, pid);
  if (!h || h == INVALID_HANDLE_VALUE) return false;
  bool ok = (TerminateProcess(h, (unsigned)-1) == TRUE);
  CloseHandle(h);
  return ok;
#endif
}

} // ZiRing_
