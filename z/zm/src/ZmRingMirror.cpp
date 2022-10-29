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

// ring buffer intra-process mirrored memory region

#include <zlib/ZmRingMirror.hpp>

bool ZmRingMirror::open(unsigned size)
{
  if (m_handle != nullHandle()) return false;

#ifdef linux
  m_handle = memfd_create("ZmVRing", MFD_CLOEXEC);
  if (ftruncate(m_handle, size) < 0) { ::close(m_handle); return false; }
  m_addr = ::mmap(
      0, size<<1, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
  if (m_addr == MAP_FAILED || !m_addr) return false;
  void *addr = ::mmap(
      m_addr, size,
      PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, m_handle, 0);
  if (addr != m_addr) return false;
  addr = ::mmap(
      static_cast<uint8_t *>(m_addr) + size, size,
      PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, m_handle, 0);
  if (addr != static_cast<void *>(
	static_cast<uint8_t *>(m_addr) + size)) return false;
#endif /* linux */

#ifdef _WIN32
  handle = 
  CreateFileMapping(INVALID_HANDLE_VALUE, 0, PAGE_READWRITE, 0, size, nullptr);
    if (!handle || handle == INVALID_HANDLE_VALUE) return false;
retry:
  m_addr = VirtualAlloc(
      0, static_cast<DWORD>(size<<1), MEM_RESERVE, PAGE_NOACCESS);
  if (!m_addr) return false;
  if (!VirtualFree(m_addr, 0, MEM_RELEASE)) return false;
  void *addr = MapViewOfFileEx(
      handle, static_cast<DWORD>(accessFlags), 0, 0,
      static_cast<DWORD>(size), m_addr);
  if (!addr) goto retry;
  if (addr != m_addr) { UnmapViewOfFile(addr); goto retry; }
  addr = MapViewOfFileEx(
      handle, static_cast<DWORD>(accessFlags), 0, 0,
      static_cast<DWORD>(size),
      static_cast<uint8_t *>(m_addr) + size);
  if (!addr) goto retry;
  if (addr != static_cast<void *>(
	static_cast<uint8_t *>(m_addr) + size)) {
    UnmapViewOfFile(m_addr);
    UnmapViewOfFile(addr);
    goto retry;
  }
#endif /* _WIN32 */

  m_size = size;
  return true;
}

void ZmRingMirror::close()
{
#ifndef _WIN32
  munmap(m_addr, m_size);
  munmap(static_cast<uint8_t *>(m_addr) + m_size, m_size);
  ::close(m_handle);
#else
  UnmapViewOfFile(m_addr);
  UnmapViewOfFile(static_cast<uint8_t *>(m_addr) + m_size);
  CloseHandle(m_handle);
#endif
  m_handle = nullHandle();
  m_addr = nullptr;
  m_size = 0;
}
