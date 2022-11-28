//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or ZiFile::(at your option) any later version.
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

// directory scanning

#include <zlib/ZiDir.hpp>

#include <zlib/ZiFile.hpp>

int ZiDir::open(const Path &name, ZeError *e)
{
  Guard guard(m_lock);

  close();

#ifdef _WIN32

  if (!ZiFile::isdir(name, e)) {
    if (e && !*e) *e = ENOTDIR;
    return Zi::IOError;
  }
  m_match = name + L"\\*";

#else /* _WIN32 */

  if (!(m_dir = opendir(name))) {
    if (e) *e = errno;
    return Zi::IOError;
  }

#endif /* _WIN32 */

  return Zi::OK;
}

int ZiDir::read(Path &name, ZeError *e)
{
  Guard guard(m_lock);

#ifdef _WIN32

  WIN32_FIND_DATA wfd;

  if (m_handle == INVALID_HANDLE_VALUE) {
    if (!m_match) {
      if (e) *e = EBADF;
      return Zi::IOError;
    }
    m_handle = FindFirstFileEx(
	m_match, FindExInfoBasic, &wfd,
	FindExSearchNameMatch, 0, FIND_FIRST_EX_LARGE_FETCH);
    if (m_handle == INVALID_HANDLE_VALUE) goto error;
  } else {
    if (!FindNextFile(m_handle, &wfd)) goto error;
  }
  name = Path{Path::Copy, static_cast<const TCHAR *>(wfd.cFileName)};
  return Zi::OK;

error:
  DWORD errNo;
  switch (errNo = GetLastError()) {
    case ERROR_NO_MORE_FILES:
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
      return Zi::EndOfFile;
    default:
      if (e) *e = errNo;
      break;
  }
  close();
  return Zi::IOError;

#else /* _WIN32 */

  if (!m_dir) {
    if (e) *e = EBADF;
    return Zi::IOError;
  }

  struct dirent *r;

  errno = 0;
  if (!(r = readdir(m_dir))) goto error;
  name = Path{Path::Copy, &(r->d_name)[0]};
  return Zi::OK;

error:
  int errno_ = errno;
  if (!errno_) return Zi::EndOfFile;
  if (e) *e = errno_;
  close();
  return Zi::IOError;

#endif /* _WIN32 */
}

void ZiDir::close()
{
  Guard guard(m_lock);

#ifdef _WIN32
  m_match = Path();
  if (m_handle != INVALID_HANDLE_VALUE) {
    FindClose(m_handle);
    m_handle = INVALID_HANDLE_VALUE;
  }
#else
  if (m_dir) {
    closedir(m_dir);
    m_dir = 0;
  }
#endif
}
