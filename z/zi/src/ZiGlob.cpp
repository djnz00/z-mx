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

// file system pathname auto-completion and directory scanning

#include <zlib/ZiGlob.hpp>

bool ZiGlob::init(ZuString prefix, ZeError *e)
{
  ZtString dirName = ZiFile::dirname(prefix);
  ZtString leafName = ZiFile::leafname(prefix);
  if (m_dirName != dirName) {
    m_leafName = {};
    if (m_dir) m_dir->close(); else m_dir = new ZiDir{};
    if (m_dir->open(dirName, e) != Zi::OK) {
      m_dir = nullptr;
      m_iterator = nullptr;
      return false;
    }
    m_dirName = ZuMv(dirName);
    if (m_entries) m_entries->clean(); else m_entries = new Entries{};
    ZiFile::Path path;
    while (m_dir->read(path) == Zi::OK) m_entries->add(ZtString{ZuMv(path)});
    m_iterator = nullptr;
  }
  if (m_leafName != leafName) {
    m_leafName = ZuMv(leafName);
    m_iterator = nullptr;
  }
  return true;
}

void ZiGlob::final()
{
  m_iterator = nullptr;
  m_entries = nullptr;
  m_dir = nullptr;
  m_dirName = {};
  m_leafName = {};
}

ZuString ZiGlob::next() const
{
  if (!m_iterator) reset();
  ZuString entryName = m_iterator->iterateKey();
  unsigned len = m_leafName.length();
  if (entryName.length() < len ||
      memcmp(entryName.data(), m_leafName.data(), len)) {
    m_iterator = nullptr;
    return {};
  }
  return entryName;
}

void ZiGlob::reset() const
{
  m_iterator = new Iterator{m_entries->readIterator(m_leafName)};
}
