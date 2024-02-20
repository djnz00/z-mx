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
      m_first = m_last = m_node = nullptr;
      return false;
    }
    m_dirName = ZuMv(dirName);
    if (m_entries) m_entries->clean(); else m_entries = new Entries{};
    ZiFile::Path path;
    while (m_dir->read(path) == Zi::OK) m_entries->add(ZtString{ZuMv(path)});
  }
  if (m_leafName != leafName) {
    m_leafName = ZuMv(leafName);
  }
  m_first = m_entries->findPtr<ZmRBTreeGreaterEqual>(m_leafName);
  if (!m_first || !match(m_first)) {
    m_first = m_last = m_node = nullptr;
    return true;
  }
  NodePtr prev = m_first;
  NodePtr next = m_entries->next(prev);
  while (next && match(next)) {
    prev = next;
    next = m_entries->next(prev);
  }
  m_last = prev;
  m_node = nullptr;
  return true;
}

void ZiGlob::final()
{
  m_first = m_last = m_node = nullptr;
  m_entries = nullptr;
  m_dir = nullptr;
  m_dirName = {};
  m_leafName = {};
}

ZuString ZiGlob::iterate(bool next, bool wrap) const
{
  if (next) {
    if (!m_node)
      m_node = m_first;
    else if (m_node != m_last)
      m_node = m_entries->next(m_node);
    else if (wrap)
      m_node = m_first;
    else
      m_node = nullptr;
  } else {
    if (!m_node)
      m_node = m_last;
    if (m_node != m_first)
      m_node = m_entries->prev(m_node);
    else if (wrap)
      m_node = m_last;
    else
      m_node = nullptr;
  }
  if (m_node) return m_node->key();
  return {};
}

void ZiGlob::reset() const
{
  m_node = nullptr;
}

bool ZiGlob::match(NodePtr node)
{
  ZuString entryName = node->key();
  unsigned len = m_leafName.length();
  return entryName.length() >= len &&
    !memcmp(entryName.data(), m_leafName.data(), len);
}
