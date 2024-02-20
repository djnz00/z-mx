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
//
// not a full glob-expression processor, this performs path
// completion for CLIs (typically via TAB or ^D)

#ifndef ZiGlob_HPP
#define ZiGlob_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZuPtr.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuNull.hpp>

#include <zlib/ZtString.hpp>

#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmRBTree.hpp>

#include <zlib/ZiFile.hpp>
#include <zlib/ZiDir.hpp>

class ZiAPI ZiGlob {
  ZiGlob(ZiGlob &) = delete;
  ZiGlob &operator =(ZiGlob &) = delete;
  ZiGlob(ZiGlob &&) = delete;
  ZiGlob &operator =(ZiGlob &&) = delete;

  struct Entry {
    Zi::Path	name;
    bool	isdir = false;

    static const ZtString &NameAxor(const Entry &entry) { return entry.name; }
  };
  using Entries = ZmRBTree<Entry, ZmRBTreeKey<Entry::NameAxor>>;
  using Iterator = decltype(ZuDeclVal<const Entries &>().readIterator());

public:
  ZiGlob() = default;

  bool init(Zi::Path prefix, ZeError *e = nullptr);
  void final();

  const Zi::Path &dirName() const { return m_dirName; }
  const Zi::Path &leafName() const { return m_leafName; }

  const Entry *iterate(bool next, bool wrap) const;

  void reset() const;

private:
  using NodePtr = Entries::Node *;

  bool match(NodePtr);

  Zi::Path			m_dirName;
  Zi::Path			m_leafName;
  ZuPtr<ZiDir>			m_dir;
  ZuPtr<Entries>		m_entries;
  NodePtr			m_first, m_last;
  mutable NodePtr		m_node;
};

#endif /* ZiGlob_HPP */
