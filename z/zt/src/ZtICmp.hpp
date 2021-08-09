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

// case-insensitive matching
//
// works for ZtString, ZtArray<char>

#ifndef ZtICmp_HPP
#define ZtICmp_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuString.hpp>

#include <zlib/ZtPlatform.hpp>
#include <zlib/ZtString.hpp>

template <typename T>
struct ZtICmp {
public:
  static int cmp(ZuString s1, ZuString s2) {
    int l1 = s1.length(), l2 = s2.length();
    if (!l1) return l2 ? -1 : 0;
    if (!l2) return 1;
    int i = Zu::stricmp_(s1.data(), s2.data(), l1 > l2 ? l2 : l1);
    if (i) return i;
    return l1 - l2;
  }
  static bool less(ZuString s1, ZuString s2) {
    int l1 = s1.length(), l2 = s2.length();
    if (!l1) return l2;
    if (!l2) return false;
    int i = Zu::stricmp_(s1.data(), s2.data(), l1 > l2 ? l2 : l1);
    if (i) return i < 0;
    return l1 < l2;
  }
  static int equals(ZuString s1, ZuString s2) {
    int l1 = s1.length(), l2 = s2.length();
    if (l1 != l2) return false;
    return !Zu::stricmp_(s1.data(), s2.data(), l1);
  }
  static bool null(const T &s) { return ZuCmp<T>::null(s); }
  static const T &null() { return ZuCmp<T>::null(); }
};

#endif /* ZtICmp_HPP */
