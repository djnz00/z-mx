//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// indexing of structs/classes by member (field)
//
// struct Point { int x, y; };
//
// Point p;
//
// ...
// struct PointYAccessor : public ZuAccessor<Point, int> {
//   inline static int value(const Point &p) { return p.y; }
// }
// ...
//
// ZuIndex<PointYAccessor> can now be used in place of both ZuCmp, ZuHash

#ifndef ZuIndex_HPP
#define ZuIndex_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>

template <typename T_, typename I_, bool Is> struct ZuAccessor_ {
  enum { Same = 0 };
  typedef T_ T;
  typedef I_ I;
  typedef ZuCmp<T> TCmp;
  typedef ZuCmp<I> Cmp;
  typedef ZuHash<I> Hash;

  template <typename P>
  inline static I value(const P &p) { return p; }
};

template <typename T_, typename I_> struct ZuAccessor_<T_, I_, true> {
  enum { Same = 1 };
  typedef T_ T;
  typedef I_ I;
  typedef ZuCmp<T> TCmp;
  typedef ZuCmp<I> Cmp;
  typedef ZuHash<I> Hash;

  template <typename P>
  inline static const I &value(const P &p) { return p; }
};

template <typename T, typename I> struct ZuAccessor :
    public ZuAccessor_<typename ZuTraits<T>::T, typename ZuTraits<I>::T,
      ZuConversion<typename ZuTraits<I>::T, typename ZuTraits<T>::T>::Is> { };

template <typename Accessor_, bool Same> struct ZuIndex_ {
  typedef Accessor_ Accessor;
  typedef typename Accessor::T T;
  typedef typename Accessor::I I;

  struct ICmp {
    template <typename T1, typename T2>
    inline static int cmp(T1 &&t1, T2 &&t2) {
      return Accessor::Cmp::cmp(
	  Accessor::value(ZuFwd<T1>(t1)), ZuFwd<T2>(t2));
    }
    template <typename T1, typename T2>
    inline static bool equals(T1 &&t1, T2 &&t2) {
      return Accessor::Cmp::equals(
	  Accessor::value(ZuFwd<T1>(t1)), ZuFwd<T2>(t2));
    }
  };

  typedef typename Accessor::Hash IHash;

  template <typename T> struct CmpT {
    template <typename T1, typename T2>
    inline static int cmp(T1 &&t1, T2 &&t2) {
      return Accessor::Cmp::cmp(Accessor::value(
	    ZuFwd<T1>(t1)), Accessor::value(ZuFwd<T2>(t2)));
    }
    template <typename T1, typename T2>
    inline static bool equals(T1 &&t1, T2 &&t2) {
      return Accessor::Cmp::equals(
	  Accessor::value(ZuFwd<T1>(t1)), Accessor::value(ZuFwd<T2>(t2)));
    }

    template <typename T_>
    inline static bool null(T_ &&t) {
      return Accessor::Cmp::null(Accessor::value(ZuFwd<T_>(t)));
    }

    inline static const T &null() {
      return ZuCmp<T>::null();
    }
  };

  struct Hash {
    template <typename T>
    inline static uint32_t hash(T &&t) {
      return Accessor::Hash::hash(Accessor::value(ZuFwd<T>(t)));
    }
  };
};

template <typename Accessor_>
struct ZuIndex_<Accessor_, true> : public Accessor_ {
  typedef Accessor_ Accessor;
  typedef typename Accessor::T T;
  typedef typename Accessor::I I;
  typedef typename Accessor::Cmp ICmp;
  typedef typename Accessor::Hash IHash;
  typedef typename Accessor::Cmp Cmp;
  typedef typename Accessor::Hash Hash;
};

template <typename Accessor>
struct ZuIndex : public ZuIndex_<Accessor, Accessor::Same> { };

#endif /* ZuIndex_HPP */
