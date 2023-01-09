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

// RAII single-owner move-only smart pointer

#ifndef ZuPtr_HPP
#define ZuPtr_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuConversion.hpp>

// rules for using ZuPtr

// * always point to objects allocated using new
// * pass by raw pointer (unless moving), and return by ZuPtr value

struct ZuPtr_ { }; // compile-time tag

template <typename T_> class ZuPtr : public ZuPtr_ {
template <typename> friend class ZuPtr;
public:
  using T = T_;

private:
  // matches ZuPtr<U> where U is not T, but is in the same type hierarchy as T
  template <typename U> struct IsOtherPtr_ {
    enum { OK =
      (ZuConversion<T, typename U::T>::Base ||
       ZuConversion<typename U::T, T>::Base) };
  };
  template <typename U, typename = void, bool = IsOtherPtr_<U>::OK>
  struct MatchOtherPtr__ { };
  template <typename U, typename R>
  struct MatchOtherPtr__<U, R, true> { using T = R; };
  template <typename U> struct IsOtherPtr1 {
    enum { OK = ZuConversion<ZuPtr_, U>::Base };
  };
  template <typename U, typename = void, bool = IsOtherPtr1<U>::OK>
  struct MatchOtherPtr_;
  template <typename U, typename R>
  struct MatchOtherPtr_<U, R, true> : public MatchOtherPtr__<U, R> { };
  template <typename U, typename R = void>
  using MatchOtherPtr = typename MatchOtherPtr_<U, R>::T;

  // matches ZuPtr<U> where U is either T or in the same type hierarchy as T
  template <typename U> struct IsZuPtr__ {
    enum { OK =
      (ZuConversion<T, typename U::T>::Is ||
       ZuConversion<typename U::T, T>::Is) };
  };
  template <typename U, typename = void, bool = IsZuPtr__<U>::OK>
  struct MatchZuPtr__ { };
  template <typename U, typename R>
  struct MatchZuPtr__<U, R, true> { using T = R; };
  template <typename U> struct IsZuPtr_ {
    enum { OK = ZuConversion<ZuPtr_, U>::Base };
  };
  template <typename U, typename = void, bool = IsZuPtr_<U>::OK>
  struct MatchZuPtr_;
  template <typename U, typename R>
  struct MatchZuPtr_<U, R, true> : public MatchZuPtr__<U, R> { };
  template <typename U, typename R = void>
  using MatchZuPtr = typename MatchZuPtr_<U, R>::T;

  // matches U * where U is either T or in the same type hierarchy as T
  template <typename U> struct IsPtr {
    enum { OK = (ZuConversion<T, U>::Is || ZuConversion<U, T>::Is) };
  };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>::OK, R>;

public:
  ZuPtr() : m_object{nullptr} { }
  ZuPtr(ZuPtr &&r) : m_object{r.m_object} {
    r.m_object = nullptr;
  }
  template <typename R>
  ZuPtr(R &&r, MatchOtherPtr<ZuDeref<R>> *_ = nullptr) : m_object{
      static_cast<T *>(const_cast<typename ZuDeref<R>::T *>(r.m_object))} {
    ZuBind<R>::mv(ZuFwd<R>(r), [](auto &&r) { r.m_object = nullptr; });
  }
  ZuPtr(T *o) : m_object{o} { }
  template <typename O>
  ZuPtr(O *o, MatchPtr<O> *_ = nullptr) : m_object{static_cast<T *>(o)} { }
  ~ZuPtr() {
    if (T *o = m_object) delete o;
  }

  template <typename R> MatchZuPtr<R> swap(R &r) noexcept {
    T *o = m_object;
    m_object = static_cast<T *>(r.m_object);
    r.m_object = static_cast<typename R::T *>(o);
  }

  template <typename R>
  friend MatchZuPtr<R> swap(ZuPtr &r1, R &r2) noexcept {
    r1.swap(r2);
  }

  ZuPtr &operator =(ZuPtr r) noexcept {
    swap(r);
    return *this;
  }
  template <typename R>
  MatchOtherPtr<R, ZuPtr &> operator =(R r) {
    swap(r);
    return *this;
  }

  template <typename O>
  MatchPtr<O, ZuPtr &> operator =(O *n) {
    T *o = m_object;
    m_object = n;
    if (o) delete o;
    return *this;
  }

  operator T *() const { return m_object; }
  T *operator ->() const { return m_object; }

  template <typename O = T>
  MatchZuPtr<ZuPtr<O>, O *> ptr() const {
    return static_cast<O *>(m_object);
  }
  T *ptr_() const { return m_object; }

  template <typename O = T>
  MatchZuPtr<ZuPtr<O>, O *> release() {
    auto ptr = static_cast<O *>(m_object);
    m_object = nullptr;
    return ptr;
  }

  bool operator !() const { return !m_object; }

  struct Traits : public ZuTraits<T *> {
    enum { IsPrimitive = 0, IsPOD = 0 };
  };
  friend Traits ZuTraitsType(ZuPtr *);

protected:
  T		*m_object;
};

template <typename T> struct ZuCmp;
template <typename T>
struct ZuCmp<ZuPtr<T> > : public ZuCmp<T *> {
  static bool null(const ZuPtr<T> &r) { return !r; }
  static const ZuPtr<T> &null() { static const ZuPtr<T> v; return v; }
};

template <typename T> struct ZuHash;
template <typename T>
struct ZuHash<ZuPtr<T> > : public ZuHash<T *> { };

template <typename T> ZuPtr<T> ZuMkPtr(T *p) { return ZuPtr<T>{p}; }

#endif /* ZuPtr_HPP */
