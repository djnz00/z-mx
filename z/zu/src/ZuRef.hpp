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

// intrusively reference-counted smart pointer

#ifndef ZuRef_HPP
#define ZuRef_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInspect.hpp>

// rules for using ZuRef

// * always point to objects allocated using new
// * always point to objects which derive from ZuObject
// * be careful to maintain a positive reference count when mixing
//   with real pointers - objects will delete themselves from
//   under you if they think they are referenced by nothing!
// * pass by raw pointer (unless moving), and return by ZuRef value

void ZuRefType(...);

template <typename T_> class ZuRef {
template <typename> friend class ZuRef;
  friend T_ ZuRefType(ZuRef *);

public:
  using T = T_;

private:
  enum Acquire_ { Acquire };
  ZuRef(T *o, Acquire_ _) : m_object{o} { }

  // matches ZuRef<U> where U is not T, but is in the same type hierarchy as T
  template <typename V> struct IsOtherRef_ :
      public ZuBool<ZuInspect<T, V>::Base || ZuInspect<V, T>::Base> { };
  template <typename U> struct IsOtherRef :
      public IsOtherRef_<decltype(ZuRefType(ZuDeclVal<U *>()))> { };
  template <typename U, typename = void, bool = IsOtherRef<U>{}>
  struct MatchOtherRef_ { };
  template <typename U, typename R>
  struct MatchOtherRef_<U, R, true> { using T = R; };
  template <typename U, typename R = void>
  using MatchOtherRef = typename MatchOtherRef_<U, R>::T;

  // matches ZuRef<U> where U is either T or in the same type hierarchy as T
  template <typename V> struct IsRef_ :
      public ZuBool<ZuInspect<T, V>::Is || ZuInspect<V, T>::Is> { };
  template <typename U> struct IsRef :
      public IsRef_<decltype(ZuRefType(ZuDeclVal<U *>()))> { };
  template <typename U, typename = void, bool = IsRef<U>{}>
  struct MatchRef_ { };
  template <typename U, typename R>
  struct MatchRef_<U, R, true> { using T = R; };
  template <typename U, typename R = void>
  using MatchRef = typename MatchRef_<U, R>::T;

  // matches U * where U is either T or in the same type hierarchy as T
  template <typename U> struct IsPtr :
    public ZuBool<(ZuInspect<T, U>::Is || ZuInspect<U, T>::Is)> { };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>{}, R>;

public:
  ZuRef() : m_object{0} { }
  ZuRef(const ZuRef &r) : m_object{r.m_object} {
    if (T *o = m_object) o->ref();
  }
  ZuRef(ZuRef &&r) : m_object{r.m_object} {
    r.m_object = 0;
  }
  template <typename R>
  ZuRef(R &&r, MatchOtherRef<ZuDeref<R>> *_ = nullptr) noexcept :
    m_object{
      static_cast<T *>(const_cast<ZuDeref<R> *>(r.m_object))} {
    ZuBind<R>::mvcp(ZuFwd<R>(r),
	[](auto &&r) { r.m_object = 0; },
	[this](const auto &) { if (T *o = m_object) o->ref(); });
  }
  ZuRef(T *o) : m_object{o} {
    if (o) o->ref();
  }
  template <typename O>
  ZuRef(O *o, MatchPtr<O> *_ = nullptr) :
      m_object{static_cast<T *>(o)} {
    if (o) o->ref();
  }
  ~ZuRef() {
    if (T *o = m_object) if (o->deref()) delete o;
  }

  template <typename R> MatchRef<R> swap(R &r) noexcept {
    T *o = m_object;
    m_object = static_cast<T *>(r.m_object);
    r.m_object = static_cast<typename R::T *>(o);
  }

  template <typename R>
  friend MatchRef<R> swap(ZuRef &r1, R &r2) noexcept {
    r1.swap(r2);
  }

  ZuRef &operator =(ZuRef r) noexcept {
    swap(r);
    return *this;
  }
  template <typename R>
  MatchOtherRef<R, ZuRef &> operator =(R r) noexcept {
    swap(r);
    return *this;
  }

  template <typename O>
  MatchPtr<O, ZuRef &> operator =(O *n) {
    if (n) n->ref();
    T *o = m_object;
    m_object = n;
    if (o && o->deref()) delete o;
    return *this;
  }

  operator T *() const { return m_object; }
  T *operator ->() const { return m_object; }

  template <typename O = T>
  MatchRef<ZuRef<O>, O *> ptr() const {
    return static_cast<O *>(m_object);
  }
  T *ptr_() const { return m_object; }

  static ZuRef acquire(T *o) {
    return ZuRef{o, Acquire};
  }
  template <typename O = T>
  MatchRef<ZuRef<O>, O *> release() {
    T *o = m_object;
    m_object = nullptr;
    return static_cast<O *>(o);
  }

  bool operator !() const { return !m_object; }

  // traits
  struct Traits : public ZuTraits<T *> {
    enum { IsPrimitive = 0, IsPOD = 0 };
  };
  friend Traits ZuTraitsType(ZuRef *);

protected:
  T		*m_object;
};

template <typename T> struct ZuCmp;
template <typename T>
struct ZuCmp<ZuRef<T> > : public ZuCmp<T *> {
  static bool null(const ZuRef<T> &r) { return !r; }
  static const ZuRef<T> &null() { static const ZuRef<T> v; return v; }
};

template <typename T> struct ZuHash;
template <typename T>
struct ZuHash<ZuRef<T> > : public ZuHash<T *> { };

template <typename T> ZuRef<T> ZuMkRef(T *p) { return ZuRef<T>{p}; }

#endif /* ZuRef_HPP */
