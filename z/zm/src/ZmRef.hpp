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

// intrusively reference-counted smart pointer class

#ifndef ZmRef_HPP
#define ZmRef_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInspect.hpp>
#include <zlib/ZuRef.hpp>
#include <zlib/ZuObjectTraits.hpp>

#ifdef ZmObject_DEBUG
#include <zlib/ZmObjectDebug.hpp>
#endif

// rules for using ZmRef

// * always point to objects allocated using new (use ZmHeap to optimize)
// * always point to objects which derive from ZmObject
// * be careful to maintain a positive reference count when mixing
//   with real pointers - objects will delete themselves from
//   under you if they think they are referenced by nothing!
// * pass by raw pointer and return by ZmRef value

void ZmRefType(...);

template <typename> class ZmRef;

#ifdef ZmObject_DEBUG
struct ZmRef__ {
  template <typename O> static ZuIs<ZmObjectDebug, O>
  ZmREF_(const O *o, const void *p) { o->ref(p); }
  template <typename O> static ZuIs<ZmObjectDebug, O>
  ZmREF_(const ZmRef<O> &o, const void *p) { o->ref(p); }
  template <typename O> static ZuIs<ZmObjectDebug, O>
  ZmDEREF_(const O *o, const void *p) { if (o->deref(p)) delete o; }
  template <typename O> static ZuIs<ZmObjectDebug, O>
  ZmDEREF_(const ZmRef<O> &o, const void *p)
    { if (o->deref(p)) delete o.ptr(); }
  template <typename O> static ZuIs<ZmObjectDebug, O>
  ZmMVREF_(const O *o, const void *p, const void *n) { o->mvref(p, n); }
  template <typename O> static void
  ZmMVREF_(const ZmRef<O> &o, const void *p, const void *n) { o->mvref(p, n); }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmREF_(const O *o, const void *) { o->ref(); }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmREF_(const ZmRef<O> &o, const void *) { o->ref(); }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmDEREF_(const O *o, const void *) { if (o->deref()) delete o; }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmDEREF_(const ZmRef<O> &o, const void *) { if (o->deref()) delete o.ptr(); }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmMVREF_(const O *, const void *, const void *) { }
  template <typename O> static ZuIsNot<ZmObjectDebug, O>
  ZmMVREF_(const ZmRef<O> &, const void *, const void *) { }
};
#define ZmREF(o) ZmRef__::ZmREF_((o), this)
#define ZmDEREF(o) ZmRef__::ZmDEREF_((o), this)
#define ZmMVREF(o, p, n) ZmRef__::ZmMVREF_((o), (p), (n))
#else
struct ZmRef__ {
  template <typename O>
  static void ZmDEREF_(const O *o) { if (o->deref()) delete o; }
  template <typename O>
  static void ZmDEREF_(const ZmRef<O> &o) { if (o->deref()) delete o.ptr(); }
};
#define ZmREF(o) ((o)->ref())
#define ZmDEREF(o) ZmRef__::ZmDEREF_(o)
#define ZmMVREF(o, p, n) (void())
#endif

template <typename T_> class ZmRef {
template <typename> friend class ZmRef;
  friend T_ ZmRefType(ZmRef *);

public:
  using T = T_;

private:
  enum Acquire_ { Acquire };
  ZmRef(T *o, Acquire_ _) : m_object{o} { }

  // matches ZmRef<U> where U is not T, but is in the same type hierarchy as T
  template <typename V> struct IsOtherRef_ :
      public ZuBool<ZuInspect<T, V>::Base || ZuInspect<V, T>::Base> { };
  template <typename U> struct IsOtherRef :
      public IsOtherRef_<decltype(ZmRefType(ZuDeclVal<U *>()))> { };
  template <typename U, typename = void, bool = IsOtherRef<U>{}>
  struct MatchOtherRef_ { };
  template <typename U, typename R>
  struct MatchOtherRef_<U, R, true> { using T = R; };
  template <typename U, typename R = void>
  using MatchOtherRef = typename MatchOtherRef_<U, R>::T;

  // matches ZmRef<U> where U is either T or in the same type hierarchy as T
  template <typename V> struct IsRef_ :
      public ZuBool<ZuInspect<T, V>::Is || ZuInspect<V, T>::Is> { };
  template <typename U> struct IsRef :
      public IsRef_<decltype(ZmRefType(ZuDeclVal<U *>()))> { };
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
  ZmRef() = default;
  ZmRef(const ZmRef &r) : m_object{r.m_object} {
    if (T *o = m_object) ZmREF(o);
  }
  ZmRef(ZmRef &&r) : m_object{r.m_object} {
    r.m_object = nullptr;
#ifdef ZmObject_DEBUG
    if (T *o = m_object) ZmMVREF(o, &r, this);
#endif
  }
  template <typename R>
  ZmRef(R &&r, MatchOtherRef<ZuDeref<R>> *_ = nullptr) noexcept : m_object{
      static_cast<T *>(const_cast<typename ZuDeref<R>::T *>(r.m_object))} {
    ZuBind<R>::mvcp(ZuFwd<R>(r),
#ifndef ZmObject_DEBUG
	[](auto &&r) { r.m_object = nullptr; }
#else
	[this](auto &&r) {
	  r.m_object = nullptr;
	  if (T *o = m_object) ZmMVREF(o, &r, this);
	}
#endif
	, [this](const auto &) { if (T *o = m_object) ZmREF(o); });
  }
  ZmRef(T *o) : m_object{o} {
    if (o) ZmREF(o);
  }
  template <typename O>
  ZmRef(O *o, MatchPtr<O> *_ = nullptr) :
      m_object{static_cast<T *>(o)} {
    if (o) ZmREF(o);
  }
  ~ZmRef() {
    if (T *o = m_object) ZmDEREF(o);
  }

  template <typename R> MatchRef<R> swap(R &r) noexcept {
    T *o = m_object;
    m_object = static_cast<T *>(r.m_object);
    r.m_object = static_cast<typename R::T *>(o);
#ifdef ZmObject_DEBUG
    if (o) ZmMVREF(o, this, &r);
    if (m_object) ZmMVREF(m_object, &r, this);
#endif
  }

  template <typename R>
  friend MatchRef<R> swap(ZmRef &r1, R &r2) noexcept {
    r1.swap(r2);
  }

  ZmRef &operator =(ZmRef r) noexcept {
    swap(r);
    return *this;
  }
  template <typename R>
  MatchOtherRef<R, ZmRef &> operator =(R r) noexcept {
    swap(r);
    return *this;
  }

  template <typename O>
  MatchPtr<O, ZmRef &> operator =(O *n) {
    if (m_object != n) {
      if (n) ZmREF(n);
      T *o = m_object;
      m_object = n;
      if (o) ZmDEREF(o);
    }
    return *this;
  }

  operator T *() const { return m_object; }
  T *operator ->() const { return m_object; }

  template <typename O = T>
  MatchRef<ZmRef<O>, O *> ptr() const {
    return static_cast<O *>(m_object);
  }
  T *ptr_() const { return m_object; }

  static ZmRef acquire(T *o) {
    return ZmRef{o, Acquire};
  }
  template <typename O = T>
  MatchRef<ZmRef<O>, O *> release() {
    T *o = m_object;
    m_object = nullptr;
    return static_cast<O *>(o);
  }

  bool operator !() const { return !m_object; }

  struct Traits : public ZuTraits<T *> {
    enum { IsPrimitive = 0, IsPOD = 0 };
  };
  friend Traits ZuTraitsType(ZmRef *);

protected:
  T		*m_object = nullptr;
};

template <typename T> struct ZuCmp;
template <typename T>
struct ZuCmp<ZmRef<T> > : public ZuCmp<T *> {
  static bool null(const ZmRef<T> &r) { return !r; }
  static const ZmRef<T> &null() { static const ZmRef<T> v; return v; }
};

template <typename T> struct ZuHash;
template <typename T>
struct ZuHash<ZmRef<T> > : public ZuHash<T *> { };

template <typename T> ZmRef<T> ZmMkRef(T *p) { return ZmRef<T>{p}; }

#endif /* ZmRef_HPP */
