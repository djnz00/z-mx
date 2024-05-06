//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// intrusively reference-counted polymorphic object (base class)
// - ZmAtomic<int> reference count (multithread-safe)

#ifndef ZmPolymorph_HH
#define ZmPolymorph_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <stddef.h>

#ifdef ZmObject_DEBUG
#include <stdlib.h>
#endif

#include <zlib/ZmAtomic.hh>
#ifdef ZmObject_DEBUG
#include <zlib/ZmObjectDebug.hh>
#endif

class ZmPolymorph
#ifdef ZmObject_DEBUG
: public ZmObjectDebug
#endif
{
  ZmPolymorph(const ZmPolymorph &) = delete;
  ZmPolymorph &operator =(const ZmPolymorph &) = delete;

  friend ZmPolymorph ZuObjectType(ZmPolymorph *);

public:
  ZmPolymorph()  : m_refCount{0} { }

  virtual ~ZmPolymorph() {
#ifdef ZmObject_DEBUG
    this->del_();
#endif
  }

  int refCount() const { return m_refCount.load_(); }

#ifdef ZmObject_DEBUG
  void ref(const void *referrer = 0) const
#else
  void ref() const
#endif
  {
#ifdef ZmObject_DEBUG
    if (ZuUnlikely(this->deleted_())) return;
    if (ZuUnlikely(this->debugging_())) ZmObject_ref(this, referrer);
#endif
    this->ref_();
  }
#ifdef ZmObject_DEBUG
  bool deref(const void *referrer = 0) const
#else
  bool deref() const
#endif
  {
#ifdef ZmObject_DEBUG
    if (ZuUnlikely(this->deleted_())) return false;
    if (ZuUnlikely(this->debugging_())) ZmObject_deref(this, referrer);
#endif
    return this->deref_();
  }

#ifdef ZmObject_DEBUG
  void mvref(const void *prev, const void *next) const {
    if (ZuUnlikely(this->debugging_())) {
      ZmObject_ref(this, next);
      ZmObject_deref(this, prev);
    }
  }
#endif

  // apps occasionally need to manipulate the refCount directly
  void ref_() const { ++m_refCount; }
  void ref2_() const { m_refCount += 2; }
  bool deref_() const { return !--m_refCount; }

private:
#ifdef ZmObject_DEBUG
  bool deleted_() const { return m_refCount.load_() < 0; }
  void del_() const { m_refCount.store_(-1); }
#endif

  mutable ZmAtomic<int>		m_refCount;
};

#endif /* ZmPolymorph_HH */
