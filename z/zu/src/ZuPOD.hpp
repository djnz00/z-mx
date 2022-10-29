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

// reference-counted POD structures

#ifndef ZuPOD_HPP
#define ZuPOD_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <zlib/ZuInt.hpp>

// generic run-time access to a reference-counted generic POD type with
// run-time variadic access to pointer and size

class ZuAnyPOD_Data {
public:
  template <typename T = void> const T *ptr() const {
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(this);
    return ptr;
  }
  template <typename T = void> T *ptr() {
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(this);
    return ptr;
  }

  template <typename T> const T &as() const {
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(this);
    return *ptr;
  }
  template <typename T> T &as() {
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(this);
    return *ptr;
  }

private:
  uintptr_t	m_data;
};

class ZuAnyPOD_Size {
public:
  ZuAnyPOD_Size(unsigned size) : m_size(size) { }

  unsigned size() const { return m_size; }

private:
  unsigned	m_size;
};

template <typename Base>
class ZuAnyPOD_ : public Base, public ZuAnyPOD_Size, public ZuAnyPOD_Data {
public:
  template <typename ...Args>
  ZuAnyPOD_(unsigned size, Args &&... args) :
    Base(ZuFwd<Args>(args)...), ZuAnyPOD_Size(size) { }
  virtual ~ZuAnyPOD_() { }

  using ZuAnyPOD_Size::size;
  using ZuAnyPOD_Data::ptr;
  using ZuAnyPOD_Data::as;
};

#include <zlib/ZuPolymorph.hpp>

using ZuAnyPOD = ZuAnyPOD_<ZuPolymorph>;

template <typename T_, class Base,
  bool Small = sizeof(T_) <= sizeof(uintptr_t)>
class ZuPOD_ : public ZuAnyPOD_<Base> {
public:
  using T = T_;

  template <typename ...Args>
  ZuPOD_(Args &&... args) :
    ZuAnyPOD_<Base>(sizeof(T), ZuFwd<Args>(args)...) { }

  template <typename T = T_>
  const T *ptr() const { return ZuAnyPOD_<Base>::template ptr<T>(); }
  template <typename T = T_>
  T *ptr() { return ZuAnyPOD_<Base>::template ptr<T>(); }

  const T &data() const { return this->template as<T>(); }
  T &data() { return this->template as<T>(); }

  static const ZuPOD_ *pod(const T *data) {
    const ZuAnyPOD_Data *ZuMayAlias(ptr) =
      reinterpret_cast<const ZuAnyPOD_Data *>(data);
    return static_cast<const ZuPOD_ *>(ptr);
  }
  static ZuPOD_ *pod(T *data) {
    ZuAnyPOD_Data *ZuMayAlias(ptr) = reinterpret_cast<ZuAnyPOD_Data *>(data);
    return static_cast<ZuPOD_ *>(ptr);
  }

private:
  char		m_data[sizeof(T) - sizeof(uintptr_t)];
};
template <typename T_, class Base>
class ZuPOD_<T_, Base, 1> : public ZuAnyPOD_<Base> {
public:
  using T = T_;

  template <typename ...Args>
  ZuPOD_(Args &&... args) :
    ZuAnyPOD_<Base>(sizeof(T), ZuFwd<Args>(args)...) { }

  template <typename U = T>
  const U *ptr() const { return ZuAnyPOD_<Base>::template ptr<U>(); }
  template <typename U = T>
  U *ptr() { return ZuAnyPOD_<Base>::template ptr<U>(); }

  const T &data() const { return this->template as<T>(); }
  T &data() { return this->template as<T>(); }
};

template <typename T> using ZuPOD = ZuPOD_<T, ZuPolymorph>;

#endif /* ZuPOD_HPP */
