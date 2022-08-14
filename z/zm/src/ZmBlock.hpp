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

// generic blocking call to async function with continuation

#ifndef ZmBlock_HPP
#define ZmBlock_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuTuple.hpp>

#include <zlib/ZmSemaphore.hpp>

template <typename ...> struct ZmBlock;
class ZmBlock_ {
  template <typename ...> friend struct ZmBlock;
  static auto sem() {
    thread_local ZmSemaphore sem;
    return &sem;
  }
};
template <typename ...Args> struct ZmBlock {
  using R = ZuTuple<Args...>;
  template <typename L>
  R operator ()(L l) const {
    R r;
    auto sem = ZmBlock_::sem();
    l([sem, &r](Args... args) {
      r = ZuMvTuple(args...);
      sem->post();
    });
    sem->wait();
    return r;
  }
};
template <typename Arg> struct ZmBlock<Arg> {
  using R = Arg;
  template <typename L>
  R operator ()(L l) const {
    R r;
    auto sem = ZmBlock_::sem();
    l([sem, &r](Arg arg) {
      r = ZuMv(arg);
      sem->post();
    });
    sem->wait();
    return r;
  }
};
template <> struct ZmBlock<> {
  template <typename L>
  void operator ()(L l) const {
    auto sem = ZmBlock_::sem();
    l([sem]() { sem->post(); });
    sem->wait();
    return;
  }
};

#endif /* ZmBlock_HPP */
