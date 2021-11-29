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

// optimized memory growth algorithm for buffers

#ifndef ZuGrow_HPP
#define ZuGrow_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

// ZuGrow(oldSize, newSize) -> returns allocSize >= newSize bytes,
// accounting for malloc header/trailer overheads, exponentially
// doubling each allocation up to 64k, then adding 64k chunks linearly

inline constexpr unsigned ZuGrow(unsigned o, unsigned n) {
  if (ZuUnlikely(o > n)) return o;

  constexpr const unsigned v = (sizeof(void *)<<1); // malloc overhead estimate

  o += v, n += v;

  if (n < 128) return 128 - v;	// minimum 128 bytes

  constexpr const unsigned m = (1U<<16) - 1;	// 64K mask

  if (o < 128) o = 128;

  if (o <= m && n < (o<<1)) return (o<<1) - v;// double old size up to 64k

  return ((n + m) & ~m) - v;	// round up to nearest 64k - overhead
}

#endif /* ZuGrow_HPP */
