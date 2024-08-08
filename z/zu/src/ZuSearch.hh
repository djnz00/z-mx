//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// binary and interpolation search of sorted data

#ifndef ZuSearch_HH
#define ZuSearch_HH

inline bool ZuSearchFound(unsigned i) { return i & 1; }
inline unsigned ZuSearchPos(unsigned i) { return i>>1; }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

// binary search in sorted data (i.e. following ZuSort)
// - returns insertion position if not found
template <typename, typename> struct ZuSearch_OK;
template <typename R> struct ZuSearch_OK<R, int> { using T = R; }; 

template <
  bool Match = true,
  typename Array,
  typename Cmp,
  typename T = decltype(ZuDeclVal<const Array &>()[0]),
  decltype(
    ZuExact<int, decltype(ZuDeclVal<Cmp>()(ZuDeclVal<const T &>()))>(),
    int()) = 0>
inline unsigned ZuSearch(const Array &data, unsigned n, Cmp cmp) {
  if (!n) return 0;
  unsigned o = 0;
  int m = n>>1;
  int v1 = cmp(data[m]);
  while (n > 2) {
    n -= m;
    if (v1 > 0) o += m;
    v1 = cmp(data[o + (m = n>>1)]);
  }
  int v2;
  if (n == 2) {
    if (v1 > 0) return (o + 2)<<1;
    v2 = v1;
    v1 = cmp(data[o]);
  }
  if constexpr (Match) if (!v1) return (o<<1) | 1;
  if (v1 <= 0) return o<<1;
  if (n == 1) return (o + 1)<<1;
  if constexpr (Match) if (!v2) return ((o + 1)<<1) | 1;
  return (o + 1)<<1;
}
template <
  bool Match = true,
  typename Array,
  typename T>
inline auto ZuSearch(const Array &data, unsigned n, const T &v) ->
ZuExact<ZuDecay<T>, ZuDecay<decltype(ZuDeclVal<const Array &>()[0])>, unsigned>
{
  return ZuSearch<Match>(data, n,
      [&v](const T &w) { return ZuCmp<T>::cmp(v, w); });
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

// interpolation search in sorted data (i.e. following ZuSort)
// - returns insertion position if not found
template <
  bool Match = true,
  typename Array,
  typename Cmp,
  typename T = decltype(ZuDeclVal<const Array &>()[0]),
  decltype(
    ZuExact<int, decltype(ZuDeclVal<Cmp>()(ZuDeclVal<const T &>()))>(),
    int()) = 0>
inline unsigned ZuInterSearch(const Array &data, unsigned n, Cmp cmp) {
  if (!n) return 0;
  if (n <= 2) { // special case for small arrays
    int v1 = cmp(data[0]);
    if constexpr (Match) if (!v1) return 1;
    if (v1 <= 0) return 0;
    if (n == 1) return 2;
    int v2 = cmp(data[1]);
    if constexpr (Match) if (!v2) return 3;
    if (v2 <= 0) return 2;
    return 4;
  }
  unsigned o = 0;
  int v1 = cmp(data[0]);
  int v2 = cmp(data[n - 1]);
  if constexpr (Match) if (!v1) return 1;
  if (v1 <= 0) return 0;
  if (v2 > 0) return n<<1;
  if (n == 3) {
    v2 = cmp(data[1]);
  } else {
    do {
      unsigned m = ((n - 3) * v1) / (v1 - v2) + 1;
      int v3 = cmp(data[o + m]);
      n -= m;
      if (v3 <= 0) {
	v2 = v3;
      } else {
	o += m;
	v1 = v3;
      }
    } while (n > 2);
    if constexpr (Match) if (!v1) return (o<<1) | 1;
    if (v1 <= 0) return o<<1;
    if (n == 1) return (o + 1)<<1;
  }
  if constexpr (Match) if (!v2) return ((o + 1)<<1) | 1;
  if (v2 <= 0) return (o + 1)<<1;
  return (o + 2)<<1;
}
template <
  bool Match = true,
  typename Array,
  typename T>
inline auto ZuInterSearch(const Array &data, unsigned n, const T &v) ->
ZuExact<ZuDecay<T>, ZuDecay<decltype(ZuDeclVal<const Array &>()[0])>, unsigned>
{
  return ZuInterSearch<Match>(data, n,
    [v](const T &w) { return ZuCmp<T>::delta(v, w); });
}

#endif /* ZuSearch_HH */
