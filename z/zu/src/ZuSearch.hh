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
  typename T = decltype(ZuDeclVal<const Array &>()[0])>
inline auto ZuSearch(const Array &data, unsigned n, Cmp cmp) ->
ZuExact<int, decltype(ZuDeclVal<Cmp>()(ZuDeclVal<const T &>())), unsigned>
{
  if (!n) return 0;
  unsigned o = 0;
  unsigned p = n>>1;
  int l = cmp(data[p]);
  while (n > 2) {
    n -= p;
    if (l > 0) o += p;
    l = cmp(data[o + (p = n>>1)]);
  }
  int r;
  if (n == 2) {
    if (l > 0) return (o + 2)<<1;
    r = l;
    l = cmp(data[o]);
  }
  if constexpr (Match) if (!l) return (o<<1) | 1;
  if (l <= 0) return o<<1;
  if (n == 1) return (o + 1)<<1;
  if constexpr (Match) if (!r) return ((o + 1)<<1) | 1;
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
  typename T = decltype(ZuDeclVal<const Array &>()[0])>
inline auto ZuInterSearch(const Array &data, unsigned n, Cmp cmp) ->
ZuExact<int, decltype(ZuDeclVal<Cmp>()(ZuDeclVal<const T &>())), unsigned>
{
  if (!n) return 0;
  if (n <= 2) { // special case for small arrays
    // std::cout << "cmp(0)\n";
    int l = cmp(data[0]);
    if constexpr (Match) if (!l) return 1;
    if (l <= 0) return 0;
    if (n == 1) return 2;
    // std::cout << "cmp(1)\n";
    int r = cmp(data[1]);
    if constexpr (Match) if (!r) return 3;
    if (r <= 0) return 2;
    return 4;
  }
  unsigned o = 0;
  // std::cout << "cmp(0)\n";
  int l = cmp(data[0]);
  // std::cout << "cmp(" << (n - 1) << ")\n";
  int r = cmp(data[n - 1]);
  if constexpr (Match) if (!l) return 1;
  if (l <= 0) return 0;
  if (r > 0) return n<<1;
  if (n == 3) {
    r = cmp(data[1]);
  } else {
    do {
      unsigned p;
      if (n <= 8)
	p = n>>1; // use binary search for small partitions
      else {
	unsigned d = l - r; // "distance" of left-to-right value span
	p = ((n - 3) * l + (d>>1)) / d + 1; // left/right interpolated pivot
      }
      int m = cmp(data[o + p]);
      // std::cout << "l=" << l << " r=" << r << " p=" << p << " o=" << o << " n=" << n << " cmp(" << (o + p) << ")=" << m << "\n";
      if (m <= 0) {
	n = p;
	r = m;
      } else {
	o += p;
	n -= p;
	l = m;
      }
    } while (n > 2);
    if constexpr (Match) if (!l) return (o<<1) | 1;
    if (l <= 0) return o<<1;
    if (n == 1) return (o + 1)<<1;
  }
  if constexpr (Match) if (!r) return ((o + 1)<<1) | 1;
  if (r <= 0) return (o + 1)<<1;
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
