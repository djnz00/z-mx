//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z library compile-time numerical sequence

#ifndef ZuSeq_HH
#define ZuSeq_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTL.hh>

// main template
template <unsigned ...I> struct ZuSeq {
  enum { N = sizeof...(I) };
};

// generate unsigned sequence [0, N)
template <typename T_, unsigned, bool> struct ZuPushSeq_ {
  using T = T_;
};
template <unsigned ...I, unsigned N>
struct ZuPushSeq_<ZuSeq<I...>, N, true> {
  enum { J = sizeof...(I) };
  using T = typename ZuPushSeq_<ZuSeq<I..., J>, N, (J < N - 1)>::T;
};
template <unsigned N> struct ZuMkSeq_ {
  using T = typename ZuPushSeq_<ZuSeq<>, N, (N > 0)>::T;
};
template <unsigned N> using ZuMkSeq = typename ZuMkSeq_<N>::T;

// convert ZuSeq to typelist
template <typename> struct ZuSeqTL_;
template <> struct ZuSeqTL_<ZuSeq<>> { using T = ZuTypeList<>; };
template <unsigned I> struct ZuSeqTL_<ZuSeq<I>> {
  using T = ZuTypeList<ZuUnsigned<I>>;
};
template <unsigned I, unsigned ...Seq>
struct ZuSeqTL_<ZuSeq<I, Seq...>> {
  using T = ZuTypeList<ZuUnsigned<I>>::template Push<
    typename ZuSeqTL_<ZuSeq<Seq...>>::T>;
};
template <typename Seq> using ZuSeqTL = typename ZuSeqTL_<Seq>::T;
// ... and back again
template <typename ...Seq> struct ZuTLSeq_ { using T = ZuSeq<Seq{}...>; };
template <typename ...Seq>
struct ZuTLSeq_<ZuTypeList<Seq...>> : public ZuTLSeq_<Seq...> { };
template <typename ...Seq> using ZuTLSeq = typename ZuTLSeq_<Seq...>::T;

// min/max of a numerical sequence
template <typename> struct ZuMin;
template <> struct ZuMin<ZuSeq<>> : public ZuUnsigned<UINT_MAX> { };
template <unsigned I> struct ZuMin<ZuSeq<I>> : public ZuUnsigned<I> { };
template <unsigned I, unsigned J>
struct ZuMin<ZuSeq<I, J>> : public ZuUnsigned<(I < J) ? I : J> { };
template <unsigned I, unsigned J, unsigned ...Seq>
struct ZuMin<ZuSeq<I, J, Seq...>> :
    public ZuMin<ZuSeq<((I < J) ? I : J), Seq...>> { };

template <typename> struct ZuMax;
template <> struct ZuMax<ZuSeq<>> : public ZuUnsigned<0> { };
template <unsigned I> struct ZuMax<ZuSeq<I>> : public ZuUnsigned<I> { };
template <unsigned I, unsigned J>
struct ZuMax<ZuSeq<I, J>> : public ZuUnsigned<(I > J) ? I : J> { };
template <unsigned I, unsigned J, unsigned ...Seq>
struct ZuMax<ZuSeq<I, J, Seq...>> :
    public ZuMax<ZuSeq<((I > J) ? I : J), Seq...>> { };

// ZuSeqCall<Axor, N>(value, lambda)
// invokes lambda(Axor.operator ()<I>(value), ...) for I in [0,N)
template <auto Axor, typename Seq, typename T> struct ZuSeqCall_;
template <auto Axor, unsigned ...I, typename T>
struct ZuSeqCall_<Axor, ZuSeq<I...>, T> {
  template <typename L>
  static decltype(auto) fn(const T &v, L l) {
    return l(Axor.template operator ()<I>(v)...);
  }
  template <typename L>
  static decltype(auto) fn(T &v, L l) {
    return l(Axor.template operator()<I>(v)...);
  }
  template <typename L>
  static decltype(auto) fn(T &&v, L l) {
    return l(Axor.template operator()<I>(ZuMv(v))...);
  }
};
template <unsigned N, auto Axor = ZuDefaultAxor(), typename T, typename L>
inline decltype(auto) ZuSeqCall(T &&v, L l) {
  return ZuSeqCall_<Axor, ZuMkSeq<N>, ZuDecay<T>>::fn(ZuFwd<T>(v), ZuMv(l));
}

#endif /* ZuSeq_HH */
