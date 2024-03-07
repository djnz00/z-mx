//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <zlib/ZuAssert.hpp>
#include <zlib/ZuUnroll.hpp>
#include <zlib/ZuUnroll.hpp>

#include <iostream>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

#define DEFINE(ID, I_) \
struct ID { enum { I = I_ }; static const char *id() { return #ID; } }

DEFINE(A, 3);
DEFINE(B, 2);
DEFINE(C, 1);
DEFINE(D, 5);
DEFINE(E, 4);

template <typename T> struct Index : public ZuUnsigned<T::I> { };

using Sorted = ZuTypeSort<Index, A, B, C, D, E>;

struct X {
  X() = default;
  X(int j_, int k_) : j{j_}, k{k_} { }
  int i = 42, j = 43, k = 44;
};

int main(int argc, char **argv)
{
  {
    ZuUnroll::all<Sorted>([]<typename T>() {
      std::cout << T::I << ' ' << T::id() << '\n';
    });
    X x;
    // X y = x;
    X z{x};
    X q;
    q = x;
  }
  {
    ZuUnroll::all<ZuMkSeq<4>>([](auto i) { std::cout << i << '\n'; });
    ZuAssert(ZuUnroll::all<ZuMkSeq<4>>(0, [](auto i, int j) {
      return j + 1;
    }) == 4);
    auto j = ZuUnroll::all<ZuMkSeq<4>>(0, [](auto i, int j) {
      std::cout << i << '\n';
      return j + 1;
    });
    std::cout << "j=" << j << '\n';
  }
}
