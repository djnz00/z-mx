//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuArrayN.hh>
#include <zlib/ZuMArray.hh>
#include <zlib/ZuBytes.hh>
#include <zlib/ZuDemangle.hh>

class I {
public:
  I() : m_i(0) { }
  I(int i) : m_i(i) { }
  ~I() { m_i = 0; }

  I(const I &i) : m_i(i.m_i) { }
  I &operator =(const I &i) {
    if (ZuLikely(this != &i)) m_i = i.m_i;
    return *this;
  }

  I &operator =(int i) { m_i = i; return *this; }

  operator int() const { return m_i; }

private:
  int	m_i;
};

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

template <typename A>
void testSplice(A &a, int offset, int length, int check1, int check2)
{
  A a_;
  a.splice(offset, length, a_);
  CHECK((!a.length() && !check1) || (int)a[0] == check1);
  if (check2 < 0)
    CHECK(!a_.length());
  else
    CHECK((!a.length() && !check2) || (int)a_[0] == check2);
}

template <typename U, typename = void>
struct IsIterable_ : public ZuFalse { };
template <typename U>
struct IsIterable_<U, decltype(
  ZuDeclVal<const U &>().end() - ZuDeclVal<const U &>().begin(), void())> :
    public ZuTrue { };
template <typename U, typename V>
struct IsIterable : public ZuBool<
    !ZuInspect<U, V>::Same &&
    !ZuTraits<U>::IsSpan &&
    bool(IsIterable_<ZuDecay<U>>{}) &&
    ZuInspect<typename ZuTraits<U>::Elem, V>::Converts> { };

int main()
{
  {
    ZuArrayN<I, 1> a;
    a << I(42);
    CHECK((int)a[0] == 42);
    a << I(43);
    CHECK((int)a[0] == 42);
    testSplice(a, 0, 1, 0, 42);
    a << I(42);
    testSplice(a, 1, 1, 42, -1);
    testSplice(a, 0, 2, 0, 42);
  }
  {
    ZuArrayN<I, 2> a;
    a << I(42);
    CHECK((int)a[0] == 42);
    a << I(43);
    CHECK((int)a[1] == 43);
    testSplice(a, 0, 1, 43, 42);
    a << I(42);
    testSplice(a, 1, 1, 43, 42);
    testSplice(a, -1, 3, 0, 43);
  }
  {
    ZuArrayN<I, 3> a;
    a << I(42);
    a << I(43);
    a << I(44);
    a << I(45);
    CHECK((int)a[0] == 42);
    CHECK((int)a[2] == 44);
    testSplice(a, 0, 2, 44, 42);
    a << I(45);
    testSplice(a, 1, 1, 44, 45);
    testSplice(a, -2, 4, 0, 44);
  }

  {
    ZuMArray<ZuBytes> a;
    CHECK(ZuTraits<decltype(a)>::IsArray);
    CHECK(!ZuTraits<decltype(a)>::IsSpan);
    CHECK(IsIterable_<decltype(a)>{});
    std::cout << ZuDemangle<decltype(a)>{} << '\n';
    std::cout << ZuDemangle<typename ZuTraits<decltype(a)>::Elem>{} << '\n';
    CHECK((IsIterable<decltype(a), ZuArray<const uint8_t>>{}));
    CHECK((ZuInspect<typename ZuTraits<decltype(a)>::Elem, ZuArray<const uint8_t>>::Constructs));
  }
}
