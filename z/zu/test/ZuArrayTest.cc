//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuArrayN.hh>

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

void test_(bool b, const char *s)
{
  printf("%s: %s\n", b ? " OK" : "NOK", s);
}

#define test(x) test_((x), #x)

template <typename A>
void testSplice(A &a, int offset, int length, int check1, int check2)
{
  A a_;
  a.splice(offset, length, a_);
  test((!a.length() && !check1) || (int)a[0] == check1);
  if (check2 < 0)
    test(!a_.length());
  else
    test((!a.length() && !check2) || (int)a_[0] == check2);
}

int main()
{
  {
    ZuArrayN<I, 1> a;
    a << I(42);
    test((int)a[0] == 42);
    a << I(43);
    test((int)a[0] == 42);
    testSplice(a, 0, 1, 0, 42);
    a << I(42);
    testSplice(a, 1, 1, 42, -1);
    testSplice(a, 0, 2, 0, 42);
  }
  {
    ZuArrayN<I, 2> a;
    a << I(42);
    test((int)a[0] == 42);
    a << I(43);
    test((int)a[1] == 43);
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
    test((int)a[0] == 42);
    test((int)a[2] == 44);
    testSplice(a, 0, 2, 44, 42);
    a << I(45);
    testSplice(a, 1, 1, 44, 45);
    testSplice(a, -2, 4, 0, 44);
  }
  {
    static const int names[] = { 0, 1, 2 };
    test(ZuArray<const int>::IsPrimitiveArray<ZuDecay<decltype(names)>>{});
  }
}
