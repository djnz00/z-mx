//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>

#include <zlib/ZuCmp.hh>

#include <zlib/ZmAtomic.hh>
#include <zlib/ZmStack.hh>
#include <zlib/ZmXRing.hh>

struct C {
  C() : m_i(0) { m_count++; }
  C(int i) : m_i(i) { m_count++; }
  C(const C &c) : m_i(c.m_i) { m_count++; }
  C &operator =(const C &c) {
    if (this != &c) m_i = c.m_i;
    return *this;
  }
  ~C() { --m_count; }
  int value() const { return m_i; }
  bool equals(const C &c) const { return m_i == c.m_i; }
  int cmp(const C &c) const { return ZuCmp<int>::cmp(m_i, c.m_i); }
  friend inline bool operator ==(const C &l, const C &r) {
    return l.equals(r);
  }
  friend inline int operator <=>(const C &l, const C &r) {
    return l.cmp(r);
  }
  bool operator !() const { return !m_i; }

  int				m_i;
  static ZmAtomic<uint32_t>	m_count;
};

template <typename S>
void dump(S &s)
{
  typename S::Iterator i(s);
  {
    C c;

    while (!ZuCmp<C>::null(c = i.iterate())) printf("%d ", c.value());
  }
  puts("");
  fflush(stdout);
}

template <typename S>
void dump2(S &s)
{
  typename S::RevIterator i(s);
  {
    C c;

    while (!ZuCmp<C>::null(c = i.iterate())) printf("%d ", c.value());
  }
  puts("");
  fflush(stdout);
}

void fail(const char *s) 
{
  printf("FAIL: %s\n", s);
}

#define test(x) ((x) ? (void()) : fail(#x))

template <typename S>
void doit(S &s)
{
  static int del1[] = { 8,7,6,4,3,1,-1 };
  static int del2[] = { 1,3,4,6,7,8,-1 };
  int i;

  for (i = 1; i < 10; i++) s.push(C{i});
  for (i = 0; del1[i] >= 0; i++) s.del(C{del1[i]});
  dump(s);
  test(s.pop().value() == 9);
  test(s.pop().value() == 5);
  test(s.pop().value() == 2);
  test(ZuCmp<C>::null(s.pop()));
  for (i = 9; i > 0; i--) s.push(C{i});
  for (i = 0; del2[i] >= 0; i++) s.del(C{del2[i]});
  dump(s);
  test(s.pop().value() == 2);
  test(s.pop().value() == 5);
  test(s.pop().value() == 9);
  test(ZuCmp<C>::null(s.pop()));
}

template <typename S>
void doit2(S &s)
{
  static int del1[] = { 8,7,6,4,3,1,-1 };
  static int del2[] = { 1,3,4,6,7,8,-1 };
  int i;

  for (i = 1; i < 10; i++) s.push(C{i});
  for (i = 0; del1[i] >= 0; i++) s.del(C{del1[i]});
  dump(s);
  test(s.pop().value() == 9);
  test(s.pop().value() == 5);
  test(s.pop().value() == 2);
  test(ZuCmp<C>::null(s.pop()));
  for (i = 1; i < 10; i++) s.push(C{i});
  for (i = 0; del2[i] >= 0; i++) s.del(C{del2[i]});
  dump(s);
  test(s.shift().value() == 2);
  test(s.shift().value() == 5);
  test(s.shift().value() == 9);
  test(ZuCmp<C>::null(s.shift()));
  for (i = 1; i < 10; i++) s.unshift(C{i});
  for (i = 0; del1[i] >= 0; i++) s.del(C{del1[i]});
  dump2(s);
  test(s.shift().value() == 9);
  test(s.shift().value() == 5);
  test(s.shift().value() == 2);
  test(ZuCmp<C>::null(s.shift()));
  for (i = 1; i < 10; i++) s.unshift(C{i});
  for (i = 0; del2[i] >= 0; i++) s.del(C{del2[i]});
  dump2(s);
  test(s.pop().value() == 2);
  test(s.pop().value() == 5);
  test(s.pop().value() == 9);
  test(ZuCmp<C>::null(s.pop()));
  s.clean();
  int n = s.size();
  s.push(C{0});
  for (i = 1; i < n; i++) { s.push(C{i}); s.shift(); }
  for (i = 0; i < n - 1; i++) s.push(C{i});
  s.clean();
  n = s.size();
  s.push(C{0});
  for (i = 1; i < n; i++) { s.push(C{i}); s.shift(); }
  n = s.size() + 1;
  for (i = 0; i < n; i++) s.push(C{i});
}

ZmAtomic<uint32_t> C::m_count = 0;

int main(int argc, char **argv)
{
  for (int i = 0; i < 100; i += 10) {
    ZmStack<C> s1, s2, s3;

    s1.init(ZmStackParams().initial(1).increment(1).maxFrag(i));
    s2.init(ZmStackParams().initial(2).increment(3).maxFrag(i));
    s3.init(ZmStackParams().initial(9).increment(9).maxFrag(i));

    doit(s1);
    doit(s2);
    doit(s3);
  }

  test(C::m_count <= 1);

  for (int i = 0; i < 100; i += 10) {
    ZmXRing<C> r1, r2, r3;

    r1.init(ZmXRingParams().initial(1).increment(1).maxFrag(i));
    r2.init(ZmXRingParams().initial(2).increment(3).maxFrag(i));
    r3.init(ZmXRingParams().initial(9).increment(9).maxFrag(i));

    doit2(r1);
    doit2(r2);
    doit2(r3);
  }

  test(C::m_count <= 1);
}
