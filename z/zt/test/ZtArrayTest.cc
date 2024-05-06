//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <assert.h>

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

struct E {
  E() : m_ptr(this), m_copied(0), m_moved(0) { }
  ~E() {
    m_ptr = 0;
  }

  E(const E &e) :
      m_ptr(this), m_copied(e.m_copied + 1), m_moved(e.m_moved) { }
  E &operator =(const E &e) {
    if (this == &e) return *this;
    m_ptr = this;
    m_copied = e.m_copied + 1;
    m_moved = e.m_moved;
    return *this;
  }
  E(E &&e) :
      m_ptr(this), m_copied(e.m_copied), m_moved(e.m_moved + 1) {
    e.m_ptr = 0;
  }
  E &operator =(E &&e) {
    m_ptr = this;
    m_copied = e.m_copied;
    m_moved = e.m_moved + 1;
    e.m_ptr = 0;
    return *this;
  }

  void *ptr() const { return m_ptr; }
  int copied() const { return m_copied; }
  int moved() const { return m_moved; }

  void	*m_ptr;
  int	m_copied;
  int	m_moved;
};

void validate(const ZtArray<E> &a, unsigned length)
{
  assert(a.length() == length);
  unsigned n = a.length();
  for (unsigned i = 0; i < n; i++) {
    if (a[i].ptr() != (const void *)&a[i]) {
      printf("\n%d %p != %p\n", i, a[i].ptr(), &a[i]);
    }
    printf("%d:%d:%d ", i, a[i].copied(), a[i].moved());
  }
  putc('\n', stdout);
}

struct Foo {
  Foo() { }
  template <typename S> Foo(const S &s) : bar(s) { }
  ZtString bar;
};

#include <vector>

int main()
{
  {
    E e[8];
    ZtArray<E> a;
    a = ZtArray<E>(e, 8);
    ZtArray<E> b;

    validate(a, 8);
    a.splice(0, 0, e, 4);
    validate(a, 12);
    a.splice(b, 0, 4);
    validate(a, 8);
    validate(b, 4);
    a.splice(0, 0, b);
    validate(a, 12);
    validate(b, 4);
    b.splice(8, 100, ZtArray<E>{b});
    validate(b, 12);
    for (int i = 0; i < 8; i++) a.push(a.shift());
    validate(a, 12);
  }
#if 0
  {
    ZtArray<ZtString> b = (const char *[]){ "hello", "world" };
    puts(b[0]);
    puts(b[1]);
  }
#endif

  {
    std::vector<const char *> v = { { "hello", "world" } };
    ZtArray<ZuString> b = v;
    puts(b[0].data());
    puts(b[1].data());
  }

  {
    ZtArray<Foo> a;
    a.push(Foo("hello"));
    a.push(Foo("world"));
    puts(a[0].bar);
    puts(a[1].bar);
  }

#if 0
  {
    // typedef const char *P;
    // typedef P N[];
    ZtArray<const char *> a {
      std::initializer_list<const char *>{ [1] = "Foo", [0] = "Bar" } };
    puts(a[0]);
    puts(a[1]);
  }
#endif

  {
    ZtArray<ZtString> a = { "Foo", "Bar" };
    puts(a[0]);
    puts(a[1]);
    for (auto &&s: a) puts(s);
  }

  {
    ZtArray<char> a = "hello world";
    auto ptr = a.data();
    ZtArray<unsigned char> b = ZuMv(a);
    ZtArray<signed char> c = ZuMv(b);
    a = ZuMv(c);
    CHECK(ptr == a.data());
  }

  {
    ZtArray<ZtString> a = { "foo", "bar", "baz", "bah" };
    ZtString out;
    a.all([&out, first = true](const ZtString &s) mutable {
      if (first) first = false; else out << '.';
      out << s;
    });
    CHECK(out == "foo.bar.baz.bah");
  }

  return 0;
}
