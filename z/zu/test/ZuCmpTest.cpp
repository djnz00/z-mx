//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdlib.h>
#include <time.h>
#include <stdio.h>

#include <zlib/ZuHash.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuPair.hpp>
#include <zlib/ZuTuple.hpp>
#include <zlib/ZuUnion.hpp>
#include <zlib/ZuArrayN.hpp>
#include <zlib/ZuStringN.hpp>
#include <zlib/ZuSort.hpp>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

#define TEST(t) \
  CHECK(ZuCmp<t>::cmp(1, 0) > 0), \
  CHECK(ZuCmp<t>::cmp(0, 1) < 0), \
  CHECK(!ZuCmp<t>::cmp(0, 0)), \
  CHECK(!ZuCmp<t>::cmp(1, 1)), \
  CHECK(ZuCmp<t>::null(ZuCmp<t>::null())), \
  CHECK(!ZuCmp<t>::null((t)1))

template <typename T> struct A : public T {
  int id() const { return T::ptr()->p1(); }
  int age() const { return T::ptr()->p2(); }
  int height() const { return T::ptr()->p3(); }
};

struct S {
  S() { m_data[0] = 0; }
  S(const S &s) { strcpy(m_data, s.m_data); }
  S &operator =(const S &s)
    { if (this != &s) strcpy(m_data, s.m_data); return *this; }
  S(const char *s) { strcpy(m_data, s); }
  S &operator =(const char *s) { strcpy(m_data, s); return *this; }
  operator char *() { return m_data; }
  operator const char *() const { return m_data; }
  bool operator !() const { return !m_data[0]; }
  bool operator ==(const S &s) const
    { return !strcmp(m_data, s.m_data); }
  bool operator ==(const char *s) const { return !strcmp(m_data, s); }
  bool operator !=(const S &s) const
    { return strcmp(m_data, s.m_data); }
  bool operator !=(const char *s) const { return strcmp(m_data, s); }
  bool operator >(const S &s) const
    { return strcmp(m_data, s.m_data) > 0; }
  bool operator >(const char *s) const { return strcmp(m_data, s) > 0; }
  bool operator <(const S &s) const
    { return strcmp(m_data, s.m_data) < 0; }
  bool operator <(const char *s) const { return strcmp(m_data, s) < 0; }
  char m_data[32];
};

template <typename T1, typename T2> void checkNull() {
  ZuBox<T1> t;
  ZuBox<T2> u = t;
  ZuBox<T2> v(t);
  ZuBox<T2> w;
  w = t;
  CHECK(!*u && !*v && !*w);
}

namespace T1 {
  using I = ZuBox<int>;
  using R = const ZuBox<int> &;
  struct _ {
    ZuDeclTuple(V, (I, id), (I, age), (I, height));
    ZuDeclTuple(T, (R, id), (R, age), (R, height));
  };
  using V = _::V;
  using T = _::T;
}

ZuAssert(ZuHash_Can<T1::V>::OK);
ZuAssert(ZuHash_Can<T1::T>::OK);

namespace T2 {
  using I = int;
  using D = double;
  using S = const char *;
  using P = ZuPair<int, int>;
  using CP = int *;
  ZuDeclUnion(V, (I, id), (D, income), (S, name), (P, dependents), (CP, foo));
}

namespace T3 {
  using I = ZuBox<int>;
  ZuDeclTuple(V, (I, id), (I, age), (I, height));
  using T = ZuTuple<ZuArrayN<V, 3>, ZuArrayN<int, 3> >;
}

template <unsigned N> struct SortTest {
  template <typename A, typename S> static void test_(A &a, S &s) {
    ZuSort<N>(&a[0], a.length());
    for (unsigned i = 0, n = a.length(); i < n; i++)
      s << (i ? " " : "") << a[i];
  }
  static void test() {
    {
      ZuArrayN<int, 1> foo{};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "");
      CHECK(ZuSearch(&foo[0], 0, 0) == 0);
      CHECK(ZuInterSearch(&foo[0], 0, 0) == 0);
    }
    {
      ZuArrayN<int, 1> foo{1};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "1");
      CHECK(ZuSearch(&foo[0], 1, 0) == 0);
      CHECK(ZuInterSearch(&foo[0], 1, 0) == 0);
      CHECK(ZuSearch(&foo[0], 1, 1) == 1);
      CHECK(ZuInterSearch(&foo[0], 1, 1) == 1);
      CHECK(ZuSearch<false>(&foo[0], 1, 1) == 0);
      CHECK(ZuInterSearch<false>(&foo[0], 1, 1) == 0);
    }
    {
      ZuArrayN<int, 2> foo{0, 1};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "0 1");
      CHECK(ZuSearch(&foo[0], 2, 0) == 1);
      CHECK(ZuInterSearch(&foo[0], 2, 0) == 1);
      CHECK(ZuSearch(&foo[0], 2, 1) == 3);
      CHECK(ZuInterSearch(&foo[0], 2, 1) == 3);
      CHECK(ZuSearch<false>(&foo[0], 2, 0) == 0);
      CHECK(ZuInterSearch<false>(&foo[0], 2, 0) == 0);
      CHECK(ZuSearch<false>(&foo[0], 2, 1) == 2);
      CHECK(ZuInterSearch<false>(&foo[0], 2, 1) == 2);
    }
    {
      ZuArrayN<int, 2> foo{1, 0};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "0 1");
    }
    {
      ZuArrayN<int, 3> foo{3, 1, 2};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "1 2 3");
      CHECK(ZuSearch(&foo[0], 3, 0) == 0);
      CHECK(ZuInterSearch(&foo[0], 3, 0) == 0);
      CHECK(ZuSearch(&foo[0], 3, 1) == 1);
      CHECK(ZuInterSearch(&foo[0], 3, 1) == 1);
      CHECK(ZuSearch(&foo[0], 3, 2) == 3);
      CHECK(ZuInterSearch(&foo[0], 3, 2) == 3);
      CHECK(ZuSearch<false>(&foo[0], 3, 2) == 2);
      CHECK(ZuInterSearch<false>(&foo[0], 3, 2) == 2);
      CHECK(ZuSearch<false>(&foo[0], 3, 3) == 4);
      CHECK(ZuInterSearch<false>(&foo[0], 3, 3) == 4);
    }
    {
      ZuArrayN<int, 4> foo{4, 1, 3, 0};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "0 1 3 4");
      CHECK(ZuSearch(&foo[0], 4, 0) == 1);
      CHECK(ZuInterSearch(&foo[0], 4, 0) == 1);
      CHECK(ZuSearch(&foo[0], 4, 1) == 3);
      CHECK(ZuInterSearch(&foo[0], 4, 1) == 3);
      CHECK(ZuSearch(&foo[0], 4, 2) == 4);
      CHECK(ZuInterSearch(&foo[0], 4, 2) == 4);
      CHECK(ZuSearch<false>(&foo[0], 4, 3) == 4);
      CHECK(ZuInterSearch<false>(&foo[0], 4, 3) == 4);
      CHECK(ZuSearch<false>(&foo[0], 4, 4) == 6);
      CHECK(ZuInterSearch<false>(&foo[0], 4, 4) == 6);
    }
    {
      ZuArrayN<int, 13> foo{3, 1, 2, 9, 5, 3, 5, 1, 10, 4, 0, 7, 6};
      ZuStringN<80> s;
      test_(foo, s);
      CHECK(s == "0 1 1 2 3 3 4 5 5 6 7 9 10");
      CHECK(ZuSearch(&foo[0], 13, 0) == 1);
      CHECK(ZuInterSearch(&foo[0], 13, 0) == 1);
      CHECK(ZuSearch(&foo[0], 13, 2) == 7);
      CHECK(ZuInterSearch(&foo[0], 13, 2) == 7);
      CHECK(ZuSearch<false>(&foo[0], 13, 5) == 14);
      CHECK(ZuInterSearch<false>(&foo[0], 13, 5) == 14);
      CHECK(ZuSearch<false>(&foo[0], 13, 10) == 24);
      CHECK(ZuInterSearch<false>(&foo[0], 13, 10) == 24);
    }
  }
};

template <unsigned k_> struct K { enum { k = k_ }; };

struct M {
  M() = default;
  M(const M &) = delete;
  M &operator =(const M &) = delete;
  M(M &&) = default;
  M &operator =(M &&) = default;
  ~M() = default;
  int cmp(const M &) const { return 0; }
  bool equals(const M &) const { return true; }
  bool operator ==(const M &) const { return true; }
  bool operator !() const { return true; }
};

int main()
{
  {
    struct X { };
    CHECK(ZuTraits<X>::IsComposite);
    CHECK(!ZuTraits<int>::IsComposite);
    enum { Foo = 42 };
    enum _ { Bar = 42 };
    CHECK(ZuTraits<decltype(Foo)>::IsEnum);
    CHECK(ZuTraits<_>::IsEnum);
    CHECK(!ZuTraits<int>::IsEnum);
    CHECK(!ZuTraits<X>::IsEnum);
  }

  TEST(bool);
  CHECK(ZuCmp<char>::cmp(1, 0) > 0),
  CHECK(ZuCmp<char>::cmp(0, 1) < 0),
  CHECK(!ZuCmp<char>::cmp(0, 0)),
  CHECK(!ZuCmp<char>::cmp(1, 1)),
  CHECK(ZuCmp<char>::null(ZuCmp<char>::null())),
  CHECK(!ZuCmp<char>::null((char)0x80)),
  CHECK(!ZuCmp<char>::null((char)1));
  TEST(signed char);
  TEST(unsigned char);
  TEST(short);
  TEST(unsigned short);
  TEST(int);
  TEST(unsigned int);
  TEST(long);
  TEST(unsigned long);
  TEST(long long);
  TEST(unsigned long long);
  TEST(float);
  TEST(double);

  {
    using I = ZuBox<int>;
    using R = const ZuBox<int> &;
    using V = ZuPair<I, I>;
    using T = ZuPair<R, R>;

    V j(1, 2);
    V i = j;
    ZuBox<int> p = 1;
    ZuBox<int> q = 2;
    CHECK(ZuCmp<V>::cmp(i, T(p, q)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, T(p, q)) < 0);
    q = 1;
    CHECK(ZuCmp<V>::cmp(i, T(p, q)) > 0);
    p = 1, q = 2;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdPair(p, q)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdPair(p, q)) < 0);
    q = 1;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdPair(p, q)) > 0);
  }

  {
    ZuTuple<int, int, int, int> s(1, 2, 3, 4);
    ZuTuple<int, int, int, int> t;
    t = s;
    printf("%d %d %d %d\n", (int)s.p<0>(), (int)s.p<1>(), (int)s.p<2>(), (int)s.p<3>());
    printf("%d %d %d %d\n", (int)t.p<0>(), (int)t.p<1>(), (int)t.p<2>(), (int)t.p<3>());
  }

  {
    using namespace T1;
    V j(1, 2, 3);
    V i;
    i = j;
    CHECK(i.id() == 1);
    CHECK(i.age() == 2);
    CHECK(i.height() == 3);
    ZuBox<int> p = 1;
    ZuBox<int> q = 2;
    ZuBox<int> r = 3;
    CHECK(ZuCmp<V>::cmp(i, T(p, q, r)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, T(p, q, r)) < 0);
    q = r = 2;
    CHECK(ZuCmp<V>::cmp(i, T(p, q, r)) > 0);
    q = 2, r = 3;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdTuple(p, q, r)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdTuple(p, q, r)) < 0);
    q = r = 2;
    CHECK(ZuCmp<V>::cmp(i, ZuFwdTuple(p, q, r)) > 0);
  }

  {
    using namespace T2;
    int c = 42;

    {
      V j;
      j.name("3");
      V i;
      i = j;
      CHECK(i.name() == j.name());
      CHECK(i == j);
      CHECK(ZuCmp<V>::cmp(i, j) == 0);
      j.name("4");
      CHECK(ZuCmp<V>::cmp(i, j) < 0);
      i.income(200.0);
      CHECK(ZuCmp<V>::cmp(i, j) < 0);
      j.id(42);
      CHECK(ZuCmp<V>::cmp(i, j) > 0);
      i.dependents(ZuFwdPair(1, 2));
      j = i;
      CHECK(i == j);
      CHECK(ZuCmp<V>::cmp(i, j) == 0);
      CHECK(i.dependents() == j.dependents());
      j.dependents(ZuFwdPair(1, 3));
      CHECK(ZuCmp<V>::cmp(i, j) < 0);
      i.dependents(ZuFwdPair(1, 4));
      CHECK(ZuCmp<V>::cmp(i, j) > 0);
      i.foo(&c);
      CHECK(*(i.foo()) == 42);
      ++*(i.foo());
    }
    CHECK(c == 43);
  }

  {
    ZuTuple<char, char, char, char> t;
    char *p = (char *)&t;
    printf("%d %d %d %d\n",
	   (int)(&t.p<0>() - p), (int)(&t.p<1>() - p),
	   (int)(&t.p<2>() - p), (int)(&t.p<3>() - p));
  }

  {
    S s1("string1");
    S s2("string2");
    S s3("string3");
    ZuTuple<int, const S &, const S &> t1(42, s1, s2);
    ZuTuple<int, const S &, const S &> t2(42, s1, s3);
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1, t2) < 0));
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1, t1) == 0));
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t2, t1) > 0));
    ZuTuple<int, const S &, const S &> t3 = ZuFwdTuple(42, s3, s3);
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1, t3) < 0));
    S s4{"hello"};
    S s5{"world"};
    std::cout << "t1=" << t1.print() << '\n' << std::flush;
    std::cout << "t2=" << ZuFwdTuple(42, s4, s5).print() << '\n' << std::flush;
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1,
	    ZuFwdTuple(42, s4, s5)) > 0));
    // ZuTuple<int, const S &, const S &> t4(42, "string1", "string2");
    // CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t4, t2) < 0));
  }

  {
    using namespace T3;
    T t, s;
    t.p<0>().length(1);
    t.p<0>()[0] = ZuFwdTuple(1, 2, 3);
    t.p<0>() += ZuFwdTuple(1, 2, 3);
    t.p<0>() << ZuFwdTuple(1, 2, 3);
    t.p<1>().length(3);
    t.p<1>()[0] = 42;
    t.p<1>()[2] = 42;
    s = t;
    CHECK((s.p<0>()[1] == s.p<0>()[0]));
    // below deliberately triggers use of uninitialized memory
    printf("%d\n", (int)t.p<1>()[1]);
  }

  {
    ZuStringN<10> s = "hello world";
    CHECK(s == "hello wor");
    s = ZuArrayN<char, 10>("hello world");
    CHECK(s == "hello wor");
    s = 'h';
    CHECK(s == "h");
    s << ZuArrayN<char, 2>("el");
    s += "lo ";
    s << ZuStringN<6>("world");
    CHECK(s == "hello wor");
  }

  {
    checkNull<int16_t, uint32_t>();
    checkNull<uint32_t, int16_t>();
    checkNull<int16_t, int32_t>();
    checkNull<int32_t, int16_t>();
    checkNull<int16_t, uint64_t>();
    checkNull<int64_t, uint16_t>();
    checkNull<double, uint16_t>();
    checkNull<int32_t, double>();
  }

  {
    SortTest<0>::test();
    SortTest<1>::test();
    SortTest<2>::test();
    SortTest<8>::test();
    SortTest<20>::test();
  }

  {
    using U = ZuUnion<int, float, double>;
    char c_[sizeof(U)];
    *reinterpret_cast<double *>(c_) = 42.0;
    auto c = reinterpret_cast<U *>(c_);
    c->type_(U::Index<double>::I);
    CHECK(c->v<double>() == 42.0);
    auto d = get<double>(*c);
    CHECK(d == 42.0);
    *c = 42.0;
    d = get<double>(*c);
    CHECK(d == 42.0);
    c->~U();
    new (c) U{42.0};
    d = get<double>(*c);
    CHECK(d == 42.0);
  }

  // structured binding smoke tests
  {
    ZuArrayN<int, 3> foo = { 1, 2, 3 };
    auto [a, b, c] = foo;
    CHECK(a == 1 && b == 2 && c == 3);
  }

  {
    ZuPair<uint64_t, uint32_t> foo = { 1U, 2U };
    auto [a, b] = foo;
    CHECK(a == 1 && b == 2);
  }

  {
    ZuTuple<uint64_t, uint32_t, uint16_t> foo =
      { 1U, 2U, static_cast<uint16_t>(3U) };
    auto [a, b, c] = foo;
    CHECK(a == 1 && b == 2 && c == 3);
  }

  {
    ZuConstant<1> i;
    ZuConstant<i> j;
    CHECK(K<j>::k == 1);
  }

  {
    struct A {
      A() : i{0} { }
      A(int i_) : i{i_} { }
      A(const A &) = default;
      A &operator =(const A &) = default;
      A(A &&) = default;
      A &operator =(A &&) = default;
      ~A() = default;
      bool operator !() const { return !i; }
      bool operator <(const A &a) const { return i < a.i; }
      int cmp(const A &a) const { return i - a.i; }
      int i;
    };
    struct B : public A {
      using A::A;
      using A::operator =;
      using A::cmp;
    };
    B a, b{1}, c{42};
    CHECK(!a);
    CHECK(!ZuCmp<A>::cmp(a, a));
    CHECK(!ZuCmp<A>::cmp(c, c));
    CHECK(a < b);
    CHECK(ZuCmp<A>::cmp(a, b) < 0);
    CHECK(ZuCmp<A>::cmp(c, b) > 0);
  }

  {
    CHECK(M{} == ZuCmp<M>::null());
  }
}
