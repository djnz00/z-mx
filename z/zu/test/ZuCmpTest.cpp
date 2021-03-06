//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

#define TEST(t) \
  CHECK(ZuCmp<t>::cmp(1, 0) > 0), \
  CHECK(ZuCmp<t>::cmp(0, 1) < 0), \
  CHECK(!ZuCmp<t>::cmp(0, 0)), \
  CHECK(!ZuCmp<t>::cmp(1, 1)), \
  CHECK(ZuCmp<t>::null(ZuCmp<t>::null())), \
  CHECK(!ZuCmp<t>::null((t)1))

template <typename T> struct A : public T {
  inline int id() const { return T::ptr()->p1(); }
  inline int age() const { return T::ptr()->p2(); }
  inline int height() const { return T::ptr()->p3(); }
};

struct S {
  inline S() { m_data[0] = 0; }
  inline S(const S &s) { strcpy(m_data, s.m_data); }
  inline S &operator =(const S &s)
    { if (this != &s) strcpy(m_data, s.m_data); return *this; }
  inline S(const char *s) { strcpy(m_data, s); }
  inline S &operator =(const char *s) { strcpy(m_data, s); return *this; }
  inline operator char *() { return m_data; }
  inline operator const char *() const { return m_data; }
  inline bool operator !() const { return !m_data[0]; }
  inline bool operator ==(const S &s) const
    { return !strcmp(m_data, s.m_data); }
  inline bool operator ==(const char *s) const { return !strcmp(m_data, s); }
  inline bool operator !=(const S &s) const
    { return strcmp(m_data, s.m_data); }
  inline bool operator !=(const char *s) const { return strcmp(m_data, s); }
  inline bool operator >(const S &s) const
    { return strcmp(m_data, s.m_data) > 0; }
  inline bool operator >(const char *s) const { return strcmp(m_data, s) > 0; }
  inline bool operator <(const S &s) const
    { return strcmp(m_data, s.m_data) < 0; }
  inline bool operator <(const char *s) const { return strcmp(m_data, s) < 0; }
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

ZuTupleFields(T1_Fields, 1, id, 2, age, 3, height);
namespace T1 {
  typedef ZuBox<int> I;
  typedef const ZuBox<int> &R;
  typedef T1_Fields<I, I, I> V;
  typedef T1_Fields<R, R, R> T;
}

ZuUnionFields(T2_Fields, 1, id, 2, income, 3, name, 4, dependents, 5, foo);
namespace T2 {
  typedef int I;
  typedef double D;
  typedef const char *S;
  typedef ZuPair<int, int> P;
  typedef int *CP;
  typedef T2_Fields<I, D, S, P, CP> V;
}

ZuTupleFields(T3_Fields, 1, id, 2, age, 3, height);
namespace T3 {
  typedef ZuBox<int> I;
  typedef T3_Fields<I, I, I> V;
  typedef ZuTuple<ZuArrayN<V, 3>, ZuArrayN<int, 3> > T;
}

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
    typedef ZuBox<int> I;
    typedef const ZuBox<int> &R;
    typedef ZuPair<I, I> V;
    typedef ZuPair<R, R> T;

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
    CHECK(ZuCmp<V>::cmp(i, ZuMkPair(p, q)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, ZuMkPair(p, q)) < 0);
    q = 1;
    CHECK(ZuCmp<V>::cmp(i, ZuMkPair(p, q)) > 0);
  }

  {
    ZuTuple<int, int, int, int> s(1, 2, 3, 4);
    ZuTuple<int, int, int, int> t;
    t = s;
    printf("%d %d %d %d\n", (int)s.p1(), (int)s.p2(), (int)s.p3(), (int)s.p4());
    printf("%d %d %d %d\n", (int)t.p1(), (int)t.p2(), (int)t.p3(), (int)t.p4());
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
    CHECK(ZuCmp<V>::cmp(i, ZuMkTuple(p, q, r)) == 0);
    q = 3;
    CHECK(ZuCmp<V>::cmp(i, ZuMkTuple(p, q, r)) < 0);
    q = r = 2;
    CHECK(ZuCmp<V>::cmp(i, ZuMkTuple(p, q, r)) > 0);
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
      i.dependents(ZuMkPair(1, 2));
      j = i;
      CHECK(i == j);
      CHECK(ZuCmp<V>::cmp(i, j) == 0);
      CHECK(i.dependents() == j.dependents());
      j.dependents(ZuMkPair(1, 3));
      CHECK(ZuCmp<V>::cmp(i, j) < 0);
      i.dependents(ZuMkPair(1, 4));
      CHECK(ZuCmp<V>::cmp(i, j) > 0);
      i.foo(&c);
      CHECK(*(i.foo()) == 42);
    }
    CHECK(c == 43);
  }

  {
    ZuTuple<char, char, char, char> t;
    char *p = (char *)&t;
    printf("%d %d %d %d\n",
	   (int)(&t.p1() - p), (int)(&t.p2() - p),
	   (int)(&t.p3() - p), (int)(&t.p4() - p));
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
    ZuTuple<int, const S &, const S &> t3 = ZuMkTuple(42, s3, s3);
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1, t3) < 0));
    CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t1,
	    ZuMkTuple(42, "hello", "world")) > 0));
    // ZuTuple<int, const S &, const S &> t4(42, "string1", "string2");
    // CHECK((ZuCmp<ZuTuple<int, const S &, const S &> >::cmp(t4, t2) < 0));
  }

  {
    using namespace T3;
    T t, s;
    t.p1().length(1);
    t.p1()[0] = ZuMkTuple(1, 2, 3);
    t.p1() += ZuMkTuple(1, 2, 3);
    t.p1() << ZuMkTuple(1, 2, 3);
    t.p2().length(3);
    t.p2()[0] = 42;
    t.p2()[2] = 42;
    s = t;
    CHECK((s.p1()[1] == s.p1()[0]));
    // below deliberately triggers use of uninitialized memory
    printf("%d\n", (int)t.p2()[1]);
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
}
