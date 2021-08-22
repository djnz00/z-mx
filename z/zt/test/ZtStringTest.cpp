//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <stdio.h>

#include <zlib/ZuLib.hpp>

#include <zlib/ZuBox.hpp>
#include <zlib/ZuStringN.hpp>

#include <zlib/ZmList.hpp>

#include <zlib/ZtString.hpp>

#include <string>
#include <sstream>

void foo(const ZtString &s, ZtString t)
{
  puts(s);
  puts(t);
}

void bar(bool b, ZtString &s)
{
  ZtString baz;
  if (b)
    baz = s;
  else
    baz = "bah";
  puts(baz);
}

int main()
{
  ZtString s1, s2, s3, s4;

  s1 = (char *)0;
  s2 = "hello";
  s3 = "world";
  s4 = s3;

  puts(s2 + s1);
  puts(s3 + s1);
  puts(s2 + " " + s3);
  s2 += ZtString(" ") + s3;
  puts(s2);
  puts(s4);

  if (!(s3 == s4)) puts("s3 == s4 FAILED");
  if (!(s3 == s3)) puts("s3 == s3 FAILED");
  if (!(s2 == s2)) puts("s2 == s2 FAILED");
  if (!(s1 == s1)) puts("s1 == s1 FAILED");
  if (!(s2 != s3)) puts("s2 != s3 FAILED");
  if (!(s1 != s3)) puts("s1 != s3 FAILED");
  if (!!s1) puts("!s1 FAILED");

  if (!(s3 > s2)) puts("s3 > s2 FAILED");
  if (!(s3 > s1)) puts("s3 > s1 FAILED");
  if (!(s3 >= s4)) puts("s3 >= s4 FAILED");
  if (!(s3 >= s3)) puts("s3 >= s3 FAILED");
  if (!(s3 >= s2)) puts("s3 >= s2 FAILED");
  if (!(s3 >= s1)) puts("s3 >= s1 FAILED");
  if (!(s2 < s3)) puts("s2 < s3 FAILED");
  if (!(s1 < s3)) puts("s1 < s3 FAILED");
  if (!(s3 <= s4)) puts("s3 <= s4 FAILED");
  if (!(s3 <= s3)) puts("s3 <= s3 FAILED");
  if (!(s2 <= s3)) puts("s2 <= s3 FAILED");
  if (!(s1 <= s3)) puts("s1 <= s3 FAILED");

  s2.splice(0, 5, "'bye ");

  puts(s2);

  s2.splice(16, 3, "!!!");

  puts(s2);

  s1.splice(2, 17, "hello world again");

  puts(s1);

  s1.splice(0, 0, ZtString(0));

  puts(s1);

  s1.splice(14, 15, "again and again");

  puts(s1);

  s1 = "this string";
  puts(s1);
  s1.splice(0, 0, "beginning of ");
  puts(s1);
  s1.splice(0, 0, "inserted at ");
  puts(s1);

  s1 = "the string";
  puts(s1);
  s1.splice(4, 0, "middle of this ");
  puts(s1);
  s1.splice(s2, 0, 4, "inserted at the ");
  puts(s2);
  puts(s1);

  {
    ZtString s;

    s.sprintf("%s %.1d %.2d %.3d %s", "hello", 1, 2, 3, "world");
    puts(s);
  }
  {
    ZtString s =
      ZtString().sprintf("%s %.1d %.2d %.3d %s", "goodbye", 1, 2, 3, "world");

    puts(s);
  }
  {
    ZtWString w;

    w.sprintf(L"%ls %.1d %.2d %.3d %ls", L"hello", 1, 2, 3, L"world");

    ZtString s = w;

    puts(s);
  }
  {
    ZtWString w =
      ZtWString().sprintf(L"%ls %.1d %.2d %.3d %ls",
			   L"goodbye", 1, 2, 3, L"world");
    ZtString s = ZtString(w);

    puts(s);
  }

  {
    ZtString s1, s2, s3;
    ZtWString w1, w2, w3;

    s1 += "Hello";
    w1 += ZtWString(s1);
    w1 += L" ";
    s1 += ZtString(w2 = L" ");
    s3 = "World";
    w3 = L"World";
    s1 += s3;
    w1 += w3;
    if (s1 != ZtString(w1)) {
      s2 = w1;
      w2 = s1;
      printf("1.1 \"%S\" != \"%S\"\n", w2.data(), w1.data());
      printf("1.2 \"%s\" != \"%s\"\n", s1.data(), s2.data());
    }
    if (w1 != ZtWString(s1)) {
      s2 = w1;
      w2 = s1;
      printf("2.1 \"%S\" != \"%S\"\n", w1.data(), w2.data());
      printf("2.2 \"%s\" != \"%s\"\n", s2.data(), s1.data());
    }
  }

  {
    ZtString s;

    s += "Foo";

    bar(true, s);
    bar(false, s);
  }

  foo(ZtSprintf("%d = %s %s", 42, "Hello", "World"),
      ZtSprintf("%d = %s %s", 43, "Goodbye", "World"));

  {
    ZtString s(256);

    s = (int)42;
    s += ' ';
    s += (unsigned int)42;
    s += ' ';
    s += (int16_t)42;
    s += ' ';
    s += (uint16_t)42;
    s += ' ';
    s += (int32_t)42;
    s += ' ';
    s += (uint32_t)42;
    s += ' ';
    s += (int64_t)42;
    s += ' ';
    s += (uint64_t)42;
    s += ' ';
    s += (float)42;
    s += ' ';
    s += (double)42;
    s += ' ';
    s += (long double)42;
    s += ' ';
    s += "Hello";
    s += ' ';
    s += "World!";
    s += ' ';
    s += "(11 x 42)";

    puts(s);
  }

  {
    using Queue = ZmList<ZtString>;

    Queue q;

    ZtString msg = "Hello World";
    q.push(msg);
    ZtString res = q.shift();
    puts(res.data());
  }

  {
    ZtString s = "Hello World \r\n";
    s.chomp();
    if (s != "Hello World") puts("chomp() 1 FAILED");
    s.null(); s.chomp();
    if (!!s) puts("chomp() 2 FAILED");
    s = "\r\n-\r\n\r\n\r\n"; s.chomp();
    if (s != "\r\n-") puts("chomp() 3 FAILED");
    s = " \t \t \r\n\r\n Hello World";
    s.strip();
    if (s != "Hello World") puts("strip() 1 FAILED");
    s = " \t \t \r\n\r\n Hello World \r\n";
    s.strip();
    if (s != "Hello World") puts("strip() 2 FAILED");
    s.null(); s.strip();
    if (!!s) puts("strip() 3 FAILED");
    s = " \t \t \r\n \r\n\r\n\r\n \t \t \r\n \r\n\r\n\r\n"; s.strip();
    if (!!s) puts("strip() 4 FAILED");
  }

  {
    char buf[12];
    ZtString s(buf, 0, 12, false);
    s += "Hello World";
    puts(s);
    if (s.mallocd()) puts("buffer 1 FAILED");
    if (s.data() != buf) puts("buffer 2 FAILED");
    s.splice(0, 5, "'Bye");
    puts(s);
    if (s.mallocd()) puts("buffer 2 FAILED");
    if (s.data() != buf) puts("buffer 3 FAILED");
    s += " - and what a nice day";
    puts(s);
    if (!s.mallocd()) puts("buffer 4 FAILED");
    if (s.data() == buf) puts("buffer 5 FAILED");
  }

  {
    ZuStringN<16> s;
    s = "Hello World";
    s += ZuBox<int>(123456789);
    if (s != "Hello World") puts("ZuStringN append 1 FAILED");
    s += ZuBox<int>(12345);
    if (s != "Hello World") puts("ZuStringN append 2 FAILED");
    s << ZuStringN<12>(ZuBox<int>(1234));
    puts(s);
    if (s != "Hello World1234") puts("ZuStringN append 3 FAILED");
    s = "";
    s << "Hello ";
    s << "World";
    if (s != "Hello World") puts("ZuStringN append 4 FAILED");
  }

  {
    if (ZuStringN<2> s = "x") puts("ZuStringN as boolean true ok");
    else puts("ZuStringN as boolean true FAILED");
    if (ZuStringN<2> s = "") puts("ZuStringN as boolean false FAILED");
    else puts("ZuStringN as boolean false ok");
  }
  {
    std::string s;
    s += ZuStringN<4>("foo");
    if (s == "foo") puts("ZuStringN appending to std::string ok");
    else puts("ZuStringN appending to std::string FAILED");
    s += ZtString(" bar");
    if (s == "foo bar") puts("ZtString appending to std::string ok");
    else puts("ZtString appending to std::string FAILED");
  }
  {
    std::stringstream s;
    s << ZuStringN<4>("foo");
    char buf[64];
    buf[s.rdbuf()->sgetn(buf, 63)] = 0;
    if (!strcmp(buf, "foo")) puts("ZuStringN writing to std::ostream ok");
    else puts("ZuStringN writing to std::ostream FAILED");
    s << ZuStringN<4>("foo") << ' ' << ZtString("bar");
    buf[s.rdbuf()->sgetn(buf, 63)] = 0;
    if (!strcmp(buf, "foo bar")) puts("ZtString writing to std::ostream ok");
    else puts("ZtString writing to std::ostream FAILED");
  }

  puts(ZtString() << "hello " << "world");

  {
    ZtString j = ZtJoin(",", { "x", "y" });
    if (j != "x,y") puts("ZtJoin FAILED");
  }

  {
    std::cout << ZtHexDump("Whoot!",
	"This\x1cis\x09""a\x05test\x01of\x04the\x1ehexadecimal\x13""dumper!",
	42);

  }

  {
    ZtString s("inline const char *");
    // s[0] = 'X';
  }
}
