//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <iostream>

#include <zlib/ZtEnum.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvCf.hh>

void fail() { Zm::exit(1); }

void out_(bool ok, ZuCSpan check, ZuCSpan diag) {
  std::cout
    << (ok ? "OK  " : "NOK ") << check << ' ' << diag
    << '\n' << std::flush;
}

#define CHECK_(x) out_((x), #x, "")
#define CHECK(x, y) out_((x), #x, y)

static const char *testdata =
"#\n"
"  #\n"
"     key4 # kick kick\n"
"\n"
"\n"
"     \\#\\ value4\n"
"key2 ok\\ \n"
"key3 ok2\\\\\n"
"\n"
"# \\grok this word\n"
"\n"
"	key1		\n"
"			\"ok \\\"this is val1\\\\\"		# comment !!\n"
"  0 \"\" 1 Arg1\n"
"key6 { a b c d\\} }\n"
"\n"
"key5 [\\#\\ k51, \"k5\\\\2\", k\\ 53\\,,\n"
"k54\\ , k55 ]\n"
"\n"
"%define FAT artma\n"
"key7 { foo { bah 1 } } key8 C${FAT}n\n";

namespace Values {
  ZtEnumValues(Values, int8_t, High, Low, Normal);
}

int main()
{
  ZeLog::init("CfTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
  ZeLog::start();

  try {
    {
      ZiFile file;
      ZeError e;
      if (file.open(
	    "in.cf", ZiFile::Create | ZiFile::Truncate, 0777, &e) != Zi::OK)
	throw e;
      if (file.write(
	    static_cast<const void *>(testdata),
	    strlen(testdata), &e) != Zi::OK)
	throw e;
    }
    {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromFile("in.cf");
      cf->toFile("out.cf");
    }
    ZtString out;
    {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromFile("out.cf");
      out << *cf;
      bool caught = false;
      try {
	cf->fromFile("out_.cf");
      } catch (const ZvError &) {
	caught = true;
      } catch (const ZeError &) {
	caught = true;
      }
      CHECK(caught, "nonexistent file detected");
    }
    {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromString(out);
      cf->fromFile("in.cf");
      cf->toFile("out2.cf");
    }
    {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromFile("out2.cf");
      ZtString out2;
      out2 << *cf;
      CHECK(out == out2, "out.cf identical to out2.cf");
    }
    {
      int argc;
      char **argv;
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromFile("in.cf");
      cf->toArgs(argc, argv);
      for (int i = 0; i < argc; i++)
	printf("%d: %s\n", i, argv[i]);
      ZvCf::freeArgs(argc, argv);
    }
    {
      static const char *argv[] = {
	"",
	"--key1=ok \"this is val1\\\\\\",
	"-A", "ok ",
	"-B", "ok2\\\\\\",
	"--key5=# k51,k5\\\\\\2,k\\ 53\\,,k54\\ ,k55",
	"-C", "b",
	"--key6-c=d}",
	"-D",
	"Arg1",
	"--key8=Cartman",
	0
      };
      static ZvOpt opts[] = {
	{ 0,   "key1", ZvOptType::Param, "key1" },
	{ 'A', "key2", ZvOptType::Param, "key2" },
	{ 'B', "key3", ZvOptType::Param, "key3" },
	{ 0,   "key4", ZvOptType::Param, "key4" },
	{ 0,   "key5", ZvOptType::Array, "key5" },
	{ 'C', "key6-a", ZvOptType::Param, "key6.a" },
	{ 0,   "key6-c", ZvOptType::Param, "key6.c" },
	{ 'D', "key7-foo-bah", ZvOptType::Flag, "key7.foo.bah" },
	{ 0,   "key8", ZvOptType::Param, "key8" },
	{ 0 }
      };

      {
	ZmRef<ZvCf> cf = new ZvCf();

	cf->set("key4", "# value4"); // default
	cf->fromArgs(ZvCf::options(opts),
	  ZvCf::args(sizeof(argv) / sizeof(argv[0]) - 1, (char **)argv));
	cf->unset("#");
	cf->toFile("out3.cf");
	ZtString out3;
	out3 << *cf;
	CHECK(out == out3, "out.cf identical to out3.cf");
      }
      {
	ZmRef<ZvCf> syntax = new ZvCf();

	syntax->setCf("", ZvCf::options(opts));

	ZmRef<ZvCf> cf = new ZvCf();

	cf->fromCLI(syntax,
	  " "
	  "--key1='ok \"this is val1\\\\\' "
	  "-A \"ok \" "
	  "-B ok2\\\\ "
	  "--key5=\"# k51,k5\\\\\\2,k 53\\,,k54 ,k55\" "
	  "-C b "
	  "--key6-c=d} "
	  "-D "
	  "--key8=Cartman "
	  "Arg1");
	cf->set("key4", "# value4"); // default
	cf->unset("#");
	cf->toFile("out4.cf");
	ZtString out4;
	out4 << *cf;
	CHECK(out == out4, "out.cf identical to out4.cf");
      }
    }
    {
      static const char *env = "CFTEST="
	"0:;"
	"1:Arg1;"
	"key1:\"ok \\\"this is val1\\\\\";"
	"key2:ok\\ ;"
	"key3:ok2\\\\;"
	"key4:\"# value4\";"
	"key5:[\"# k51\",k5\\\\2,\"k 53,\",k54\\ ,k55];"
	"key6:{a:b;c:d\\}};"
	"key7:{foo:{bah:1}};"
	"key8:Cartman";

#ifdef _WIN32
      _putenv(env);
#else
      putenv((char *)env);
#endif

      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromEnv("CFTEST");
      cf->toFile("out5.cf");
      ZtString out5;
      out5 << *cf;
      if (out != out5) {
	std::cout << "NOK out.cf and out5.cf differ\n";
	fail();
      }
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromString("i 101");
      if (cf->getInt("j", 1, 100, 42) != 42) {
	std::cout << "NOK getInt() default failed\n";
	fail();
      }
      try {
	cf->getInt<true>("j", 1, 100);
	std::cout << "NOK getInt() required failed\n";
	fail();
      } catch (const ZvError &e) {
	std::cout << "OK: " << e << '\n';
      }
      cf->getInt("i", 1, 100, 42);
      std::cout << "NOK getInt() range failed\n";
      fail();
    } catch (const ZvError &e) {
      std::cout << "OK: " << e << '\n';
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromString("i 100.01");
      if (cf->getDouble("j", .1, 100, .42) != .42) {
	std::cout << "NOK getDbl() default failed\n";
	fail();
      }
      try {
	cf->getDouble<true>("j", .1, 100);
	std::cout << "NOK getDbl() required failed\n";
	fail();
      } catch (const ZvError &e) {
	std::cout << "OK: " << e << '\n';
      }
      cf->getDouble("i", .1, 100, .42);
      std::cout << "NOK getDbl() range failed\n";
      fail();
    } catch (const ZvError &e) {
      std::cout << "OK  " << e << '\n';
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();
      cf->fromString("i FooHigh");
      if (cf->getEnum<Values::Map>("j") >= 0) {
	std::cout << "NOK getEnum() default failed\n";
	fail();
      }
      cf->getEnum<Values::Map, true>("i");
      std::cout << "NOK getEnum() invalid failed\n";
      fail();
    } catch (const ZvError &e) {
      std::cout << "OK  " << e << '\n';
    }

    {
      ZmRef<ZvCf> cf1 = new ZvCf{}, cf2 = new ZvCf{},
		  cf3 = new ZvCf{}, cf4 = new ZvCf{};

      cf1->fromString("i foo l { m baz }");
      cf2->fromString("j { k bar } n bah");
      cf3->merge(cf1);
      cf3->merge(cf2);
      cf4->merge(cf2);
      cf4->merge(cf1);
      cf3->toFile("out6.cf");
      cf4->toFile("out7.cf");
      ZtString out3, out4;
      out3 << *cf3;
      out4 << *cf4;
      CHECK(out3 == out4, "out6.cf is identical to out7.cf");
    }

    {
      ZmRef<ZvCf> cf = new ZvCf();
      cf->fromString("\\=A value");
      CHECK_(cf->get("=A") == "value");
    }

    {
      ZmRef<ZvCf> cf = new ZvCf();
      cf->fromString("x { y z }");
      CHECK_(cf->get("x.y") == "z");
    }

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (...) {
    std::cerr << "unknown exception\n" << std::flush;
    Zm::exit(1);
  }

  ZeLog::stop();
  return 0;
}
