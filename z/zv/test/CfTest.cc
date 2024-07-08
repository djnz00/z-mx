//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>

#include <zlib/ZtEnum.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvCf.hh>

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
"# \\grok this sucker\n"
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
      if (!caught) ZeLOG(Error, "Nonexistent file not detected");
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
      if (out != out2) ZeLOG(Error, "out.cf and out2.cf differ");
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
	"-AB", "ok ", "ok2\\\\\\",
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
	if (out != out3) ZeLOG(Error, "out.cf and out3.cf differ");
      }
      {
	ZmRef<ZvCf> syntax = new ZvCf();

	syntax->setCf("", ZvCf::options(opts));

	ZmRef<ZvCf> cf = new ZvCf();

	cf->fromCLI(syntax,
	  " "
	  "--key1='ok \"this is val1\\\\\' "
	  "-AB \"ok \" ok2\\\\ "
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
	if (out != out4) ZeLOG(Error, "out.cf and out4.cf differ");
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
      if (out != out5) ZeLOG(Error, "out.cf and out5.cf differ");
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromString("i 101");
      if (cf->getInt("j", 1, 100, 42) != 42)
	ZeLOG(Error, "getInt() default failed");
      try {
	cf->getInt<true>("j", 1, 100);
	ZeLOG(Error, "getInt() required failed");
      } catch (const ZvError &e) {
	std::cout << "OK: " << e << '\n';
      }
      cf->getInt("i", 1, 100, 42);
      ZeLOG(Error, "getInt() range failed");
    } catch (const ZvError &e) {
      std::cout << "OK: " << e << '\n';
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromString("i 100.01");
      if (cf->getDouble("j", .1, 100, .42) != .42)
	ZeLOG(Error, "getDbl() default failed");
      try {
	cf->getDouble<true>("j", .1, 100);
	ZeLOG(Error, "getDbl() required failed");
      } catch (const ZvError &e) {
	std::cout << "OK: " << e << '\n';
      }
      cf->getDouble("i", .1, 100, .42);
      ZeLOG(Error, "getDbl() range failed");
    } catch (const ZvError &e) {
      std::cout << "OK: " << e << '\n';
    }

    try {
      ZmRef<ZvCf> cf = new ZvCf();
      cf->fromString("i FooHigh");
      if (cf->getEnum<Values::Map>("j") >= 0)
	ZeLOG(Error, "getEnum() default failed");
      cf->getEnum<Values::Map>("i", 0);
      ZeLOG(Error, "getEnum() invalid failed");
    } catch (const ZvError &e) {
      std::cout << "OK: " << e << '\n';
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
      if (out3 != out4)
	ZeLOG(Error, "merge() results inconsistent - out6.cf, out7.cf");
    }

    {
      ZmRef<ZvCf> cf = new ZvCf();
      cf->fromString("\\=A value");
      std::cout << "OK: \"\\=A\"=" << cf->get("=A", 1) << '\n';
    }

  } catch (const ZvError &e) {
    std::cerr << e << '\n';
    Zm::exit(1);
  } catch (const ZeError &e) {
    fputs(e.message(), stderr);
    fputc('\n', stderr);
    Zm::exit(1);
  } catch (...) {
    fputs("unknown exception\n", stderr);
    Zm::exit(1);
  }

  ZeLog::stop();
  return 0;
}
