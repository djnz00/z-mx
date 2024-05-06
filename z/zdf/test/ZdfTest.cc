//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuStringN.hh>

#include <zlib/ZdfMockStore.hh>
#include <zlib/ZdfFileStore.hh>
#include <zlib/Zdf.hh>
#include <zlib/ZdfStats.hh>

void print(const char *s) {
  std::cout << s << '\n' << std::flush;
}
void print(const char *s, int64_t i) {
  std::cout << s << ' ' << i << '\n' << std::flush;
}
void ok(const char *s) { } // { print(s); }
void ok(const char *s, int64_t i) { } // { print(s, i); }
void fail(const char *s) { print(s); }
void fail(const char *s, int64_t i) { print(s, i); }
#define CHECK(x) ((x) ? ok("OK  " #x) : fail("NOK " #x))
#define CHECK2(x, y) ((x == y) ? ok("OK  " #x, x) : fail("NOK " #x, x))

struct Frame {
  uint64_t	v1;
  ZuFixedVal	v2_;

  ZuFixed v2() const { return ZuFixed{v2_, 9}; }
  void v2(ZuFixed v) { v2_ = v.adjust(9); }
};
ZtFields(Frame,
    (((v1)), (UInt), (Ctor<0>, Series, Index, Delta)),
    (((v2, Fn)), (Fixed), (Series, Delta, NDP<9>)));

void usage() {
  std::cerr << "usage: ZdfTest mem|load|save\n" << std::flush;
  ::exit(1);
};
int main(int argc, char **argv)
{
  if (argc < 2) usage();
  enum { Mem = 0, Load, Save };
  int mode;
  if (!strcmp(argv[1], "mem"))
    mode = Mem;
  else if (!strcmp(argv[1], "load"))
    mode = Load;
  else if (!strcmp(argv[1], "save"))
    mode = Save;
  else {
    usage();
    return 1; // suppress compiler warning
  }
  ZeLog::init("ZdfTest");
  ZeLog::level(0);
  ZeLog::start();
  using namespace Zdf;
  using namespace ZdfCompress;
  Zdf::MockStore mockStore;
  if (mode == Mem) mockStore.init(nullptr, nullptr);
  ZmScheduler sched(ZmSchedParams().nThreads(2));
  Zdf::FileStore fileStore;
  if (mode != Mem) {
    ZmRef<ZvCf> cf = new ZvCf{};
    cf->fromString(
	"dir .\n"
	"coldDir .\n"
	"writeThread 1\n");
    fileStore.init(&sched, cf);
  }
  DataFrame df{ZtMFieldList<Frame>(), "frame"};
  if (mode == Mem)
    df.init(&mockStore);
  else
    df.init(&fileStore);
  sched.start();
  ZmBlock<>{}([&df](auto wake) {
    df.open([wake = ZuMv(wake)](OpenResult) mutable {
      // FIXME - open result
      wake();
    });
  });
  Frame frame;
  if (mode == Mem || mode == Save) {
    auto writer = df.writer();
    for (uint64_t i = 0; i < 300; i++) { // 1000
      frame.v1 = i;
      frame.v2_ = i * 42;
      writer.write(&frame);
    }
  }
  if (mode == Mem || mode == Load) {
    AnyReader index, reader;
    df.find(index, 0, ZuFixed{20, 0});
    std::cout << "offset=" << index.offset() << '\n';
    df.seek(reader, 1, index.offset());
    ZuFixed v;
    CHECK(reader.read(v));
    CHECK(v.mantissa() == 20 * 42);
    CHECK(v.exponent() == 9);
    index.findFwd(ZuFixed{200, 0});
    std::cout << "offset=" << index.offset() << '\n';
    reader.seekFwd(index.offset());
    CHECK(reader.read(v));
    CHECK(v.mantissa() == 200 * 42);
    CHECK(v.exponent() == 9);
    index.findRev(ZuFixed{100, 0});
    std::cout << "offset=" << index.offset() << '\n';
    reader.seekRev(index.offset());
    AnyReader cleaner;
    {
      auto offset = reader.offset();
      offset = offset < 100 ? 0 : offset - 100;
      df.seek(cleaner, 1, offset);
    }
    Zdf::StatsTree<> w;
    while (reader.read(v)) {
      w.add(v);
      if (cleaner.read(v)) w.del(v);
      std::cout << "min=" << ZuBoxed(w.minimum()) <<
	" max=" << ZuBoxed(w.maximum()) <<
	" mean=" << ZuBoxed(w.mean()) <<
	" stddev=" << ZuBoxed(w.std()) <<
	" median=" << ZuBoxed(w.median()) <<
	" 95%=" << ZuBoxed(w.rank(0.95)) << '\n';
    }
    // for (auto k = w.begin(); k != w.end(); ++k) std::cout << *k << '\n';
    // for (auto k: w) std::cout << k.first << '\n';
    // std::cout << "stddev=" << w.std() << '\n';
  }
  ZmBlock<>{}([&df](auto wake) {
    df.close([wake = ZuMv(wake)](CloseResult) mutable {
      // FIXME - close result
      wake();
    });
  });
  sched.stop();
  ZeLog::stop();
  std::cout << ZmHeapMgr::csv();
  // df.final();
  // mgr.final();
}
