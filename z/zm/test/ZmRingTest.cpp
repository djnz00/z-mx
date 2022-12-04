//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuStringN.hpp>

#include <zlib/ZmRing.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmSpinLock.hpp>
#include <zlib/ZmTimeInterval.hpp>

void usage()
{
  std::cerr <<
    "usage: ZmRingTest [OPTION]...\n"
    "  test read/write ring buffer in shared memory\n\n"
    "Options:\n"
    "  -l N\t\t- loop N times\n"
    "  -b BUFSIZE\t- set buffer size to BUFSIZE (default: 8192)\n"
    "  -n COUNT\t- set number of messages to COUNT (default: 1)\n"
    "  -w N\t\t- number of writer threads\n"
    "  -i INTERVAL\t- set delay between messages in seconds (default: 0)\n"
    "  -L\t\t- low-latency (readers spin indefinitely and do not yield)\n"
    "  -s SPIN\t- set spin count to SPIN (default: 1000)\n"
    "  -S\t\t- slow reader (sleep INTERVAL seconds in between reads)\n"
    "  -c CPUSET\t- bind memory to CPUSET\n";
  Zm::exit(1);
}

struct Msg {
  Msg() : m_p(reinterpret_cast<uintptr_t>(this)) { }
  ~Msg() { m_p = 0; }
  bool ok() const { return m_p == reinterpret_cast<uintptr_t>(this); }
  uintptr_t m_p;
};

using Ring = ZmRing<ZmRingT<Msg, ZmRingMW<true>>>;

struct App {
  int main(int, char **);
  void reader();
  void writer(unsigned);

  Ring				*ring = nullptr;
  ZmTime			start, end;
  ZmTimeInterval<ZmSpinLock>	readTime, writeTime;
  unsigned			count = 1;
  unsigned			writers = 1;
  ZmTime			interval;
  bool				slow = false;
  ZmBitmap			cpuset;
};

int main(int argc, char **argv)
{
  App a;
  return a.main(argc, argv);
}

int App::main(int argc, char **argv)
{
  unsigned bufsize = 8192;
  bool ll = false;
  unsigned spin = 1000;
  unsigned loop = 1;

  for (int i = 1; i < argc; i++) {
    if (argv[i][0] != '-') usage();
    switch (argv[i][1]) {
      case 'l':
	if (++i >= argc) usage();
	loop = atoi(argv[i]);
	break;
      case 'b':
	if (++i >= argc) usage();
	bufsize = atoi(argv[i]);
	break;
      case 'n':
	if (++i >= argc) usage();
	count = atoi(argv[i]);
	break;
      case 'i':
	if (++i >= argc) usage();
	interval = ZmTime((double)ZuBox<double>(argv[i]));
	break;
      case 'L':
	ll = true;
	break;
      case 's':
	if (++i >= argc) usage();
	spin = atoi(argv[i]);
	break;
      case 'S':
	slow = true;
	break;
      case 'c':
	if (++i >= argc) usage();
	cpuset = argv[i];
	break;
      case 'w':
	if (++i >= argc) usage();
	writers = atoi(argv[i]);
	break;
      default:
	usage();
	break;
    }
  }

  ring = new Ring(ZmRingParams(bufsize).ll(ll).spin(spin).cpuset(cpuset));

  for (unsigned i = 0; i < loop; i++) {
    {
      if (ring->open(Ring::Read | Ring::Write) != Zu::OK) {
	std::cerr << "open failed\n" << std::flush;
	Zm::exit(1);
      }
    }

    std::cerr <<
      "address: 0x" << ZuBoxPtr(ring->data()).hex() <<
      "  ctrlSize: " << ZuBoxed(ring->ctrlSize()) <<
      "  size: " << ZuBoxed(ring->size()) <<
      "  msgSize: " << ZuBoxed(sizeof(Msg)) << '\n';

    {
      ZmThread r, w[writers];

      r = ZmThread(0, ZmFn<>::Member<&App::reader>::fn(this));
      for (unsigned i = 0; i < writers; i++)
	w[i] = ZmThread(0, ZmFn<>([this, i]() { this->writer(i); }));
      for (unsigned i = 0; i < writers; i++)
	if (w[i]) w[i].join();
      ring->eof();
      if (r) r.join();
    }

    start = end - start;

    {
      ZuStringN<80> s;
      s << "total time: " <<
	ZuBoxed(start.sec()) << '.' <<
	  ZuBoxed(start.nsec()).fmt<ZuFmt::Frac<9>>() <<
	"  avg time: " <<
	ZuBoxed((double)((start.dtime() / (double)(count * writers)) *
	      (double)1000000)) <<
	" usec\n";
      std::cerr << s;
    }
    {
      ZuStringN<256> s;
      s << "shift: " << readTime << "\n"
	<< "push:  " << writeTime << "\n";
      std::cerr << s;
    }

    ring->close();
  }

  return 0;
}

void App::reader()
{
  std::cerr << "reader started\n";
  for (unsigned j = 0, n = count * writers; j < n; j++) {
    ZmTime readStart(ZmTime::Now);
    if (const Msg *msg = ring->shift()) {
      // printf("shift: \"%s\"\n", msg->data());
      // fwrite("msg read\n", 1, 9, stderr);
      // assert(*msg == "hello world");
      if (ZuUnlikely(!msg->ok())) goto failed;
      msg->~Msg();
      ring->shift2();
      ZmTime readEnd(ZmTime::Now);
      readTime.add(readEnd -= readStart);
    } else {
      int i = ring->readStatus();
      if (i == Zu::EndOfFile) {
	end.now();
	std::cerr << "reader EOF\n";
	break;
      } else if (!i)
	std::cerr << "ring empty\n";
      else {
	ZuStringN<80> s;
	s << "readStatus() returned " << ZuBoxed(i) << '\n';
	std::cerr << s;
      }
      Zm::sleep(.1);
      --j;
      continue;
    }
    if (slow && !!interval) Zm::sleep(interval);
  }
  end.now();
  return;
failed:
  end.now();
  std::cerr << "reader msg validation FAILED\n";
  return;
}

void App::writer(unsigned i)
{
  unsigned failed = 0;
  std::cerr << "writer started\n";
  if (!i) start.now();
  for (unsigned j = 0; j < count; j++) {
    unsigned full_ = ring->full();
    ZmTime writeStart(ZmTime::Now);
    if (void *ptr = ring->push()) {
      // puts("push");
      Msg *msg = new (ptr) Msg();
      // fwrite("msg written\n", 1, 12, stderr);
      ring->push2(ptr);
      ZmTime writeEnd(ZmTime::Now);
      if (ring->full() == full_) writeTime.add(writeEnd -= writeStart);
    } else {
      int i = ring->writeStatus();
      if (i == Zu::EndOfFile) {
	end.now();
	std::cerr << "writer EOF\n";
	break;
      } else if (i >= (int)sizeof(Msg))
	std::cerr << "writer OK!\n";
      else {
	std::cerr << "Ring Full\n";
	++failed;
      }
      Zm::sleep(.1);
      --j;
      continue;
    }
    if (!!interval) Zm::sleep(interval);
  }
  {
    ZuStringN<64> s;
    s << "push failed " << ZuBoxed(failed) << " times\n"
      << "ring full " << ZuBoxed(ring->full()) << " times\n";
    std::cerr << s;
  }
}
