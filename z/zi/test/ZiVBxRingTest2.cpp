//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuStringN.hpp>

#include <zlib/ZeLog.hpp>

// #define ZiVBxRing_STRESSTEST
#include <zlib/ZiVBxRing.hpp>

void usage()
{
  std::cerr <<
    "usage: ZiVBxRingTest2 [OPTION]... NAME\n"
    "  test read/write ring buffer in shared memory\n"
    "\tNAME\t- name of shared memory segment\n\n"
    "Options:\n"
    "  -r\t\t- read from buffer\n"
    "  -w\t\t- write to buffer (default)\n"
    "  -x\t\t- read and write in same process\n"
    "  -l N\t\t- loop N times\n"
    "  -g\t\t- test GC / attach / detach contention\n"
    "  -b BUFSIZE\t- set buffer size to BUFSIZE (default: 8192)\n"
    "  -m MSGSIZE\t- set message size to MSGSIZE (default: 1024)\n"
    "  -n COUNT\t- set number of messages to COUNT (default: 1)\n"
    "  -i INTERVAL\t- set delay between messages in seconds (default: 0)\n"
    "  -L\t\t- low-latency (readers spin indefinitely and do not yield)\n"
    "  -s SPIN\t- set spin count to SPIN (default: 1000)\n"
    "  -S\t\t- slow reader (sleep INTERVAL seconds in between reads)\n"
    "  -c CPUSET\t- bind memory to CPUSET\n"
    << std::flush;
  Zm::exit(1);
}

using Ring = ZiVBxRing;

class Msg {
public:
  Msg() : m_type(0), m_length(0) { }
  Msg(unsigned type, unsigned length) :
    m_type(type), m_length(length) { }

  ZuInline unsigned type() const { return m_type; }
  ZuInline unsigned length() const { return m_length; }
  ZuInline void *ptr() { return (void *)&this[1]; }
  ZuInline const void *ptr() const { return (const void *)&this[1]; }

private:
  uint16_t	m_type;
  uint16_t	m_length;
};

unsigned sizeFn(const void *ptr)
{
  return sizeof(Msg) + static_cast<const Msg *>(ptr)->length();
}

struct App {
  App() :
    flags(Ring::Write | Ring::Create), gc(false), ring(0),
    count(1), msgsize(1024), interval((time_t)0), slow(false) { }

  int main(int, char **);
  void reader();
  void writer();

  unsigned	flags;
  bool		gc;
  Ring		*ring;
  ZmTime	start, end;
  unsigned	count;
  unsigned	msgsize;
  ZmTime	interval;
  bool		slow;
  ZmBitmap	cpuset;
};

int main(int argc, char **argv)
{
  App a;
  return a.main(argc, argv);
}

int App::main(int argc, char **argv)
{
  const char *name = 0;
  unsigned bufsize = 8192;
  bool ll = false;
  unsigned spin = 1000;
  unsigned loop = 1;
  
  for (int i = 1; i < argc; i++) {
    if (argv[i][0] != '-') {
      if (name) usage();
      name = argv[i];
      continue;
    }
    switch (argv[i][1]) {
      case 'r':
	flags = Ring::Read | Ring::Create;
	break;
      case 'w':
	flags = Ring::Write | Ring::Create;
	break;
      case 'x':
	flags = Ring::Read | Ring::Write | Ring::Create;
	break;
      case 'l':
	if (++i >= argc) usage();
	loop = atoi(argv[i]);
	break;
      case 'g':
	gc = true;
	break;
      case 'b':
	if (++i >= argc) usage();
	bufsize = atoi(argv[i]);
	break;
      case 'm':
	if (++i >= argc) usage();
	msgsize = atoi(argv[i]);
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
      default:
	usage();
	break;
    }
  }

  if (!name) usage();

  ring = new Ring(sizeFn, ZiVBxRingParams(name).
      size(bufsize).ll(ll).spin(spin).coredump(true).cpuset(cpuset));

  for (unsigned i = 0; i < loop; i++) {
    {
      ZeError e;

      if (ring->open(flags, &e) != Zi::OK) {
	std::cerr << e << '\n' << std::flush;
	Zm::exit(1);
      }
    }

    std::cerr << 
      "address: 0x" << ZuBoxPtr(ring->data()).hex() <<
      "  ctrlSize: " << ZuBoxed(ring->ctrlSize()) <<
      "  size: " << ZuBoxed(ring->size()) << '\n';

    {
      ZmThread r, w;

      if (flags & Ring::Read)
	r = ZmThread(0, ZmFn<>::Member<&App::reader>::fn(this));
      if (flags & Ring::Write)
	w = ZmThread(0, ZmFn<>::Member<&App::writer>::fn(this));
      if (r) r.join();
      if (w) w.join();
    }

    start = end - start;

    std::cerr <<
      "total time: " <<
      ZuBoxed(start.sec()) << '.' <<
	ZuBoxed(start.nsec()).fmt<ZuFmt::Frac<9>>() <<
      "  avg time: " <<
      ZuBoxed((double)((start.dtime() / (double)count) * (double)1000000)) <<
      " usec\n";

    ring->close();
  }

  return 0;
}

void App::reader()
{
  std::cerr << (ZuStringN<80>{} << "reader " << Zm::getTID() << " started\n") << std::flush;
  if (!gc) {
    ring->attach();
    std::cerr << "reader attached\n";
  }
  if (!(flags & Ring::Write)) start.now();
  for (int j = 0; j < count; j++) {
    if (gc) {
      ring->attach();
      ring->detach();
      continue;
    }
    if (const void *msg_ = ring->shift()) {
      // auto msg = static_cast<const Msg *>(msg_);
      // std::cerr << (ZuStringN<80>{} << "shift: " << ZuBoxPtr(msg).hex() << " len: " << ZuBoxed(sizeof(Msg) + msg->length()) << '\n') << std::flush;
      // for (unsigned i = 0, n = msg->length(); i < n; i++) assert(((const char *)(msg->ptr()))[i] == (char)(i & 0xff));
      // std::cerr << "msg read\n";
      ring->shift2();
    } else {
      ZuStringN<80> s;
      s << "reader count " << j << ' ';
      int i = ring->readStatus();
      if (i == Zi::EndOfFile)
	s << "EOF\n";
      else if (!i)
	s << "ring empty\n";
      else
	s << "readStatus() returned " << ZuBoxed(i) << '\n';
      std::cerr << s << std::flush;
      Zm::sleep(.1);
      --j;
      continue;
    }
    if (slow && !!interval) Zm::sleep(interval);
  }
  std::cerr << (ZuStringN<80>{} << "reader count " << count << " completed\n") << std::flush;
  end.now();
  if (!gc) ring->detach();
}

void App::writer()
{
  std::cerr << (ZuStringN<80>{} << "writer " << Zm::getTID() << " started\n") << std::flush;
  start.now();
  for (int j = 0; j < count; j++) {
    if (gc) {
      ring->gc();
      // int i = ring->gc();
      // { ZuStringN<32> s; s << "GC: " << ZuBoxed(i) << "\n"; std::cerr << s; }
      continue;
    }
    if (void *ptr = ring->push(msgsize)) {
      // std::cerr << (ZuStringN<80>{} << "push: " << ZuBoxPtr(ptr).hex() << " len: " << ZuBoxed(msgsize) << '\n') << std::flush;
      // Msg *msg = new (ptr) Msg(0, msgsize - sizeof(Msg));
      // for (unsigned i = 0, n = msg->length(); i < n; i++) ((char *)(msg->ptr()))[i] = (char)(i & 0xff);
      // std::cerr << "msg written\n";
      ring->push2();
    } else {
      ZuStringN<80> s;
      s << "writer count " << j << ' ';
      int i = ring->writeStatus();
      if (i == Zi::EndOfFile)
	s << "EOF\n";
      else if (i == Zi::IOError)
	s << "I/O Error\n";
      else if (i == Zi::NotReady)
	s << "Not Ready - no readers\n";
      else
	s << "Ring Full\n";
      std::cerr << s << std::flush;
      Zm::sleep(.1);
      --j;
      continue;
    }
    if (!slow && !!interval) Zm::sleep(interval);
  }
  std::cerr << (ZuStringN<80>{} << "writer count " << count << " completed\n") << std::flush;
  if (!(flags & Ring::Read)) end.now();
  ring->eof();
  std::cerr << (ZuStringN<80>{} << "ring full " << ZuBoxed(ring->full()) << " times\n") << std::flush;
}
