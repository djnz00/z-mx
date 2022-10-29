//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <stdlib.h>
#include <stdio.h>

#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmThread.hpp>

class ZiVBxRing_Breakpoint {
public:
  ZiVBxRing_Breakpoint() : m_enabled(false), m_oneshot(false) { }
  void enable(bool oneshot = true) {
    m_enabled = true;
    m_oneshot = oneshot;
  }
  void disable() { m_enabled = false; }
  void wait() { m_reached.wait(); }
  void proceed() { m_proceed.post(); }
  void reached(const char *name) {
    std::cout << "\t       " << name << std::flush;
    if (!m_enabled) return;
    if (m_oneshot) m_enabled = false;
    m_reached.post();
    m_proceed.wait();
  }

private:
  bool		m_enabled;
  bool		m_oneshot;
  ZmSemaphore	m_reached;
  ZmSemaphore	m_proceed;
};

#define ZiVBxRing_FUNCTEST
#include <zlib/ZiVBxRing.hpp>
#include "../src/ZiVBxRing.cpp"

void fail()
{
  Zm::exit(1);
}
#define ensure(x) ((x) ? void{} : check_(x, __LINE__, #x))
#define check(x) check_(x, __LINE__, #x)
void check_(bool ok, unsigned line, const char *exp)
{
  printf("%s %6d %s\n", ok ? " OK " : "NOK ", line, exp);
  fflush(stdout);
  if (!ok) fail();
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

class Thread;

class App {
public:
  App() : m_nThreads(0), m_threads(0) { }
  ~App() { if (m_threads) delete [] m_threads; }

  const Thread *thread(unsigned i) const { return m_threads[i]; }
  Thread *thread(unsigned i) { return m_threads[i]; }

  void start(unsigned nThreads, ZiVBxRingParams params);
  void stop();

  const ZiVBxRingParams &params() const { return m_params; }

private:
  unsigned	m_nThreads;
  ZmRef<Thread>	*m_threads;
  ZiVBxRingParams	m_params;
};

class Work;

class Thread : public ZmObject {
public:
  Thread(App *app, unsigned id) :
    m_app(app), m_id(id), m_ring(sizeFn, app->params()), m_result(0) { }
  ~Thread() { }

  void operator ()();

  App *app() const { return m_app; }
  unsigned id() const { return m_id; }
  const Ring &ring() const { return m_ring; }
  Ring &ring() { return m_ring; }

  void start() {
    m_thread = ZmThread(0, ZmFn<>::Member<&Thread::operator()>::fn(this));
  }
  int synchronous(Work *work, ZeError *e = 0) {
    m_work = work;
    m_pending.post();
    m_completed.wait();
    if (e) *e = m_error;
    return m_result;
  }
  void asynchronous(Work *work) {
    m_work = work;
    m_pending.post();
  }
  int result(ZeError *e = 0) {
    m_completed.wait();
    if (e) *e = m_error;
    return m_result;
  }
  void stop() {
    m_work = 0;
    m_pending.post();
    m_thread.join();
  }

private:
  App			*m_app;
  unsigned		m_id;
  ZmThread		m_thread;
  Ring			m_ring;
  ZmSemaphore		m_pending;
  ZmSemaphore		m_completed;
  ZmRef<Work>		m_work;
  int			m_result;
  ZeError		m_error;
};

void App::start(unsigned nThreads, ZiVBxRingParams params)
{
  m_params = ZuMv(params);
  m_threads = new ZmRef<Thread>[m_nThreads = nThreads];
  for (unsigned i = 0; i < nThreads; i++)
    (m_threads[i] = new Thread(this, i))->start();
}

void App::stop()
{
  for (unsigned i = 0; i < m_nThreads; i++) {
    m_threads[i]->stop();
    m_threads[i] = 0;
  }
  delete [] m_threads;
  m_threads = 0;
  m_nThreads = 0;
}

class Work : public ZmObject {
public:
  enum Insn {
    Open = 0,
    Close,
    Push,
    Push2,
    EndOfFile,
    GC,
    Attach,
    Detach,
    Shift,
    Shift2,
    ReadStatus,
    WriteStatus
  };

  Work(Insn insn, unsigned param = 0) : m_insn(insn), m_param(param) { }

  int operator ()(Thread *, ZeError *);

private:
  Insn		m_insn;
  unsigned	m_param;
};

void Thread::operator ()()
{
  for (;;) {
    m_pending.wait();
    {
      ZmRef<Work> work = m_work;
      if (!work) return;
      m_work = 0;
      m_result = (*work)(this, &m_error);
    }
    m_completed.post();
  }
}

int Work::operator ()(Thread *thread, ZeError *e)
{
  Ring &ring = thread->ring();
  int result = 0;

  switch (m_insn) {
    case Open:
      result = ring.open(m_param, e);
      printf("\t%6u open(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case Close:
      ring.close();
      printf("\t%6u close()\n", thread->id()); fflush(stdout);
      break;
    case Push:
      if (void *ptr = ring.push(m_param)) {
	result = m_param;
	Msg *msg = new (ptr) Msg(0, m_param - sizeof(Msg));
	for (unsigned i = 0, n = msg->length(); i < n; i++)
	  ((char *)(msg->ptr()))[i] = (char)(i & 0xff);
      } else
	result = 0;
      printf("\t%6u push(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case Push2:
      ring.push2();
      printf("\t%6u push2()\n", thread->id()); fflush(stdout);
      break;
    case EndOfFile:
      ring.eof();
      printf("\t%6u eof()\n", thread->id()); fflush(stdout);
      break;
    case GC:
      result = ring.gc();
      printf("\t%6u gc(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case Attach:
      result = ring.attach();
      printf("\t%6u attach(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case Detach:
      result = ring.detach();
      printf("\t%6u detach(): %d\n", 
	  thread->id(), result); fflush(stdout);
      break;
    case Shift:
      if (const void *msg_ = ring.shift()) {
	result = sizeFn(msg_);
	auto msg = static_cast<const Msg *>(msg_);
	for (unsigned i = 0, n = msg->length(); i < n; i++)
	  ensure(((const char *)(msg->ptr()))[i] == (char)(i & 0xff));
      } else
	result = 0;
      printf("\t%6u shift(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case Shift2:
      ring.shift2();
      printf("\t%6u shift2()\n", thread->id()); fflush(stdout);
      break;
    case ReadStatus:
      result = ring.readStatus();
      printf("\t%6u readStatus(): %d\n", thread->id(), result); fflush(stdout);
      break;
    case WriteStatus:
      result = ring.writeStatus();
      printf("\t%6u writeStatus(): %d\n", thread->id(), result); fflush(stdout);
      break;
  }

  return result;
}

App *app()
{
  return ZmSingleton<App>::instance();
}

int synchronous(int tid, Work *work, ZeError *e = 0)
{
  return app()->thread(tid)->synchronous(work, e);
}

void asynchronous_(int tid, Work *work, ZiVBxRing_Breakpoint &bp)
{
  bp.enable();
  app()->thread(tid)->asynchronous(work);
  bp.wait();
}

#define asynchronous(tid, work, bp) \
  asynchronous_(tid, work, app()->thread(tid)->ring().bp_##bp)
#define proceed(tid, bp) \
  (app()->thread(tid)->ring().bp_##bp.proceed())

int result(int tid, ZeError *e = 0)
{
  return app()->thread(tid)->result(e);
}

#define Open(p) new Work(Work::Open, p)
#define Close() new Work(Work::Close)
#define Push(p) new Work(Work::Push, p)
#define Push2() new Work(Work::Push2)
#define EndOfFile() new Work(Work::EndOfFile)
#define GC() new Work(Work::GC)
#define Attach() new Work(Work::Attach)
#define Detach() new Work(Work::Detach)
#define Shift() new Work(Work::Shift)
#define Shift2() new Work(Work::Shift2)
#define ReadStatus() new Work(Work::ReadStatus)
#define WriteStatus() new Work(Work::WriteStatus)

void cleanup()
{
#ifndef _WIN32
  unlink("/dev/shm/ZiVBxRingTest.ctrl");
  unlink("/dev/shm/ZiVBxRingTest.data");
#endif
}

void usage()
{
  std::cerr <<
    "usage: ZiVBxRingTest [SIZE]\n"
    "\tSIZE - optional requested size of ring buffer\n"
    << std::flush;
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  atexit(&cleanup);

  int size = 8192;

  if (argc < 1 || argc > 2) usage();
  if (argc == 2) {
    size = atoi(argv[1]);
    if (!size) usage();
  }

  app()->start(3, ZiVBxRingParams("ZiVBxRingTest").size(size));

  check(synchronous(0, Open(Ring::Read | Ring::Create)) == Zi::OK);
  check(synchronous(1, Open(Ring::Read | Ring::Create)) == Zi::OK);
  check(synchronous(2, Open(Ring::Write)) == Zi::OK);

  int size1 =
    app()->thread(2)->ring().size() - Zm::CacheLineSize - 9;
  int size2 = (app()->thread(2)->ring().size() / 2) - 7;

  printf("requested size: %u actual size: %u size1: %u size2: %u\n",
      size, app()->thread(2)->ring().size(), size1, size2);
  fflush(stdout);

  // test push with concurrent attach
  check(synchronous(0, Attach()) == Zi::OK);
  asynchronous(1, Attach(), attach2);
  check(synchronous(2, Push(size1)) > 0);
  asynchronous(0, Shift(), shift1);
  synchronous(2, Push2());
  proceed(0, shift1);
  proceed(1, attach2);
  check(result(0) == size1);
  check(result(1) == Zi::OK);
  synchronous(0, Shift2());

  // test push with concurrent attach (2)
  check(synchronous(0, Detach()) == Zi::OK);
  asynchronous(0, Attach(), attach3);
  check(synchronous(2, Push(size1)) > 0);
  synchronous(2, Push2());
  proceed(0, attach3);
  check(result(0) == Zi::OK);
  check(synchronous(0, Shift()) == size1);
  synchronous(0, Shift2());
  check(synchronous(1, Shift()) == size1);
  synchronous(1, Shift2());

  // test push with concurrent dual shift
  check(synchronous(2, Push(size2)) > 0);
  asynchronous(0, Shift(), shift1);
  asynchronous(1, Shift(), shift1);
  synchronous(2, Push2());
  proceed(0, shift1);
  proceed(1, shift1);
  check(result(0) == size2);
  check(result(1) == size2);
  synchronous(0, Shift2());
  synchronous(1, Shift2());

  // test push with concurrent detach
  check(synchronous(2, Push(size1)) > 0); 
  asynchronous(0, Detach(), detach4);
  synchronous(2, Push2());
  check(synchronous(1, Shift()) == size1);
  synchronous(1, Shift2());
  proceed(0, detach4);
  check(result(0) == Zi::OK);
  check(app()->thread(2)->ring().length() == 0);
  check(synchronous(1, Detach()) == Zi::OK);

  // test overflow (gc / status) with concurrent detach
  check(synchronous(0, Attach()) == Zi::OK);
  check(synchronous(1, Attach()) == Zi::OK);
  check(synchronous(2, Push(size2)) > 0); synchronous(2, Push2());
  check(synchronous(2, Push(size2)) == 0);
  check(synchronous(2, GC()) == 0);
  check(synchronous(0, Shift()) == size2);
  synchronous(0, Shift2());
  asynchronous(1, Detach(), detach1);
  check(synchronous(0, ReadStatus()) == 0);
  proceed(1, detach1);
  check(result(1) == Zi::OK);
  check(synchronous(0, Detach()) == Zi::OK);

  synchronous(0, Close());
  synchronous(1, Close());
  synchronous(2, Close());

  return 0;
}
