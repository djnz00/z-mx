#include <mxbase/MxTelemetry.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

static ZmSemaphore sem;

class App : public MxTelemetry::Server {
public:
  App() : m_time(ZuTime::Now) { }

private:
  void run(MxTelemetry::Server::Cxn *cxn) {
    using namespace MxTelemetry;

    ZmHeapMgr::all(ZmFn<void(ZmHeapCache *)>{cxn,
	[](Cxn *cxn, ZmHeapCache *h) { cxn->transmit(heap(h)); }});

    ZmHashMgr::all(ZmFn<void(ZmAnyHash *)>{
	cxn, [](Cxn *cxn, ZmAnyHash *h) {
	  cxn->transmit(hashTbl(h));
	}});

    ZmSpecific<ZmThreadContext>::all([cxn](ZmThreadContext *tc) {
      cxn->transmit(thread(tc));
    });
  }

  ZuTime	m_time;
};

int main()
{
  ZmTrap::sigintFn([]{ sem.post(); });
  ZmTrap::trap();

  ZeLog::init("MxTelServer");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
  ZeLog::start();

  ZmRef<ZvCf> cf = new ZvCf();
  cf->fromString(
      "telemetry {\n"
      "  ip 127.0.0.1\n"
      "  port 19300\n"
      "  freq 1000000\n"
      "}\n",
      false);

  ZmRef<MxMultiplex> mx = new MxMultiplex("mx", cf->getCf("mx"));

  App app;

  app.init(mx, cf->getCf<true>("telemetry"));

  mx->start();

  ZmThread{0, []{ while (sem.trywait() < 0); sem.post(); },
    ZmThreadParams().name("busy")};

  app.start();

  sem.wait();

  app.stop();

  mx->stop();

  app.final();
}
