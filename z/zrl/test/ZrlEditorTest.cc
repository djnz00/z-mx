#include <zlib/ZuCArray.hh>

#include <zlib/ZmSemaphore.hh>

#include <zlib/ZrlEditor.hh>

int main()
{
  ZmScheduler s{ZmSchedParams{}.id("sched").nThreads(1)};
  s.start();
  Zrl::App app;
  Zrl::Config config;
  Zrl::Editor editor;
  ZmSemaphore done;
  app.prompt = [](ZtArray<uint8_t> &s) { if (!s) s = "-->] "; };
  app.enter = [&done](ZuCSpan s) -> bool {
    std::cout << s << '\n';
    if (s == "quit") {
      done.post();
      return true;
    }
    return false;
  };
  app.end = [&done]() { done.post(); };
  app.sig = [&done](int sig) -> bool {
    switch (sig) {
      case SIGINT: std::cout << "SIGINT\n" << std::flush; break;
      case SIGQUIT: std::cout << "SIGQUIT\n" << std::flush; break;
      case SIGTSTP: std::cout << "SIGTSTP\n" << std::flush; break;
    }
    if (sig == SIGINT || sig == SIGQUIT) {
      done.post();
      return true;
    }
    return false;
  };
  ZtArray<ZtArray<const uint8_t>> history;
  app.histSave = [&history](unsigned i, ZuSpan<const uint8_t> s) {
    if (history.length() <= i) history.grow(i + 1);
    history[i] = s;
  };
  app.histLoad = [&history](unsigned i, Zrl::HistFn fn) -> bool {
    if (i >= history.length()) return false;
    fn(history[i]);
    return true;
  };
  editor.init(config, app);
  editor.open(&s, 1);
  editor.start([](Zrl::Editor &editor) {
    std::cout << editor.dumpVKeys();
    std::cout << editor.dumpMaps();
  });
  done.wait();
  editor.stop();
  editor.close();
  editor.final();
  s.stop();
}
