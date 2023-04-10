//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// command line interface

#include <zlib/Zrl.hpp>

#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmCondition.hpp>

#include <zlib/ZrlCLI.hpp>
#include <zlib/ZrlGlobber.hpp>
#include <zlib/ZrlHistory.hpp>

namespace {

class Context {
  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using Cond = ZmCondition<Lock>;

private:
  // Finite State Machine
  enum {
    Stopped,
    Editing,
    Processing
  };
  // 		 Stopped    Editing    Processing
  // 		 -------    -------    ----------
  // readline()  Editing               Editing
  // app.enter()            Processing
  // app.end()              Stopped
  // SIGINT                 Stopped
  // app.error()            Stopped
  // stop()                 Stopped    Stopped

  Lock			lock;
    Zrl::Globber	  globber;
    Zrl::History	  history{100};
    Zrl::CLI		  cli;
    ZtArray<uint8_t>	  prompt;
    char		  *data = nullptr;
    Cond		  cond{lock};
    int			  state = Stopped;

public:
  Context() {
    cli.init(Zrl::App{
      .error = [this](ZuString s) { std::cerr << s << '\n'; stop(); },
      .prompt = [this](ZtArray<uint8_t> &s) {
	Guard guard(lock);
	if (prompt.owned()) s = ZuMv(prompt);
      },
      .enter = [this](ZuString s) -> bool { return process(s); },
      .end = [this]() { stop(); },
      .sig = [this](int sig) -> bool {
	switch (sig) {
	  case SIGINT:
	    stop();
	    return true;
#ifdef _WIN32
	  case SIGQUIT:
	    GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0);
	    return true;
#endif
	  case SIGTSTP:
	    raise(sig);
	    return false;
	  default:
	    return false;
	}
      },
      .compInit = globber.initFn(),
      .compNext = globber.nextFn(),
      .histSave = history.saveFn(),
      .histLoad = history.loadFn()
    });
  }
  ~Context() {
    cli.final();
  }

  void start(const char *prompt_) {
    Guard guard(lock);
    if (prompt_) prompt.copy(prompt_);
    if (state == Stopped) start_();
  }
private:
  void start_() {
    if (!cli.open()) {
      state = Stopped;
      cond.broadcast();
      return;
    }
    cli.start();
    state = Editing;
    cond.broadcast();
    while (state == Editing) cond.wait();
  }

  void stop_() {
    cli.stop();
    cli.close();
  }

public:
  void stop() {
    Guard guard(lock);
    state = Stopped;
    cond.broadcast();
  }

  bool running() {
    Guard guard(lock);
    return state != Stopped;
  }

private:
  void edit_() {
    state = Editing;
    cond.broadcast();
    while (state == Editing) cond.wait();
  }

  bool process(ZuString s) {
    Guard guard(lock);
    {
      unsigned n = s.length();
      data = static_cast<char *>(::malloc(n + 1));
      memcpy(data, s.data(), n);
      data[n] = 0;
    }
    state = Processing;
    cond.broadcast();
    while (state == Processing) cond.wait();
    return state == Stopped;
  }

public:
  char *readline(const char *prompt_) {
    Guard guard(lock);
    if (prompt_) prompt.copy(prompt_);
    switch (state) {
      case Stopped:
	start_();
	if (state == Stopped) return nullptr;
	break;
      case Processing:
	edit_();
	break;
      default:
	return nullptr;	// multiple overlapping readline() calls
    }
    if (state == Stopped) {
      stop_();
      return nullptr;
    }
    return data; // caller frees
  }
};

inline Context *instance() {
  return ZmSingleton<Context>::instance();
}

}

ZrlExtern char *Zrl_readline(const char *prompt)
{
  return instance()->readline(prompt);
}

ZrlExtern void Zrl_start(const char *prompt)
{
  instance()->start(prompt);
}

ZrlExtern void Zrl_stop()
{
  instance()->stop();
}

ZrlExtern bool Zrl_running()
{
  return instance()->running();
}
