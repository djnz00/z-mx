//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// command line interface - application callbacks

#ifndef ZrlApp_HH
#define ZrlApp_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZrlLib_HH
#include <zlib/ZrlLib.hh>
#endif

#include <signal.h>
#ifndef SIGQUIT
#define SIGQUIT 3
#endif
#ifndef SIGTSTP
#define SIGTSTP 20
#endif

#include <zlib/ZuString.hh>

#include <zlib/ZePlatform.hh>

namespace Zrl {

using ErrorFn = ZmFn<ZuString>;		// (message)

using OpenFn = ZmFn<bool>;		// (ok)
using CloseFn = ZmFn<>;

using PromptFn = ZmFn<ZtArray<uint8_t> &>;

using EnterFn = ZmFn<ZuString>;
using EndFn = ZmFn<>;
using SigFn = ZmFn<int>;

using CompSpliceFn = ZmFn<
  unsigned,			// off     - byte offset
  ZuUTFSpan,			// span    - UTF8 span to be replaced
  ZuArray<const uint8_t>,	// replace - replacement data
  ZuUTFSpan>;			// rspan   - UTF8 span of replacement
using CompIterFn = ZmFn<
  ZuArray<const uint8_t>,	// data    - completion data
  ZuUTFSpan>;			// span    - UTF8 span of completion

using CompInitFn = ZmFn<		// initialize completion
  ZuArray<const uint8_t>,	// data    - line data (entire line)
  unsigned,			// cursor  - byte offset of cursor
  CompSpliceFn>;		// splice  - line splice function
using CompStartFn = ZmFn<>;		// re-start iteration
using CompSubstFn = ZmFn<		// substitute next/prev completion
  CompSpliceFn,			// splice  - line splice function
  bool>;			// next    - true for next, false for previous
using CompNextFn = ZmFn<CompIterFn>;	// iterate next completion
using CompFinalFn = ZmFn<>;		// finalize completion

using HistFn = ZmFn<ZuArray<const uint8_t>>;

using HistSaveFn = ZmFn<unsigned, ZuArray<const uint8_t>>;
using HistLoadFn = ZmFn<unsigned, HistFn>;

struct App {
  ErrorFn	error;		// I/O error

  OpenFn	open;		// terminal opened
  CloseFn	close;		// terminal closed

  PromptFn	prompt;

  EnterFn	enter;		// line entered
  EndFn		end;		// end of input (EOF)
  SigFn		sig;		// signal (^C ^\ ^Z)

  CompInitFn	compInit;	// initialize completions
  CompFinalFn	compFinal;	// finalize completions
  CompStartFn	compStart;	// (re-)start enumeration of completions
  CompSubstFn	compSubst;	// substitute next completion in sequence
  CompNextFn	compNext;	// iterate next completion in sequence

  HistSaveFn	histSave;	// save line in history with index
  HistLoadFn	histLoad;	// load line from history given index
};

} // Zrl

#endif /* ZrlApp_HH */
