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

// command line interface - application callbacks

#ifndef ZrlApp_HPP
#define ZrlApp_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZrlLib_HPP
#include <zlib/ZrlLib.hpp>
#endif

#include <signal.h>
#ifndef SIGQUIT
#define SIGQUIT 3
#endif
#ifndef SIGTSTP
#define SIGTSTP 20
#endif

#include <zlib/ZuString.hpp>

#include <zlib/ZePlatform.hpp>

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

#endif /* ZrlApp_HPP */
