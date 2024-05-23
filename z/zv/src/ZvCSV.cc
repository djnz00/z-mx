//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// CSV parser

#include <stdio.h>
#include <ctype.h>

#include <zlib/ZePlatform.hh>

#include <zlib/ZvCSV.hh>

namespace ZvCSV_ {

void split(ZuString row, ZtArray<ZtArray<char>> &values)
{
  enum { Value = 0, Quoted, Quoted2, EOV, EOL };
  int state;
  char ch;
  int offset = 0, length = row.length();
  int start, end, next;
  bool simple;

  for (;;) {
    start = offset;
    state = Value;
    simple = true;

    for (;;) {
      ch = offset >= length ? 0 : row[offset];
      switch (state) {
	case Value:
	  if (!ch) { end = offset; state = EOL; break; }
	  if (ch == '"') { simple = false; state = Quoted; break; }
	  if (ch == ',') { end = offset; state = EOV; break; }
	  break;
	case Quoted:
	  if (!ch) { end = offset; state = EOL; break; }
	  if (ch == '"') { state = Quoted2; break; }
	  break;
	case Quoted2:
	  if (!ch) { end = offset; state = EOL; break; }
	  if (ch == '"') { state = Quoted; break; }
	  if (ch == ',') { end = offset; state = EOV; break; }
	  state = Value;
	  break;
      }
      if (state == EOV || state == EOL) break;
      offset++;
    }

    if (state == EOV) {
      while (offset < length && isspace(row[++offset]));
      next = offset;
    } else // state == EOL
      next = -1;

    if (simple) {
      // relies on caller ensuring that values scope does not exceed row scope
      new (values.push())
	ZtArray<char>(row.data() + start, end - start, end - start, false);
    } else {
      ZtArray<char> value;
      value.size(end - start);
      state = Value;
      offset = start;
      while (offset < end) {
	ch = row[offset++];
	switch (state) {
	  case Value:
	    if (ch == '"') { state = Quoted; break; }
	    value += ch;
	    break;
	  case Quoted:
	    if (ch == '"') { state = Quoted2; break; }
	    value += ch;
	    break;
	  case Quoted2:
	    if (ch == '"') { value += ch; state = Quoted; break; }
	    value += ch;
	    state = Value;
	    break;
	}
      }
      values.push(ZuMv(value));
    }

    if (next < 0) break;
    offset = next;
  }
}

} // ZvCSV_
