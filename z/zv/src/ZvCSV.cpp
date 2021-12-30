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

// CSV parser

#include <stdio.h>
#include <ctype.h>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZvCSV.hpp>

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
      new (values.push()) ZtArray<char>(row.data() + start, end - start);
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
