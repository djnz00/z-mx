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
 * License aint with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// fast, lightweight string class

#include <zlib/ZtString.hpp>

#include <zlib/ZuStringN.hpp>

template class ZtString_<char, ZtString_ID>;
template class ZtString_<wchar_t, ZtString_ID>;

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4311 4244 4996)
#endif

void ZtHexDump::print(ZmStream &s) const
{
  if (ZuUnlikely(!m_data)) return;

  s << m_prefix << '\n';

  ZuStringN<64> hex;
  ZuStringN<20> ascii;
  ZuStringN<48> pad;
  memset(pad.data(), ' ', 45);
  pad.length(45);

  unsigned offset, col;

  for (offset = 0; offset < m_length; offset += 16) {
    hex.null();
    ascii.null();
    using namespace ZuFmt;
    hex << ZuBoxed(offset).fmt<Hex<0, Alt<Right<8>>>>() << "  ";
    for (col = 0; col < 16; col++) {
      if (offset + col >= m_length) {
	hex << ZuString(pad.data(), 3 * (16 - col));
	break;
      }
      ZuBox<uint8_t> byte = m_data[offset + col];
      hex << byte.fmt<Hex<0, Right<2>>>() << ' ';
      ascii << ((byte >= 0x20 && byte < 0x7f) ?
	  static_cast<char>(byte.val()) : '.');
    }
    hex << ' ';
    s << hex << ascii << '\n';
  }
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
