//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// MxT

#include <mxt/MxTVersion.hpp>

#include "../version.h"

unsigned long MxTVULong(int major, int minor, int patch)
{
  return MXT_VULONG(major, minor, patch);
}

unsigned long MxTVersion() { return MXT_VERSION; }
const char *MxTVerName() { return MXT_VERNAME; }

int MxTVMajor(unsigned long n)
{
  return MXT_VMAJOR(n);
}
int MxTVMinor(unsigned long n)
{
  return MXT_VMINOR(n);
}
int MxTVPatch(unsigned long n)
{
  return MXT_VPATCH(n);
}

