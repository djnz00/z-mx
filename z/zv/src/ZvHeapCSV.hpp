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

// heap configuration

#ifndef ZvHeapCSV_HPP
#define ZvHeapCSV_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuStringN.hpp>

#include <zlib/ZmHeap.hpp>

#include <zlib/ZtField.hpp>
#include <zlib/ZvCSV.hpp>

struct ZvHeapCSV {
  struct Data : public ZtFieldTuple<Data> {
    static const ZtFieldArray fields();

    ZmIDString		id;
    ZuBox<unsigned>	partition;
    ZuBox<unsigned>	alignment;
    ZuBox<uint64_t>	cacheSize;
    ZmBitmap		cpuset;
  };
  const ZtFieldArray Data::fields() {
    ZtFields(Data,
	(String, id),
	(Int, partition),
	(Int, alignment),
	(Int, cacheSize),
	(String, cpuset)
  }

  class CSV : public ZvCSV<Data> {
  public:
    void read(ZuString file) {
      ZvCSV::readFile(file,
	  [this]() { return &m_data; },
	  [](Data *data) {
	    ZmHeapMgr::init(data->id, data->partition, ZmHeapConfig{
		data->alignment,
		data->cacheSize,
		data->cpuset});
	  });
    }

  private:
    Data	m_data;
  };

  static void init(ZuString file) {
    if (!file) return;
    CSV().read(file);
  }
};

#endif /* ZvHeapCSV_HPP */
