//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// heap configuration

#ifndef ZvHeapCSV_HH
#define ZvHeapCSV_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuStringN.hh>

#include <zlib/ZmHeap.hh>

#include <zlib/ZtField.hh>
#include <zlib/ZvCSV.hh>

namespace ZvHeapCSV {

struct Data {
  ZmIDString	id;
  unsigned	partition;
  unsigned	alignment;
  uint64_t	cacheSize;
  ZmBitmap	cpuset;
};

ZtFields(Data,
    (((id)), (String), (Ctor(0))),
    (((partition)), (Int), (Ctor(1))),
    (((alignment)), (Int), (Ctor(2))),
    (((cacheSize)), (Int), (Ctor(3))),
    (((cpuset)), (String), (Ctor(4))));

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

inline void init(ZuString file) {
  if (file) CSV{}.read(file);
}

} // ZvHeapCSV

#endif /* ZvHeapCSV_HH */
