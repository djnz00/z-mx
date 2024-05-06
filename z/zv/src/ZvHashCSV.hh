//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// hash table configuration

#ifndef ZvHashCSV_HH
#define ZvHashCSV_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuStringN.hh>

#include <zlib/ZmHeap.hh>
#include <zlib/ZmHash.hh>

#include <zlib/ZtField.hh>
#include <zlib/ZvCSV.hh>

namespace ZvHashCSV {

struct Data {
  ZmIDString	id;
  unsigned	bits;
  double	loadFactor;
  unsigned	cBits;
};

ZtFields(Data,
    (((id)), (String), (Ctor(0))),
    (((bits)), (Int), (Ctor(1))),
    (((loadFactor)), (Int), (Ctor(2))),
    (((cBits)), (Int), (Ctor(3))));

class CSV : public ZvCSV<Data> {
public:
  void read(ZuString file) {
    ZvCSV::readFile(file,
	[this]() { return &m_data; },
	[](Data *data) {
	  ZmHashMgr::init(data->id, ZmHashParams{}.
	      bits(data->bits).
	      loadFactor(data->loadFactor).
	      cBits(data->cBits));
	});
  }

private:
  Data	m_data;
};

inline void init(ZuString file) {
  if (file) CSV{}.read(file);
}

} // ZvHashCSV

#endif /* ZvHashCSV_HH */
