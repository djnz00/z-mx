//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame library - DB schema

#ifndef ZdfSchema_HH
#define ZdfSchema_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <Zdb.hh>

#include <zlib/zdf_dataframe_fbs.h>
#include <zlib/zdf_series_fbs.h>
#include <zlib/zdf_blk_fbs.h>

namespace Zdf::DB {

struct DataFrame {
  uint32_t	id;
  ZuDateTime	epoch;
  ZtString	name;
};
ZfbFields(DataFrame,
  (((id),	(Ctor<0>, Keys<0>)),		(UInt32)),
  (((name),	(Ctor<2>, Keys<1>, Mutable)),	(String)),
  (((epoch),	(Ctor<1>)),			(DateTime)));

struct Series {
  uint32_t	dfID;
  uint32_t	id;
  ZtString	name;
};
ZfbFields(Series,
  (((dfID),	(Ctor<0>, Keys<0>, Group<0>)),	(UInt32)),
  (((id),	(Ctor<1>, Keys<0>)),		(String)),
  (((name),	(Ctor<2>, Keys<1>, Mutable)),	(DateTime)));

struct Blk {
  uint32_t	seriesID;
  uint32_t	blkID;
  uint64_t	offset;
  int64_t	last;
  uint16_t	count;
  uint8_t	ndp;
};
ZfbFields(Blk,
  (((seriesID),	(Ctor<0>, Keys<0>, Group<0>, Descend<0>)),	(UInt32)),
  (((blkID),	(Ctor<1>, Keys<0>, Descend<0>)),		(UInt32)),
  (((offset),	(Ctor<2>, Mutable)),				(UInt64)),
  (((last),	(Ctor<3>, Mutable)),				(Int64)),
  (((count),	(Ctor<4>, Mutable)),				(UInt16)),
  (((ndp),	(Ctor<5>, Mutable)),				(UInt8)));

enum { BlkSize = 4096 };

struct BlkData {
  uint32_t	seriesID;
  uint32_t	blkID;
  uint8_t	data[BlkSize];
};
ZfbFields(BlkData,
  (((seriesID),	(Ctor<0>, Keys<0>, Group<0>, Descend<0>)),	(UInt32)),
  (((blkID),	(Ctor<1>, Keys<0>, Descend<0>)),		(UInt32)),
  (((data),	(Mutable)),					(Bytes)));

} // Zdf::DB

#endif /* ZdfSchema_HH */
