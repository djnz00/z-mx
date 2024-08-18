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
  DFID		id;		// 1-based
  ZtString	name;
  ZuDateTime	epoch;
};
ZfbFields(DataFrame,
  (((id),	(Ctor<0>, Keys<0>, Descend<0>)),	(UInt32)),
  (((name),	(Ctor<1>, Keys<1>, Mutable)),		(String)),
  (((epoch),	(Ctor<2>)),				(DateTime)));

struct SeriesFixed {
  SeriesID	id;		// 1-based
  DFID		dfid;		// 0 is null
  ZtString	name;
  Fixed		first;		// first value in series
  ZuDateTime	epoch;		// intentionally denormalized
  BlkOffset	blkOffset;	// first block
  NDP		ndp;		// NDP of first value in series
};
ZfbFields(SeriesFixed,
  (((id),	(Ctor<0>, Keys<0>, Descend<0>)),	(UInt32)),
  (((dfid),	(Ctor<1>, Keys<2>, Group<2>)),		(UInt32)),
  (((name),	(Ctor<2>, Keys<1, 2>)),			(String)),
  (((first),	(Ctor<3>, Mutable)),			(Int64)),
  (((ndp),	(Ctor<6>, Mutable)),			(UInt8)),
  (((epoch),	(Ctor<4>)),				(DateTime)),
  (((blkOffset),(Ctor<5>, Mutable)),			(UInt64)));

struct SeriesFloat {
  SeriesID	id;		// 1-based
  DFID		dfid;		// 0 is null
  ZtString	name;
  double	first;		// first value in series
  ZuDateTime	epoch;		// intentionally denormalized
  BlkOffset	blkOffset;	// first block
};
ZfbFields(SeriesFloat,
  (((id),	(Ctor<0>, Keys<0>, Descend<0>)),	(UInt32)),
  (((dfid),	(Ctor<1>, Keys<2>, Group<2>)),		(UInt32)),
  (((name),	(Ctor<2>, Keys<1, 2>)),			(String)),
  (((first),	(Ctor<3>, Mutable)),			(Float)),
  (((epoch),	(Ctor<4>)),				(DateTime)),
  (((blkOffset),(Ctor<5>, Mutable)),			(UInt64)));

struct BlkHdrFixed {
  BlkOffset	blkOffset;
  Offset	offset;
  Fixed		last;
  SeriesID	seriesID;
  BlkCount	count;
  NDP		ndp;
};
ZfbFields(BlkHdrFixed,
  (((seriesID),	(Ctor<3>, Keys<0>, Group<0>, Descend<0>)),	(UInt32)),
  (((blkOffset),(Ctor<0>, Keys<0>, Descend<0>)),		(UInt64)),
  (((offset),	(Ctor<1>, Mutable)),				(UInt64)),
  (((last),	(Ctor<2>, Mutable)),				(Int64)),
  (((count),	(Ctor<4>, Mutable)),				(UInt16)),
  (((ndp),	(Ctor<5>, Mutable)),				(UInt8)));

struct BlkHdrFloat {
  BlkOffset	blkOffset;
  Offset	offset;
  double	last;
  SeriesID	seriesID;
  BlkCount	count;
};
ZfbFields(BlkHdrFloat,
  (((seriesID),	(Ctor<3>, Keys<0>, Group<0>, Descend<0>)),	(UInt32)),
  (((blkOffset),(Ctor<0>, Keys<0>, Descend<0>)),		(UInt64)),
  (((offset),	(Ctor<1>, Mutable)),				(UInt64)),
  (((last),	(Ctor<2>, Mutable)),				(Float)),
  (((count),	(Ctor<4>, Mutable)),				(UInt16)));

enum { BlkSize = 4096 };

using BlkDataBuf = ZuArrayN<uint8_t, BlkSize>;

struct BlkData {
  BlkOffset		blkOffset;
  SeriesID		seriesID;
  BlkDataBuf		buf;
};
ZfbFields(BlkData,
  (((seriesID),	(Ctor<1>, Keys<0>, Group<0>, Descend<0>)),	(UInt32)),
  (((blkOffset),(Ctor<0>, Keys<0>, Descend<0>)),		(UInt32)),
  (((buf),	(Mutable)),					(Bytes)));

} // Zdf::DB

#endif /* ZdfSchema_HH */
