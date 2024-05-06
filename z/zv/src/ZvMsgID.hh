//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// concrete generic msg ID
//
// Link ID - ZuID (union of 8-byte string with uint64)
// SeqNo - uint64

#ifndef ZvMsgID_HH
#define ZvMsgID_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuID.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZvSeqNo.hh>

struct ZvMsgID {
  ZuID		linkID;
  ZvSeqNo	seqNo;

  void update(const ZvMsgID &u) {
    linkID.update(u.linkID);
    seqNo.update(u.seqNo);
  }
  template <typename S> void print(S &s) const {
    s << "linkID=" << linkID << " seqNo=" << seqNo;
  }
  friend ZuPrintFn ZuPrintType(ZvMsgID *);
};

#endif /* ZvMsgID_HH */
