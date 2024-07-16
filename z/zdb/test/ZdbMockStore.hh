//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// mock data store for Zdb automated testing
// - based on in-memory data store
// - optionally defers work with a work queue
// - optionally defers callbacks with a callback queue

#ifndef ZdbMockStore_HH
#define ZdbMockStore_HH

#include <zlib/ZdbMemStore.hh>

namespace zdbtest {

using namespace ZdbMem;

ZmXRing<ZmFn<>, ZmXRingLock<ZmPLock>> work, callbacks;

bool deferWork = false, deferCallbacks = false;

void performWork() { while (auto fn = work.shift()) fn(); }
void performCallbacks() { while (auto fn = callbacks.shift()) fn(); }

class StoreTbl : public ZdbMem::StoreTbl {
public:
  StoreTbl(
    ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
    const reflection::Schema *schema, BufAllocFn bufAllocFn
  ) : ZdbMem::StoreTbl{
    id, ZuMv(fields), ZuMv(keyFields), schema, ZuMv(bufAllocFn)
  } { }

  auto count() { return ZdbMem::StoreTbl::count(); }

  void count(unsigned keyID, ZmRef<const IOBuf> buf, CountFn countFn) {
    auto work_ = [
      this, keyID, buf = ZuMv(buf), countFn = ZuMv(countFn)
    ]() mutable {
      ZdbMem::StoreTbl::count(keyID, ZuMv(buf), ZuMv(countFn));
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void select(
    bool selectRow, bool selectNext, bool inclusive,
    unsigned keyID, ZmRef<const IOBuf> buf,
    unsigned limit, TupleFn tupleFn)
  {
    auto work_ = [
      this, selectRow, selectNext, inclusive,
      keyID, buf = ZuMv(buf), limit, tupleFn = ZuMv(tupleFn)
    ]() mutable {
      ZdbMem::StoreTbl::select(
	selectRow, selectNext, inclusive,
	keyID, ZuMv(buf), limit, [
	  tupleFn = ZuMv(tupleFn)
	](TupleResult result) mutable {
	  auto callback = [
	    tupleFn = ZuMv(tupleFn), result = ZuMv(result)
	  ]() mutable { tupleFn(ZuMv(result)); };
	  deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	});
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void find(unsigned keyID, ZmRef<const IOBuf> buf, RowFn rowFn) {
    auto work_ = [
      this, keyID, buf = ZuMv(buf), rowFn = ZuMv(rowFn)
    ]() mutable {
      ZdbMem::StoreTbl::find(keyID, ZuMv(buf), [
	rowFn = ZuMv(rowFn)
      ](RowResult result) mutable {
	auto callback = [
	  rowFn = ZuMv(rowFn), result = ZuMv(result)
	]() mutable { rowFn(ZuMv(result)); };
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      });
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void recover(UN un, RowFn rowFn) {
    auto work_ = [this, un, rowFn = ZuMv(rowFn)]() mutable {
      ZdbMem::StoreTbl::recover(un, [
	rowFn = ZuMv(rowFn)
      ](RowResult result) mutable {
	auto callback = [
	  rowFn = ZuMv(rowFn), result = ZuMv(result)
	]() mutable { rowFn(ZuMv(result)); };
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      });
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void write(ZmRef<const IOBuf> buf, CommitFn commitFn) {
    auto work_ = [
      this, buf = ZuMv(buf), commitFn = ZuMv(commitFn)
    ]() mutable {
      ZdbMem::StoreTbl::write(ZuMv(buf), [
	commitFn = ZuMv(commitFn)
      ](ZmRef<const IOBuf> buf, CommitResult result) mutable {
	auto callback = [
	  commitFn = ZuMv(commitFn), buf = ZuMv(buf), result = ZuMv(result)
	]() mutable { commitFn(ZuMv(buf), ZuMv(result)); };
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      });
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }
};

// --- mock data store

using Store = ZdbMem::Store_<StoreTbl>;

} // zdbtest

#endif /* ZdbMockStore_HH */
