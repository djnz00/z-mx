//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxT order state management

#ifndef MxTOrderMgr_HH
#define MxTOrderMgr_HH

#ifndef MxTLib_HH
#include <mxt/MxTLib.hh>
#endif

#include <mxbase/MxBase.hh>

#include <mxt/MxTTypes.hh>
#include <mxt/MxTOrder.hh>

// CRTP - implementation must conform to the following interface:
#if 0
struct AppTypes : public MxTAppTypes<AppTypes> {
  using Base = MxTAppTypes<AppTypes>;

  // extend data definitions
  struct NewOrder : public Base::NewOrder {
    // fields
    MxIDString	clOrdID;

    // update
    template <typename Update>
    ZuIs<NewOrder, Update> update(const Update &u) {
      Base::update(u);
      clOrdID.update(u.clOrdID);
    }
    template <typename Update>
    ZuIsNot<NewOrder, Update> update(const Update &u) {
      Base::update(u);
    }

    // print
    template <typename S> void print(S &s) const {
      s << static_cast<const Base::NewOrder &>(*this) <<
	" clOrdID=" << clOrdID;
    }
  };
};
struct App : public MxTOrderMgr<App, AppTypes> {
  // log abnormal OSM transition
  template <typename Update> void abnormal(Order *, const Update &);

  // returns true if async. modify enabled for order
  bool asyncMod(Order *);
  // returns true if async. cancel enabled for order
  bool asyncCxl(Order *);

  // initializes synthetic cancel (allocates clOrdID, etc.)
  void synCancel(Order *);
};
#endif

template <typename App_, typename TxnTypes_>
class MxTOrderMgr : public TxnTypes_ {
public:
  using App = App_;
  using TxnTypes = TxnTypes_;

  MxTImport(TxnTypes);

  ZuInline const App *app() const { return static_cast<const App *>(this); }
  ZuInline App *app() { return static_cast<App *>(this); }

  MxTOrderMgr() {
    { // ensure all static initializers are run once
      AnyTxn txn;

      txn.initNewOrder();
      txn.initOrdered();
      txn.initReject();

      txn.initModify();
      txn.initModSimulated();
      txn.initModified();
      txn.initModReject();
      txn.initModRejectCxl();

      txn.initCancel();
      txn.initCanceled();
      txn.initCxlReject();

      txn.initFill();

      txn.initClosed();
    }
  }

  // order state management

  // emits ack message (Ordered/Modified/Canceled)
  template <typename Out>
  ZuBase<Txn_, Out, bool> ack(Order *order, Out &out) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    auto &ack = order->ack();
    if (ack.eventState != MxTEventState::Sent) return false;
    switch ((int)ack.eventType) {
      case MxTEventType::Ordered: {
	auto &ordered = out.template initOrdered<0>(
	    ack.eventFlags | (1U<<MxTEventFlags::Tx), ack.eventLeg);
	if (newOrder.ack()) ordered.update(newOrder);
	// if (modify.ack()) ordered.update(modify);
	ordered.eventState = MxTEventState::Sent;
	return true;
      }
      case MxTEventType::Modified: {
	auto &modified = out.template initModified<0>(
	    ack.eventFlags | (1U<<MxTEventFlags::Tx), ack.eventLeg);
	if (newOrder.ack()) modified.update(newOrder);
	if (modify.ack()) modified.update(modify);
	modified.eventState = MxTEventState::Sent;
	return true;
      }
      case MxTEventType::Canceled: {
	auto &canceled = out.template initCanceled<0>(
	    ack.eventFlags | (1U<<MxTEventFlags::Tx), ack.eventLeg);
	// if (newOrder.ack()) canceled.update(newOrder);
	// if (modify.ack()) canceled.update(modify);
	if (cancel.ack()) canceled.update(cancel);
	canceled.eventState = MxTEventState::Sent;
	return true;
      }
      default:
	return false;
    }
  }

  // returns either new order or modify, according to the fill target
  Modify &fillModify(Order *order) {
    auto &fill = order->execTxn.template as<Fill>();
    if (ZuUnlikely(fill.ack())) return order->modify();
    return order->newOrder();
  }

private:
  static void newOrderIn__(Order *order) {
    auto &newOrder = order->newOrder();
    unsigned n = newOrder.nLegs();
    for (unsigned i = 0; i < n; i++) {
      OrderLeg &leg = newOrder.legs[i];
      leg.cumQty = 0;
      leg.leavesQty = leg.orderQty;
      leg.cumValue = 0;
    }
  }
  static void closed__(Order *order) {
    auto &newOrder = order->newOrder();
    unsigned n = newOrder.nLegs();
    for (unsigned i = 0; i < n; i++) {
      OrderLeg &leg = newOrder.legs[i];
      leg.leavesQty = 0;
    }
  }
  template <int State>
  static void newOrderIn_(Order *order) {
    auto &newOrder = order->newOrder();
    newOrder.eventState = State;
    newOrder.template rxtx<1, State == MxTEventState::Queued, 0>();
    // no need to set/clr other rx/tx bits since order is new
    // order->modify().template rxtx<0, 0, 0>();
    // order->cancel().template rxtx<0, 0, 0>();
    // order->ack().null();
    // order->exec().null();
    newOrderIn__(order);
  }
  template <int State>
  static void newOrderIn(Order *order) {
    auto &newOrder = order->newOrder();
    newOrder.eventState = State;
    newOrder.template rxtx<1, State == MxTEventState::Queued, 0>();
    order->modify().template rxtx<0, 0, 0>();
    order->cancel().template rxtx<0, 0, 0>();
    order->ack().null();
    order->exec().null();
    newOrderIn__(order);
  }
  static void newOrderOut(Order *order) {
    auto &newOrder = order->newOrder();
    newOrder.eventState = MxTEventState::Queued;
    newOrder.tx_set();
  }
  template <int State>
  static void modifyIn(Order *order) {
    order->newOrder().template rxtx<0, 0, 0>();
    auto &modify = order->modify();
    modify.eventState = State;
    modify.template rxtx<1, State == MxTEventState::Queued, 0>();
    order->cancel().template rxtx<0, 0, 0>();
    order->ack().null();
    order->exec().null();
  }
  static void modifyOut(Order *order) {
    auto &modify = order->modify();
    modify.eventState = MxTEventState::Queued;
    modify.tx_set();
  }
  template <int State>
  static void cancelIn(Order *order) {
    order->newOrder().template rxtx<0, 0, 0>();
    order->modify().template rxtx<0, 0, 0>();
    auto &cancel = order->cancel();
    cancel.eventState = State;
    cancel.template rxtx<1, State == MxTEventState::Queued, 0>();
    order->ack().null();
    order->exec().null();
  }
  static void cancelOut(Order *order) {
    auto &cancel = order->cancel();
    cancel.eventState = MxTEventState::Queued;
    cancel.tx_set();
  }

  template <int Type, int State, bool NewOrder, bool Modify, bool Cancel,
    bool Unsolicited, bool Synthetic, bool Pending>
  static void ackIn(Order *order, int leg = 0) {
    order->newOrder().template rxtx<0, 0, NewOrder>();
    order->modify().template rxtx<0, 0, Modify>();
    order->cancel().template rxtx<0, 0, Cancel>();
    auto &ack = order->ack();
    ack.eventType = Type;
    ack.eventState = State;
    ack.eventFlags =
      (1U<<MxTEventFlags::Rx) |
      (((unsigned)(State == MxTEventState::Sent))<<MxTEventFlags::Tx) |
      (((unsigned)Unsolicited)<<MxTEventFlags::Unsolicited) |
      (((unsigned)Synthetic)<<MxTEventFlags::Synthetic) |
      (((unsigned)Pending)<<MxTEventFlags::Pending);
    ack.eventLeg = leg;
    order->exec().null();
  }
  template <int Type, int State, bool NewOrder, bool Modify, bool Cancel,
    bool Unsolicited, bool Synthetic, bool Pending>
  static void ackOut(Order *order, int leg = 0) {
    if constexpr (NewOrder) order->newOrder().ack_set();
    if constexpr (Modify) order->modify().ack_set();
    if constexpr (Cancel) order->cancel().ack_set();
    auto &ack = order->ack();
    ack.eventType = Type;
    ack.eventState = State;
    ack.eventFlags =
      (1U<<MxTEventFlags::Rx) |
      (((unsigned)(State == MxTEventState::Sent))<<MxTEventFlags::Tx) |
      (((unsigned)Unsolicited)<<MxTEventFlags::Unsolicited) |
      (((unsigned)Synthetic)<<MxTEventFlags::Synthetic) |
      (((unsigned)Pending)<<MxTEventFlags::Pending);
    ack.eventLeg = leg;
  }

  template <int State>
  static void execIn(Order *order) {
    order->newOrder().template rxtx<0, 0, 0>();
    order->modify().template rxtx<0, 0, 0>();
    order->cancel().template rxtx<0, 0, 0>();
    order->ack().null();
    auto &exec = order->exec();
    exec.eventState = State;
    exec.template rxtx<1, State == MxTEventState::Sent, 0>();
  }
  template <int State = MxTEventState::Sent>
  static void execOut(Order *order) {
    auto &exec = order->exec();
    exec.eventState = State;
    if constexpr (State == MxTEventState::Sent) exec.tx_set();
  }

  // continuation helpers
  template <typename In> struct Next {
    typedef uintptr_t (*Fn)(App *, Order *, In &);
  };
  template <typename In, typename Fn>
  static uintptr_t nextFn(Fn &&fn) {
    return reinterpret_cast<uintptr_t>(
	static_cast<typename Next<In>::Fn>(ZuFwd<Fn>(fn)));
  }
public:
  template <typename In>
  static uintptr_t nextInvoke(
      uintptr_t fn, App *app, Order *order, In &in) {
    return reinterpret_cast<typename Next<In>::Fn>(fn)(app, order, in);
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  newOrder(Order *order, In &in) {
    order->orderTxn = in.template data<NewOrder>();
    newOrderIn_<MxTEventState::Queued>(order);
    return 0;
  }

private:
  template <bool Unsolicited, typename In>
  static void ordered_(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    newOrder.eventState = MxTEventState::Acknowledged;
    newOrder.update(in.template as<Ordered>());
    unsigned leg = in.template as<Event>().eventLeg;
    if (ZuUnlikely(newOrder.modifyNew())) {
      newOrder.modifyNew_clr();
      ackIn<MxTEventType::Modified, MxTEventState::Sent,
	1, 0, 0, Unsolicited, 0, 0>(order, leg);
    } else
      ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	1, 0, 0, Unsolicited, 0, 0>(order, leg);
  }

public:
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  ordered(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuLikely(!newOrder.modifyCxl() &&
	  MxTEventState::matchHQS(newOrder.eventState))) {
      // usual case - solicited
      ordered_<0>(order, in); // solicited
      if (ZuUnlikely(MxTEventState::matchD(cancel.eventState)))
	cancelOut(order);
      else if (ZuUnlikely(MxTEventState::matchD(modify.eventState)))
	modifyOut(order);
      return 0;
    }
    if (ZuUnlikely(MxTEventState::matchAC(newOrder.eventState))) {
      // unsolicited - order already acknowledged / closed - sink ack
      auto &ordered = in.template as<Ordered>();
      newOrder.update(ordered);
      ackIn<MxTEventType::Ordered, MxTEventState::Received,
	1, 0, 0, 1, 0, 0>(order, ordered.eventLeg); // OmcUsp
      return 0;
    }
    if (ZuLikely(newOrder.modifyCxl() &&
	  MxTEventState::matchS(newOrder.eventState) &&
	  MxTEventState::matchHD(modify.eventState) &&
	  MxTEventState::matchDQSPX(cancel.eventState))) {
      // solicited - synthetic cancel/replace in process
      ordered_<0>(order, in); // solicited
      if (ZuUnlikely(MxTEventState::matchDX(cancel.eventState)))
	cancelOut(order);
      return 0;
    }
    // abnormal
    app()->abnormal(order, in.template as<Ordered>());
    ordered_<1>(order, in); // unsolicited
    return 0;
  }

private:
  // synthetic modify reject
  static void modReject_(Order *order, int reason) {
    auto &modify = order->modify();
    modify.eventState = MxTEventState::Rejected;
    auto &modReject = order->execTxn.template initModReject<1>();
    modReject.rejReason = reason;
    modReject.update(modify);
  }
  // synthetic cancel reject
  static void cxlReject_(Order *order, int reason) {
    auto &cancel = order->cancel();
    cancel.eventState = MxTEventState::Rejected;
    auto &cxlReject = order->execTxn.template initCxlReject<1>();
    cxlReject.rejReason = reason;
    cxlReject.update(cancel);
  }

public:
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  reject(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    auto next = [](App *, Order *order, In &in) -> uintptr_t {
      auto &newOrder = order->newOrder();
      bool unsolicited = false;
      if (ZuUnlikely(MxTEventState::matchAX(newOrder.eventState)))
	unsolicited = true;
      if (!MxTEventState::matchXC(newOrder.eventState))
	newOrder.eventState = MxTEventState::Rejected;
      newOrder.modifyNew_clr();
      {
	order->execTxn = in.template data<Reject>();
	execIn<MxTEventState::Sent>(order);
	auto &reject = order->execTxn.template as<Reject>();
	newOrder.update(reject);
	reject.update(newOrder);
	if (ZuUnlikely(unsolicited)) reject.unsolicited_set();
      }
      closed__(order);
      return 0;
    };
    if (ZuLikely(!newOrder.modifyCxl() &&
	  MxTEventState::matchHQSAX(newOrder.eventState))) {
      // usual case
      if (ZuUnlikely(MxTEventState::matchDQ(cancel.eventState))) {
	cxlReject_(order, MxTRejReason::OrderClosed);
	execIn<MxTEventState::Sent>(order);
	return nextFn<In>(next);
      } else if (ZuUnlikely(MxTEventState::matchDQ(modify.eventState))) {
	modReject_(order, MxTRejReason::OrderClosed);
	execIn<MxTEventState::Sent>(order);
	return nextFn<In>(next);
      } else {
	return next(app(), order, in);
      }
    }
    if (ZuLikely(newOrder.modifyCxl() &&
	  MxTEventState::matchSA(newOrder.eventState) &&
	  MxTEventState::matchHD(modify.eventState) &&
	  MxTEventState::matchDQSPX(cancel.eventState))) {
      // simulated modify in process
      if (ZuUnlikely(MxTEventState::matchDQ(cancel.eventState)))
	cancel.null();
      modReject_(order, MxTRejReason::OrderClosed);
      execIn<MxTEventState::Sent>(order);
      return nextFn<In>(next);
    }
    // abnormal
    in.template as<Reject>().unsolicited_set();
    app()->abnormal(order, in.template as<Reject>());
    return next(app(), order, in);
  }

  MxEnum filterModify(Order *order) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuUnlikely(MxTEventState::matchXC(newOrder.eventState)))
      return MxTRejReason::OrderClosed;
    if (ZuUnlikely(MxTEventState::matchDQSP(cancel.eventState))) {
      if (!newOrder.modifyCxl())
	return MxTRejReason::CancelPending;
      else
	return MxTRejReason::ModifyPending;
    }
    if (ZuUnlikely(MxTEventState::matchDQSP(modify.eventState)))
      return MxTRejReason::ModifyPending;
    if (ZuUnlikely(
	  !MxTEventState::matchA(newOrder.eventState) &&
	  MxTEventState::matchQ(modify.eventState)))
      return MxTRejReason::ModifyPending;
    return MxTRejReason::OK;
  }

  static uintptr_t modify_filled(Order *order) {
    // modify-on-queue reduced qty so that order is filled
    auto &newOrder = order->newOrder();
    newOrder.modifyNew_clr();
    ackIn<MxTEventType::Modified, MxTEventState::Sent,
      1, 0, 0, 0, 1, 0>(order); // OmcuSp
    return 0;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  modify(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuLikely(
	  MxTEventState::matchUAX(modify.eventState) &&
	  MxTEventState::matchUAX(cancel.eventState))) {
      if (ZuLikely(
	    MxTEventState::matchSA(newOrder.eventState))) {
	// usual case
	order->modifyTxn = in.template data<Modify>();
	if (ZuUnlikely(MxTEventState::matchS(newOrder.eventState) &&
	      !app()->asyncMod(order)))
	  modifyIn<MxTEventState::Deferred>(order); // async. modify disabled
	else
	  modifyIn<MxTEventState::Queued>(order);
	// no need for modifyOut()
	return 0;
      }
      if (MxTEventState::matchHT(newOrder.eventState)) {
	// new order held/queued, modify-on-queue
	newOrder.eventState = MxTEventState::Acknowledged;
	if (!newOrder.modifyNew()) {
	  // new order is original, not following a previous modify-on-queue
	  ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	    1, 0, 0, 0, 1, 1>(order); // OmcuSP
	  auto next = [](App *, Order *order, In &in) -> uintptr_t {
	    auto &newOrder = order->newOrder();
	    newOrder.modifyNew_set();
	    newOrder.update(in.template as<Modify>());
	    if (newOrder.filled()) return modify_filled(order);
	    newOrderIn<MxTEventState::Queued>(order);
	    return 0;
	  };
	  return nextFn<In>(next);
	}
	// new order is synthetic, following a previous modify-on-queue
	ackIn<MxTEventType::Modified, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 1>(order); // OmcuSP
	auto next = [](App *, Order *order, In &in) -> uintptr_t {
	  auto &newOrder = order->newOrder();
	  newOrder.update(in.template as<Modify>());
	  if (newOrder.filled()) return modify_filled(order);
	  newOrderIn<MxTEventState::Queued>(order);
	  return 0;
	};
	return nextFn<In>(next);
      }
    }
    if (ZuLikely(
	  MxTEventState::matchA(newOrder.eventState) &&
	  MxTEventState::matchHDT(modify.eventState))) {
      // modify-on-queue
      modify.eventState = MxTEventState::Acknowledged;
      ackIn<MxTEventType::Modified, MxTEventState::Sent,
	0, 1, 0, 0, 1, 1>(order); // oMcuSP
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	auto &newOrder = order->newOrder();
	auto &modify = order->modify();
	auto &cancel = order->cancel();
	modify.update(in.template as<Modify>());
	if (newOrder.modifyCxl()) {
	  // synthetic cancel/replace in process
	  if (MxTEventState::matchSP(cancel.eventState)) {
	    // cancel sent
	    modifyIn<MxTEventState::Deferred>(order);
	    return 0;
	  }
	  cancel.null();
	  newOrder.modifyCxl_clr();
	}
	modifyIn<MxTEventState::Queued>(order);
	return 0;
      };
      return nextFn<In>(next);
    }
    // should never happen; somehow this point was reached despite
    // filterModify() previously returning OK
    return 0;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  modSimulated(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuLikely(
	  MxTEventState::matchUAX(modify.eventState) &&
	  MxTEventState::matchUAX(cancel.eventState))) {
      if (ZuLikely(
	    MxTEventState::matchSA(newOrder.eventState))) {
	// usual case
	newOrder.modifyCxl_set();
	order->modifyTxn = in.template data<Modify>();
	modifyIn<MxTEventState::Deferred>(order);
	order->cancelTxn.template initCancel<1>();
	app()->synCancel(order);
	cancel.update(newOrder);
	if (ZuUnlikely(MxTEventState::matchS(newOrder.eventState) &&
	      !app()->asyncCxl(order)))
	  cancel.eventState = MxTEventState::Deferred;
	else
	  cancelOut(order);
	return 0;
      }
      if (MxTEventState::matchHT(newOrder.eventState)) {
	// new order held/queued, modify-on-queue
	newOrder.eventState = MxTEventState::Acknowledged;
	if (!newOrder.modifyNew()) {
	  // new order is original, not following a previous modify-on-queue
	  newOrder.modifyNew_set();
	  ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	    1, 0, 0, 0, 1, 1>(order); // OmcuSP
	  auto next = [](App *, Order *order, In &in) -> uintptr_t {
	    auto &newOrder = order->newOrder();
	    newOrder.update(in.template as<Modify>());
	    if (newOrder.filled()) return modify_filled(order);
	    newOrderIn<MxTEventState::Queued>(order);
	    return 0;
	  };
	  return nextFn<In>(next);
	}
	// new order is modify, following a previous modify-on-queue
	ackIn<MxTEventType::Modified, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 1>(order); // OmcuSP
	auto next = [](App *, Order *order, In &in) -> uintptr_t {
	  auto &newOrder = order->newOrder();
	  newOrder.update(in.template as<Modify>());
	  if (newOrder.filled()) return modify_filled(order);
	  newOrderIn<MxTEventState::Queued>(order);
	  return 0;
	};
	return nextFn<In>(next);
      }
    }
    if (ZuLikely(
	  MxTEventState::matchA(newOrder.eventState) &&
	  MxTEventState::matchHDT(modify.eventState))) {
      // modify held/deferred/queued, modify-on-queue
      modify.eventState = MxTEventState::Acknowledged;
      ackIn<MxTEventType::Modified, MxTEventState::Sent,
	0, 1, 0, 0, 1, 1>(order); // oMcuSP
      auto next = [](App *app, Order *order, In &in) -> uintptr_t {
	auto &newOrder = order->newOrder();
	auto &modify = order->modify();
	auto &cancel = order->cancel();
	modify.update(in.template as<Modify>());
	modify.eventState = MxTEventState::Deferred;
	if (newOrder.modifyCxl()) {
	  // simulated modify in process
	  if (MxTEventState::matchQSP(cancel.eventState))
	    return 0; // cancel already sent
	} else
	  newOrder.modifyCxl_set();
	order->cancelTxn.template initCancel<1>();
	app->synCancel(order);
	cancel.update(newOrder);
	cancelIn<MxTEventState::Queued>(order);
	return 0;
      };
      return nextFn<In>(next);
    }
    // should never happen; somehow this point was reached despite
    // filterModify() previously returning OK
    return 0;
  }

private:
  template <bool Unsolicited, typename In>
  static void modified_(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if constexpr (Unsolicited) {
      order->modifyTxn.template initModify<1>();
      modify.update(newOrder);
    }
    modify.update(in.template as<Modified>());
    if (newOrder.pendingFill(modify)) {
      // modify ack is pending a fill - defer it and resume in fill()
      modify.eventState = MxTEventState::PendingFill;
      ackIn<MxTEventType::Modified, MxTEventState::PendingFill,
	0, 1, 0, Unsolicited, 0, 0>(order); // oMcUsp
      modify.ackFlags = order->ack().eventFlags;
    } else {
      // modify ack can be processed immediately
      modify.eventState = MxTEventState::Acknowledged;
      newOrder.update(modify);
      ackIn<MxTEventType::Modified, MxTEventState::Sent,
	1, 0, 0, Unsolicited, 0, 0>(order); // OmcUsp
    }
    if (MxTEventState::matchD(cancel.eventState)) // deferred cancel
      cancelOut(order);
  }

public:
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  modified(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (MxTEventState::matchSA(newOrder.eventState) &&
	MxTEventState::matchDQS(modify.eventState)) {
      // usual case
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	modified_<0>(order, in); // solicited
	return 0;
      };
      // acknowledge any sent new order
      if (MxTEventState::matchS(newOrder.eventState)) {
	newOrder.eventState = MxTEventState::Acknowledged;
	ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 0>(order); // OmcuSp
	return nextFn<In>(next);
      }
      return next(app(), order, in);
    }
    // unsolicited (i.e. unilaterally restated by broker/market)
    {
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	modified_<1>(order, in); // unsolicited
	return 0;
      };
      // acknowledge any pending new order (unless simulating modify)
      if (!newOrder.modifyCxl() &&
	  MxTEventState::matchHQS(newOrder.eventState)) {
	newOrder.eventState = MxTEventState::Acknowledged;
	ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 0>(order); // OmcuSp
	return nextFn<In>(next);
      }
      if (MxTEventState::matchAC(newOrder.eventState))
	return next(app(), order, in); // usual case
    }
    if (newOrder.modifyCxl() &&
	MxTEventState::matchS(newOrder.eventState) &&
	MxTEventState::matchHD(modify.eventState) &&
	MxTEventState::matchDQSPX(cancel.eventState)) {
      // unacknowledged order and simulated modify in process
      newOrder.eventState = MxTEventState::Acknowledged;
      ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	1, 0, 0, 0, 1, 0>(order); // OmcuSp
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	modified_<1>(order, in);
	// send any cancel needed for cancel/replace
	// deferred cancels are taken care of by modified_()
	auto &cancel = order->cancel();
	if (MxTEventState::matchX(cancel.eventState))
	  cancelOut(order);
	return 0;
      };
      return nextFn<In>(next);
    }
    // abnormal
    app()->abnormal(order, in.template as<Modified>());
    newOrder.update(in.template as<Modified>());
    ackIn<MxTEventType::Modified, MxTEventState::Sent,
      1, 0, 0, 1, 0, 0>(order); // OmcUsp
    return 0;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  modReject(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    order->execTxn = in.template data<ModReject>();
    auto &modReject = order->execTxn.template as<ModReject>();
    if (ZuLikely(
	  MxTEventState::matchSA(newOrder.eventState) &&
	  MxTEventState::matchDQSP(modify.eventState))) {
      // usual case
      execIn<MxTEventState::Sent>(order);
      modReject.update(modify);
      modify.eventState = MxTEventState::Rejected;
      if (MxTEventState::matchD(cancel.eventState))
	cancelOut(order);
      return 0;
    }
    if (MxTEventState::matchXC(newOrder.eventState) &&
	MxTEventState::matchSP(modify.eventState)) {
      // late modify reject - order already closed
      execIn<MxTEventState::Received>(order);
      modReject.update(modify);
      modify.eventState = MxTEventState::Rejected;
      return 0;
    }
    app()->abnormal(order, in.template as<ModReject>());
    execIn<MxTEventState::Received>(order);
    return 0;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  modRejectCxl(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    order->execTxn = in.template data<ModReject>();
    auto &modReject = order->execTxn.template as<ModReject>();
    if (ZuLikely(
	  MxTEventState::matchSA(newOrder.eventState) &&
	  MxTEventState::matchDQSP(modify.eventState))) {
      // usual case
      execIn<MxTEventState::Sent>(order);
      newOrder.modifyCxl_clr();
      modReject.update(modify);
      modify.eventState = MxTEventState::Rejected;
      // do not need to cancel if order already filled
      if (ZuLikely(!newOrder.filled())) {
	if (MxTEventState::matchQSP(cancel.eventState)) return 0;
	if (!MxTEventState::matchD(cancel.eventState)) {
	  order->cancelTxn.template initCancel<1>();
	  app()->synCancel(order);
	  cancel.update(newOrder);
	}
	if (ZuUnlikely(MxTEventState::matchS(newOrder.eventState) &&
	      !app()->asyncCxl(order)))
	  cancel.eventState = MxTEventState::Deferred;
	else
	  cancelOut(order);
      }
      return 0;
    }
    if (MxTEventState::matchXC(newOrder.eventState) &&
	MxTEventState::matchSP(modify.eventState)) {
      // late modify reject - order already closed
      execIn<MxTEventState::Received>(order);
      modReject.update(modify);
      modify.eventState = MxTEventState::Rejected;
      return 0;
    }
    app()->abnormal(order, in.template as<ModReject>());
    execIn<MxTEventState::Received>(order);
  }

  MxEnum filterCancel(Order *order) {
    auto &newOrder = order->newOrder();
    auto &cancel = order->cancel();
    if (ZuUnlikely(MxTEventState::matchXC(newOrder.eventState)))
      return MxTRejReason::OrderClosed;
    if (ZuUnlikely(MxTEventState::matchDQSP(cancel.eventState))) {
      if (!newOrder.modifyCxl())
	return MxTRejReason::CancelPending;
      else {
	newOrder.modifyCxl_clr();
	return MxTRejReason::CancelPending;
      }
    }
    return MxTRejReason::OK;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  cancel(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuLikely(MxTEventState::matchSA(newOrder.eventState))) {
      // usual case
      newOrder.modifyCxl_clr();
      order->cancelTxn = in.template data<Cancel>();
      if (ZuUnlikely(
	    (MxTEventState::matchS(newOrder.eventState) ||
	     MxTEventState::matchS(modify.eventState)) &&
	    !app()->asyncCxl(order)))
	cancelIn<MxTEventState::Deferred>(order); // async. cancel disabled
      else
	cancelIn<MxTEventState::Queued>(order);
      cancel.update(newOrder);
      // synthetically reject any held/deferred/queued modify
      if (ZuUnlikely(MxTEventState::matchHDT(modify.eventState))) {
	modReject_(order, MxTRejReason::OrderClosed);
	execOut(order);
      }
      return 0;
    }
    // close any held/queued new order
    if (MxTEventState::matchHT(newOrder.eventState)) {
      order->cancelTxn = in.template data<Cancel>();
      newOrder.modifyNew_clr();
      // new order held/queued, cancel-on-queue
      newOrder.eventState = MxTEventState::Closed;
      // inform client that order is canceled
      ackIn<MxTEventType::Canceled, MxTEventState::Sent,
	1, 0, 0, 0, 1, 0>(order); // OmcuSp
      return 0;
    }
    // should never happen; somehow this point was reached despite
    // filterCancel() previously returning OK when app called cancelIn()
    return 0;
  }

private:
  template <bool Unsolicited, typename In>
  static void canceled_(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &cancel = order->cancel();
    if constexpr (Unsolicited) {
      order->cancelTxn.template initCancel<1>();
      cancel.update(newOrder);
    }
    cancel.update(in.template as<Canceled>());
    if (newOrder.pendingFill(cancel)) {
      cancel.eventState = MxTEventState::PendingFill;
      ackIn<MxTEventType::Canceled, MxTEventState::PendingFill,
	0, 0, 1, Unsolicited, 0, 0>(order); // omCUsp
      cancel.ackFlags = order->ack().eventFlags;
    } else {
      cancel.eventState = MxTEventState::Acknowledged;
      newOrder.eventState = MxTEventState::Closed;
      newOrder.update(cancel);
      ackIn<MxTEventType::Canceled, MxTEventState::Sent,
	1, 0, 0, Unsolicited, 0, 0>(order); // OmcUsp
    }
  }

  template <bool Unsolicited, typename In>
  static void modCanceled_(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if constexpr (Unsolicited) {
      order->cancelTxn.template initCancel<1>();
      cancel.update(newOrder);
    }
    cancel.update(in.template as<Canceled>());
    if (newOrder.pendingFill(cancel)) {
      cancel.eventState = MxTEventState::PendingFill;
      ackIn<MxTEventType::Modified, MxTEventState::PendingFill,
	0, 1, 0, 0, 0, 0>(order); // oMcusp
      cancel.ackFlags = order->ack().eventFlags;
    } else {
      newOrder.update(modify);
      if (MxTEventState::matchH(modify.eventState)) {
	newOrder.eventState = MxTEventState::Held;
	modify.null();
      }
      newOrder.template mc<1, 0>();
      cancel.eventState = MxTEventState::Acknowledged;
      ackIn<MxTEventType::Modified, MxTEventState::Sent,
	1, 0, 0, 0, 0, 0>(order); // Omcusp
    }
  }

public:
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  canceled(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    if (ZuLikely(!newOrder.modifyCxl() &&
	  MxTEventState::matchHQSA(newOrder.eventState))) {
      // usual case
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	auto &modify = order->modify();
	auto &cancel = order->cancel();
	if (ZuLikely(MxTEventState::matchDQS(cancel.eventState)))
	  canceled_<0>(order, in); // solicited
	else
	  canceled_<1>(order, in); // unsolicited
	// reject any pending modify
	if (MxTEventState::matchDQ(modify.eventState)) {
	  modReject_(order, MxTRejReason::OrderClosed);
	  execOut(order);
	}
	closed__(order);
	return 0;
      };
      // acknowledge any pending new order
      if (MxTEventState::matchHQS(newOrder.eventState)) {
	newOrder.eventState = MxTEventState::Acknowledged;
	ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 0>(order); // OmcuSp
	return nextFn<In>(next);
      }
      return next(app(), order, in);
    }
    if (ZuUnlikely(MxTEventState::matchXC(newOrder.eventState))) {
      // late - order already closed
      if (MxTEventState::matchDQS(cancel.eventState)) {
	// solicited
	cancel.eventState = MxTEventState::Acknowledged;
	ackIn<MxTEventType::Canceled, MxTEventState::Received,
	  1, 0, 0, 0, 0, 0>(order); // Omcusp
      } else {
	// unsolicited
	ackIn<MxTEventType::Canceled, MxTEventState::Received,
	  1, 0, 0, 1, 0, 0>(order); // OmcUsp
      }
      return 0;
    }
    if (ZuLikely(newOrder.modifyCxl() &&
	  MxTEventState::matchSA(newOrder.eventState) &&
	  MxTEventState::matchHD(modify.eventState))) {
      // simulated modify held or deferred
      auto next = [](App *, Order *order, In &in) -> uintptr_t {
	auto &newOrder = order->newOrder();
	auto &cancel = order->cancel();
	// apply simulated modify to order
	if (MxTEventState::matchDQS(cancel.eventState))
	  modCanceled_<0>(order, in); // solicited
	else
	  modCanceled_<1>(order, in); // unsolicited
	if (newOrder.filled()) {
	  // modify may have reduced qty so that order is now filled
	  newOrder.modifyNew_clr();
	  newOrder.eventState = MxTEventState::Acknowledged;
	} else
	  newOrderOut(order);
	return 0;
      };
      // acknowledge any pending new order
      if (MxTEventState::matchS(newOrder.eventState)) {
	newOrder.eventState = MxTEventState::Acknowledged;
	ackIn<MxTEventType::Ordered, MxTEventState::Sent,
	  1, 0, 0, 0, 1, 0>(order); // OmcuSp
	return nextFn<In>(next);
      }
      return next(app(), order, in);
    }
    // abnormal
    in.template as<Event>().unsolicited_set();
    app()->abnormal(order, in.template as<Canceled>());
    if (!MxTEventState::matchXC(newOrder.eventState))
      newOrder.eventState = MxTEventState::Closed;
    ackIn<MxTEventType::Canceled, MxTEventState::Received,
      1, 0, 0, 1, 0, 0>(order); // OmcUsp
    closed__(order);
    return 0;
  }

  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  cxlReject(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    // auto &modify = order->modify();
    auto &cancel = order->cancel();
    order->execTxn = in.template data<CxlReject>();
    auto &cxlReject = order->execTxn.template as<CxlReject>();
    if (ZuLikely(MxTEventState::matchDQS(cancel.eventState))) {
      // usual case
      cancel.eventState = MxTEventState::Rejected;
      if (newOrder.modifyCxl())
	execIn<MxTEventState::Received>(order); // simulated modify
      else
	execIn<MxTEventState::Sent>(order);
      cxlReject.update(cancel);
      return 0;
    }
    // abnormal
    in.template as<Event>().unsolicited_set();
    cxlReject.unsolicited_set();
    app()->abnormal(order, in.template as<CxlReject>());
    execIn<MxTEventState::Received>(order);
    return 0;
  }

private:
  template <typename In>
  static void fillOrdered_(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    newOrder.eventState = MxTEventState::Acknowledged;
    unsigned leg = in.template as<Event>().eventLeg;
    if (ZuUnlikely(newOrder.modifyNew())) {
      newOrder.modifyNew_clr();
      ackOut<MxTEventType::Modified, MxTEventState::Sent,
	1, 0, 0, 0, 1, 0>(order, leg); // OmcuSp
    } else {
      ackOut<MxTEventType::Ordered, MxTEventState::Sent,
	1, 0, 0, 0, 1, 0>(order, leg); // OmcuSp
    }
  }

public:
  // apply fill to order
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  fill(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    auto &modify = order->modify();
    auto &cancel = order->cancel();
    // always process fills, regardless of order status
    order->execTxn = in.template data<Fill>();
    execIn<MxTEventState::Sent>(order);
    auto &fill = order->execTxn.template as<Fill>();
    if (ZuLikely(
	  MxTEventState::matchAC(newOrder.eventState)))
      // order already acknowledged / closed
      goto applyFill;
    if (ZuLikely(!newOrder.modifyCxl() &&
	  MxTEventState::matchHQS(newOrder.eventState))) {
      // order unacknowledged, possible pending cancel or modify
      fillOrdered_(order, in); // solicited
      if (ZuUnlikely(MxTEventState::matchD(cancel.eventState)))
	cancelOut(order);
      else if (ZuUnlikely(MxTEventState::matchD(modify.eventState)))
	modifyOut(order);
      goto applyFill;
    }
    if (ZuLikely(newOrder.modifyCxl() &&
	  MxTEventState::matchS(newOrder.eventState) &&
	  MxTEventState::matchHD(modify.eventState) &&
	  MxTEventState::matchDQSPX(cancel.eventState))) {
      // order unacknowledged, simulated modify held or deferred
      fillOrdered_(order, in); // solicited
      if (ZuUnlikely(MxTEventState::matchDX(cancel.eventState)))
	cancelOut(order);
      goto applyFill;
    }
applyFill:
    bool fillModify = false;
    OrderLeg *leg;
    ModifyLeg *modLeg;
    {
      // apply fill to order leg
#if _NLegs > 1
      unsigned leg_ = !*fill.eventLeg ? 0U : (unsigned)fill.eventLeg;
      if (leg_ >= _NLegs) {
	// fill has been forwarded to client, but cannot be applied to order
	// due to invalid leg
	app()->abnormal(order, in.template as<Fill>());
	return;
      }
      leg = &newOrder.legs[leg_];
#else
      leg = &newOrder.legs[0];
#endif
      leg->cumQty += fill.lastQty;
      if (MxTEventState::matchHDQS(modify.eventState)) {
#if _NLegs > 1
	modLeg = &modify.legs[leg_];
#else
	modLeg = &modify.legs[0];
#endif
	if (*modLeg->orderQty && leg->cumQty > leg->orderQty) {
	  if (leg->cumQty <= modLeg->orderQty)
	    // modify qty up, fill implies modified
	    fillModify = true;
	} else if (*modLeg->px) {
	  if (leg->side == MxSide::Buy) {
	    if (fill.lastPx > leg->px && fill.lastPx <= modLeg->px)
	      // modify bid price up, fill implies modified
	      fillModify = true;
	  } else {
	    if (fill.lastPx < leg->px && fill.lastPx >= modLeg->px)
	      // modify ask price down, fill implies modified
	      fillModify = true;
	  }
	}
      }
      leg->updateLeavesQty();
      leg->cumValue +=
	(MxValNDP{fill.lastPx, fill.pxNDP} *
	 MxValNDP{fill.lastQty, fill.qtyNDP}).value;
    }
    if (ZuUnlikely(fillModify)) {
      modLeg->update(fill);
      modify.update(fill);
      modify.eventState = MxTEventState::Acknowledged;
      newOrder.update(modify);
      fill.update(newOrder);
      ackOut<MxTEventType::Modified, MxTEventState::Sent,
	1, 0, 0, 0, 1, 1>(order); // OmcuSP
    } else {
      leg->update(fill);
      newOrder.update(fill);
      fill.update(newOrder);
      if (MxTEventState::matchP(modify.eventState) &&
	  !newOrder.pendingFill(modify)) {
	// modify ack pending
	modify.eventState = MxTEventState::Acknowledged;
	newOrder.update(modify);
	ackOut<MxTEventType::Modified, MxTEventState::Sent,
	  1, 0, 0, 0, 0, 0>(order); // Omcusp
      }
      if (MxTEventState::matchP(cancel.eventState) &&
	  !newOrder.pendingFill(cancel)) {
	// cancel ack pending - (simulated modify cancels are never deferred)
	cancel.eventState = MxTEventState::Acknowledged;
	newOrder.eventState = MxTEventState::Closed;
	newOrder.update(cancel);
	ackOut<MxTEventType::Canceled, MxTEventState::Sent,
	  1, 0, 0, 0, 0, 0>(order); // Omcusp
      }
    }
    return 0;
  }

  // closes the order
  template <typename In>
  ZuBase<Txn_, In, uintptr_t>
  closed(Order *order, In &in) {
    auto &newOrder = order->newOrder();
    order->execTxn = in.template data<Closed>();
    execIn<MxTEventState::Sent>(order);
    auto &closed = order->execTxn.template as<Closed>();
    newOrder.eventState = MxTEventState::Closed;
    newOrder.update(closed);
    closed.update(newOrder);
    closed__(order);
    return 0;
  }
};

#endif /* MxTOrderMgr_HH */
