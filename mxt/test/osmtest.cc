#include <stdio.h>
#include <iostream>

// #define MxT_NLegs 4

#include <mxt/MxTOrder.hh>
#include <mxt/MxTOrderMgr.hh>

struct AppTypes : public MxTAppTypes<AppTypes> {
  using Base = MxTAppTypes<AppTypes>;

  struct AppCxlLeg {
    template <typename S> void print(S &) const { }
    template <typename Update> void update(const Update &) { }
  };
  struct AppModLeg : public AppCxlLeg { };
  struct AppOrderLeg : public AppModLeg {
    MxIDString	instrument;	// instrument symbol

    template <typename S> void print(S &s) const {
      AppModLeg::print(s);
      s << "instrument=" << instrument;
    }
  };

  struct AppMsgID {
    MxIDString	clOrdID;
    MxIDString	origClOrdID;
    MxIDString	orderID;
    MxIDString	execID;

    template <typename U> void updateClOrdID(const U &u) {
      origClOrdID = clOrdID;
      clOrdID = u.clOrdID;
    }
    template <typename U> void updateOrderID(const U &u) {
      orderID = u.orderID;
    }
    template <typename U> void updateExecID(const U &u) {
      execID = u.execID;
    }

    template <typename S> void print(S &s) const {
      s << "clOrdID=" << clOrdID
	<< " origClOrdID=" << origClOrdID
	<< " orderID=" << orderID
	<< " execID=" << execID;
    }
  };

  struct AppExecID {
    MxIDString	execID;

    template <typename S> void print(S &s) const {
      s << "execID=" << execID;
    }
  };

  struct AppRequest;
  struct AppAck;
  struct AppExec;

  struct AppRequest : public AppMsgID {
    template <typename Update>
    ZuIs<AppAck, Update> update(const Update &u) {
      updateOrderID(u);
      updateExecID(u);
    }
    template <typename Update>
    ZuIs<AppExec, Update> update(const Update &u) {
      updateExecID(u);
    }
    template <typename Update>
    ZuIfT<
      !ZuInspect<AppAck, Update>::Is &&
      !ZuInspect<AppExec, Update>::Is> update(const Update &) { }
  };

  struct AppAck : public AppMsgID {
    template <typename Update>
    ZuIs<AppRequest, Update> update(const Update &u) {
      // updateClOrdID(u);
      updateOrderID(u);
      updateExecID(u);
    }
    template <typename Update>
    ZuIsNot<AppRequest, Update> update(const Update &) { }
  };

  struct AppOrdered : public AppAck { };
  struct AppCanceled : public AppAck { };
  struct AppModified : public AppAck { };

  struct AppExec : public AppMsgID {
    template <typename Update>
    void update(const Update &) { }
  };

  struct AppCancel : public AppRequest {
    template <typename U>
    ZuIfT<
	ZuInspect<AppCancel, U>::Is> update(const U &u) {
      AppMsgID::updateClOrdID(u);
      AppRequest::update(u);
    }
    template <typename U>
    ZuIfT<
	!ZuInspect<AppCancel, U>::Is> update(const U &u) {
      AppRequest::update(u);
    }
  };
  struct AppModify : public AppRequest {
    template <typename U>
    ZuIfT<
	ZuInspect<AppModify, U>::Is ||
	ZuInspect<AppCancel, U>::Is ||
	ZuInspect<AppCanceled, U>::Is> update(const U &u) {
      AppMsgID::updateClOrdID(u);
      AppRequest::update(u);
    }
    template <typename U>
    ZuIfT<
	!ZuInspect<AppModify, U>::Is &&
	!ZuInspect<AppCancel, U>::Is &&
	!ZuInspect<AppCanceled, U>::Is> update(const U &u) {
      AppRequest::update(u);
    }
  };
  struct AppNewOrder : public AppRequest {
    MxIDString	account;	// account identifier

    template <typename U>
    ZuIfT<
	ZuInspect<AppNewOrder, U>::Is ||
	ZuInspect<AppModify, U>::Is ||
	ZuInspect<AppModified, U>::Is ||
	ZuInspect<AppCancel, U>::Is ||
	ZuInspect<AppCanceled, U>::Is> update(const U &u) {
      AppMsgID::updateClOrdID(u);
      AppRequest::update(u);
    }
    template <typename U>
    ZuIfT<
	!ZuInspect<AppNewOrder, U>::Is &&
	!ZuInspect<AppModify, U>::Is &&
	!ZuInspect<AppModified, U>::Is &&
	!ZuInspect<AppCancel, U>::Is &&
	!ZuInspect<AppCanceled, U>::Is> update(const U &u) {
      AppRequest::update(u);
    }

    template <typename S> void print(S &s) const {
      AppRequest::print(s);
      s << " account=" << account;
    }
  };

#ifdef DeclType
#undef DeclType
#endif
#define DeclType(Type, AppType) \
  struct Type : public Base::Type, public App ## AppType { \
    template <typename Update> void update(const Update &u) { \
      Base::Type::update(u); \
      App ## AppType::update(u); \
    } \
    template <typename S> void print(S &s) const { \
      Base::Type::print(s); s << ' '; App ## AppType::print(s); \
    } \
  }

  DeclType(CancelLeg, CxlLeg);
  DeclType(CanceledLeg, CxlLeg);

  DeclType(ModifyLeg, ModLeg);
  DeclType(ModifiedLeg, ModLeg);

  DeclType(OrderLeg, OrderLeg);
  // DeclType(OrderedLeg, OrderedLeg);

  DeclType(NewOrder, NewOrder);
  DeclType(Ordered, Ordered);
  DeclType(Reject, Ack);

  DeclType(Modify, Modify);
  DeclType(Modified, Modified);
  DeclType(ModReject, Ack);

  DeclType(Cancel, Cancel);
  DeclType(Canceled, Canceled);
  DeclType(CxlReject, Ack);

  DeclType(Fill, Exec);
  DeclType(Closed, Exec);

#undef DeclType
};

using TxnTypes = MxTTxnTypes<AppTypes>;

struct App : public MxTOrderMgr<App, TxnTypes> {
  // log abnormal OSM transition
  template <typename Txn> void abnormal(Order *, const Txn &) { }

  // returns true if async. modify enabled for order
  bool asyncMod(Order *) { return true; }
  // returns true if async. cancel enabled for order
  bool asyncCxl(Order *) { return true; }

  // initialize synthetic cancel
  void synCancel(Order *) { }

  // process messages
  void sendNewOrder(Order *) { }
  void sendModify(Order *) { }
  void sendCancel(Order *) { }

  template <typename L> void sendOrdered(Order *, L) { }
  template <typename L> void sendReject(Order *, L) { }

  template <typename L> void sendModified(Order *, L) { }
  template <typename L> void sendModReject(Order *, L) { }

  template <typename L> void sendCanceled(Order *, L) { }
  template <typename L> void sendCxlReject(Order *, L) { }

  template <typename L> void sendSuspend(Order *, L) { }
  template <typename L> void sendRelease(Order *, L) { }

  template <typename L> void sendFill(Order *, L) { }

  template <typename L> void sendClosed(Order *, L) { }

  void main();
};

int main()
{
  App app;
  app.main();
}

void App::main()
{
  Order *order = new Order(); // would be derived and indexed by real app
  {
    Txn<NewOrder> txn;

    // initialize new order
    NewOrder &newOrder = txn.initNewOrder();
    // fill out new order data
    newOrder.clOrdID = "foo";
    newOrder.legs[0].side = MxSide::Buy;
    newOrder.legs[0].px = 100;
    newOrder.legs[0].pxNDP = 0;
    newOrder.legs[0].orderQty = 100;
    newOrder.legs[0].qtyNDP = 0;
    // order state management
    this->newOrder(order, txn);
    // dump order
    std::cout << *order << '\n';
  }
  {
    Txn<Ordered> txn;

    Ordered &ordered = txn.initOrdered();
    ordered.clOrdID = "foo";
    ordered.orderID = "bar";
    ordered.execID = "baz";
    this->ordered(order, txn);
    std::cout << *order << '\n';
    if (this->ack(order, txn))
      std::cout << txn << '\n';
  }
  {
    Txn<Cancel> txn;

    Cancel &cancel = txn.initCancel();
    cancel.clOrdID = "foo2";
    cancel.origClOrdID = "foo";
    this->cancel(order, txn);
    std::cout << *order << '\n';
  }
  {
    Txn<Canceled> txn;

    Canceled &canceled = txn.initCanceled();
    canceled.clOrdID = "foo";
    canceled.orderID = "bar2";
    canceled.execID = "baz2";
    canceled.legs[0].cumQty = 50;
    canceled.legs[0].qtyNDP = 0;
    this->canceled(order, txn);
    std::cout << *order << '\n';
    if (this->ack(order, txn))
      std::cout << "ACK: " << txn << '\n';
  }
  {
    Txn<Fill> txn;

    Fill &fill = txn.initFill();
    fill.clOrdID = "foo";
    fill.orderID = "bar2";
    fill.execID = "baz2";
    fill.lastPx = 100;
    fill.lastQty = 50;
    fill.pxNDP = 0;
    fill.qtyNDP = 0;
    this->fill(order, txn);
    std::cout << *order << '\n';
    if (this->ack(order, txn))
      std::cout << "ACK: " << txn << '\n';
  }

  delete order;
  order = new Order();

  {
    Txn<NewOrder> txn;

    NewOrder &newOrder = txn.initNewOrder();
    newOrder.clOrdID = "foo";
    newOrder.legs[0].side = MxSide::Buy;
    newOrder.legs[0].px = 100;
    newOrder.legs[0].pxNDP = 0;
    newOrder.legs[0].orderQty = 100;
    newOrder.legs[0].qtyNDP = 0;
    this->newOrder(order, txn);
    std::cout << *order << '\n';
  }
  {
    Txn<Ordered> txn;

    Ordered &ordered = txn.initOrdered();
    ordered.clOrdID = "foo";
    ordered.orderID = "bar";
    ordered.execID = "baz";
    this->ordered(order, txn);
    std::cout << *order << '\n';
    if (this->ack(order, txn))
      std::cout << txn << '\n';
  }
  {
    Txn<Modify> txn;

    Modify &modify = txn.initModify();
    modify.clOrdID = "foo2";
    modify.origClOrdID = "foo";
    modify.legs[0].px = 80;
    modify.legs[0].pxNDP = 0;
    modify.legs[0].orderQty = 80;
    modify.legs[0].qtyNDP = 0;
    this->modify(order, txn);
    std::cout << *order << '\n';
  }
  {
    Txn<ModReject> txn;

    ModReject &modReject = txn.initModReject();
    modReject.clOrdID = "foo2";
    modReject.origClOrdID = "foo";
    this->modReject(order, txn);
    std::cout << *order << '\n';
  }

  delete order;

  std::cout <<
    "sizeof(Reject): " << ZuBoxed(sizeof(Reject)) << '\n' <<
    "sizeof(NewOrder): " << ZuBoxed(sizeof(NewOrder)) << '\n' <<
    "sizeof(OrderLeg): " << ZuBoxed(sizeof(OrderLeg)) << '\n' <<
    "sizeof(Modify): " << ZuBoxed(sizeof(Modify)) << '\n' <<
    "sizeof(ModifyLeg): " << ZuBoxed(sizeof(ModifyLeg)) << '\n' <<
    "sizeof(Cancel): " << ZuBoxed(sizeof(Cancel)) << '\n' <<
    "sizeof(CancelLeg): " << ZuBoxed(sizeof(CancelLeg)) << '\n' <<
    "sizeof(OrderTxn): " << ZuBoxed(sizeof(OrderTxn)) << '\n' <<
    "sizeof(ModifyTxn): " << ZuBoxed(sizeof(ModifyTxn)) << '\n' <<
    "sizeof(CancelTxn): " << ZuBoxed(sizeof(CancelTxn)) << '\n' <<
    "sizeof(AckTxn): " << ZuBoxed(sizeof(AckTxn)) << '\n' <<
    "sizeof(AnyTxn): " << ZuBoxed(sizeof(AnyTxn)) << '\n' <<
    "sizeof(Order): " << ZuBoxed(sizeof(Order)) << '\n';
}
