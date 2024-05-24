//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxT received messages database

#ifndef MxRxDB_HH
#define MxRxDB_HH

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <zlib/Zdb.hh>

#include <mxbase/MxBase.hh>

// CRTP - implementation must conform to the following interface:
#if 0
struct App : public MxRxDB<App> {
  void rxAdded(RxPOD *);		// reception recovered/replicated
};
#endif

template <typename App_> class MxRxDB {
public:
  using App = App_;

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  enum { DBVersion = 0 }; // increment when schema changes

  struct RxData {
    MxMsgID	msgID;

    template <typename S> static void csvHdr(S &s) {
      s << "linkID,seqNo\n";
    }
    template <typename S> void csv(S &s) const {
      s << msgID.linkID
	<< ',' << msgID.seqNo << '\n';
    }
  };

  using RxDB = Zdb<RxData>;
  using RxPOD = ZdbPOD<RxData>;

  void init(ZdbEnv *dbEnv, const ZvCf *cf) {
    m_rxDB = new RxDB(
	dbEnv, "rxDB", DBVersion, ZdbCacheMode::Normal,
	ZdbHandler{
	  [](ZdbAny *db, ZmRef<ZdbAnyPOD> &pod) { pod = new RxPOD(db); },
	  ZdbAddFn{app(), [](App *app, ZdbAnyPOD *pod, int op, bool) {
	    if (op != ZdbOp::Del)
	      app->rxAdded(static_cast<RxPOD *>(pod)); }},
	  app()->rxWriteFn()});
  }
  void final() {
    m_rxDB = nullptr;
  }

  RxDB *rxDB() const { return m_rxDB; }

  template <typename Link>
  void rxStore(Link *link, const MxMsgID &msgID) {
    auto &rxPOD = link->rxPOD;
    if (ZuUnlikely(!rxPOD))
      rxPOD = m_rxDB->push();
    else
      rxPOD = m_rxDB->update(rxPOD);
    if (ZuUnlikely(!rxPOD)) { ZeLOG(Error, "rxDB update failed"); return; }
    auto &rxData = rxPOD->data();
    rxData.msgID = msgID;
    if (ZuUnlikely(rxPOD->rn() == rxPOD->prevRN()))
      m_rxDB->put(rxPOD);
    else
      m_rxDB->putUpdate(rxPOD);
  }

private:
  ZmRef<RxDB>	m_rxDB;
};

#endif /* MxRxDB_HH */
