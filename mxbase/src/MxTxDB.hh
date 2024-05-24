//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxT transmission database

#ifndef MxTxDB_HH
#define MxTxDB_HH

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <zlib/Zdb.hh>

#include <mxbase/MxBase.hh>

// CRTP - implementation must conform to the following interface:
#if 0
struct App : public MxTxDB<App> {
  void txAdded(TxPOD *);		// transmission recovered/replicated
};
#endif

template <typename App_> class MxTxDB {
public:
  using App = App_;

  ZuInline const App *app() const { return static_cast<const App *>(this); }
  ZuInline App *app() { return static_cast<App *>(this); }

  enum { DBVersion = 0 }; // increment when schema changes

  struct TxData {
    MxMsgID	msgID;
    ZdbRN	msgRN = ZdbNullRN;
    MxInt	msgType;

    struct MsgRN : public ZuPrintable {
      ZdbRN	rn;
      MsgRN(ZdbRN rn_) : rn{rn_} { }
      template <typename S>
      void print(S &s) const {
	if (rn != ZdbNullRN) s << ZuBoxed(rn);
      }
    };
    template <typename S> static void csvHdr(S &s) {
      s << "linkID,seqNo,msgRN,msgType\n";
    }
    template <typename S> void csv(S &s) const {
      s << msgID.linkID
	<< ',' << msgID.seqNo
	<< ',' << MsgRN{msgRN}
	<< ',' << msgType << '\n';
    }
  };

  using TxDB = Zdb<TxData>;
  using TxPOD = ZdbPOD<TxData>;

  void init(ZdbEnv *dbEnv, const ZvCf *cf) {
    m_txDB = new TxDB(
	dbEnv, "txDB", DBVersion, ZdbCacheMode::Normal,
	ZdbHandler{
	  [](ZdbAny *db, ZmRef<ZdbAnyPOD> &pod) { pod = new TxPOD(db); },
	  ZdbAddFn{app(), [](App *app, ZdbAnyPOD *pod, int op, bool) {
	    if (op != ZdbOp::Del)
	      app->txAdded(static_cast<TxPOD *>(pod)); }},
	  app()->txWriteFn()});
  }
  void final() {
    m_txDB = nullptr;
  }

  TxDB *txDB() const { return m_txDB; }

  // l(MxQMsg *msg, ZdbRN &rn, int32_t &type) -> void
  template <typename Link, typename L>
  void txStore(Link *link, const MxMsgID &msgID, L l) {
    auto &txPOD = link->txPOD;
    if (ZuUnlikely(!txPOD))
      txPOD = m_txDB->push();
    else {
      // protect against rewinds
      if (ZuUnlikely(msgID.seqNo <= txPOD->data().msgID.seqNo)) return;
      txPOD = m_txDB->update(txPOD);
    }
    if (ZuUnlikely(!txPOD)) { ZeLOG(Error, "txDB update failed"); return; }
    auto &txData = txPOD->data();
    txData.msgID = msgID;
    l(txData.msgRN, txData.msgType);
    if (ZuUnlikely(txPOD->rn() == txPOD->prevRN()))
      m_txDB->put(txPOD);
    else
      m_txDB->putUpdate(txPOD, false);
  }

  // l(ZdbRN rn, int32_t type, MxSeqNo seqNo) -> ZmRef<MxQMsg>
  template <typename Link, typename L>
  ZmRef<MxQMsg> txRetrieve(Link *link, MxSeqNo seqNo, MxSeqNo avail, L l) {
    auto txPOD = link->txPOD;
    auto txQueue = link->txQueue();
    while (txPOD) {
      auto &txData = txPOD->data();
      auto txSeqNo = txData.msgID.seqNo;
      if (ZuUnlikely(txSeqNo < seqNo)) return nullptr;
      if (txSeqNo == seqNo || txSeqNo < avail) {
	ZmRef<MxQMsg> msg = l(txData.msgRN, txData.msgType, txSeqNo);
	if (msg) {
	  msg->load(txData.msgID);
	  if (ZuUnlikely(txSeqNo == seqNo)) return msg;
	  txQueue->unshift(ZuMv(msg));
	}
      }
      ZdbRN prevRN = txPOD->prevRN();
      if (txPOD->rn() == prevRN) return nullptr;
      txPOD = m_txDB->get(prevRN);
    }
    return nullptr;
  }

private:
  ZmRef<TxDB>	m_txDB;
};

#endif /* MxTxDB_HH */
