//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD library

#ifndef MxMD_HH
#define MxMD_HH

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuIndex.hh>
#include <zlib/ZuObject.hh>
#include <zlib/ZuRef.hh>

#include <zlib/ZmFn.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmHash.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmRWLock.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmShard.hh>
#include <zlib/ZmScheduler.hh>

#include <zlib/ZcmdHost.hh>

#include <mxbase/MxBase.hh>

#include <mxmd/MxMDTypes.hh>

// exceptions

auto MxMDOrderNotFound(const char *op, MxIDString id) {
  return [op, id](auto &s) {
      s << "MxMD " << op << ": order " << id << " not found"; };
}
auto MxMDDuplicateOrderID(const char *op, MxIDString id) {
  return [op, id](auto &s) {
      s << "MxMD " << op << ": order " << id << " already exists"; };
}
auto MxMDNoPxLevel(const char *op) {
  return [op](auto &s) {
      s << "MxMD " << op << ": internal error - missing price level"; };
}
auto MxMDNoOrderBook(const char *op) {
  return [op](auto &s) {
      s << "MxMD " << op << ": internal error - missing order book"; };
}
auto MxMDMissedUpdates(MxID venue, unsigned n) {
  return [venue, n](auto &s) {
      s << "MxMD: missed " << ZuBoxed(n) << " updates from " << venue; };
}
auto MxMDMissedOBUpdates(MxID venue, unsigned n, MxIDString id) {
  return [venue, n, id](auto &s) {
      s << "MxMD: missed " << ZuBoxed(n) << " updates from " << venue
	<< " for " << id; };
}

// generic venue-specific flags handling

template <class T> struct MxMDFlags {
  struct Default {
    static void print(MxMDFlagsStr &out, MxFlags flags) {
      out << flags.hex<false, ZuFmt::Right<8>>();
    }
    static void scan(const MxMDFlagsStr &in, MxFlags &flags) {
      flags.scan<ZuFmt::Hex<false, ZuFmt::Right<8>>>(in);
    }
  };
  typedef Default XTKS;
  typedef Default XTKD;
  typedef Default XOSE;
  typedef Default XASX;

  typedef void (*PrintFn)(MxMDFlagsStr &, MxFlags);
  typedef void (*ScanFn)(const MxMDFlagsStr &, MxFlags &);

  template <class, typename> struct FnPtr;
  template <class Venue> struct FnPtr<Venue, PrintFn> {
    static PrintFn fn() { return &Venue::print; }
  };
  template <class Venue> struct FnPtr<Venue, ScanFn> {
    static ScanFn fn() { return &Venue::scan; }
  };

  template <typename Fn>
  class FnMap {
    using Map =
      ZmRBTree<MxID,
	ZmRBTreeVal<Fn,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmNoLock> > > >;

  public:
    FnMap() {
      m_map.add("XTKS", FnPtr<typename T::XTKS, Fn>::fn());
      m_map.add("XTKD", FnPtr<typename T::XTKD, Fn>::fn());
      m_map.add("XOSE", FnPtr<typename T::XOSE, Fn>::fn());
      m_map.add("XASX", FnPtr<typename T::XASX, Fn>::fn());
    }
    Fn fn(MxID venue) { return m_map.findVal(venue); }

  private:
    Map	m_map;
  };

  static void print(MxMDFlagsStr &out, MxID venue, MxFlags flags) {
    static FnMap<PrintFn> map;
    PrintFn fn = map.fn(venue);
    if (fn) (*fn)(out, flags);
  }
  static MxMDFlagsStr print(MxID venue, MxFlags flags) {
    MxMDFlagsStr out;
    print(out, venue, flags);
    return out;
  }
  static void scan(const MxMDFlagsStr &in, MxID venue, MxFlags &flags) {
    static FnMap<ScanFn> map;
    ScanFn fn = map.fn(venue);
    if (fn) (*fn)(in, flags);
  }
};

// pre-declarations

class MxMDCore;
class MxMDLib;
class MxMDShard;
class MxMDFeed;
class MxMDVenue;
class MxMDVenueShard;
class MxMDInstrument;
class MxMDOrderBook;
class MxMDOBSide;
class MxMDPxLevel_;
class MxMDOrder_;

typedef ZmSharded<MxMDShard> MxMDSharded;

// L1 flags

struct MxMDL1Flags : public MxMDFlags<MxMDL1Flags> {
  struct Liffe : public Default {
    static unsigned tradeType(const MxFlags &flags) {
      return (unsigned)flags;
    }
    static void tradeType(MxFlags &flags, unsigned tradeType) {
      flags = tradeType;
    }
  };
  typedef Liffe XTKD;

  struct XASX : public Default {
    static bool printable(const MxFlags &flags) {
      return flags & 1;
    }
    static void printable(MxFlags &flags, bool printable) {
      flags = ((unsigned)flags & ~1) | (unsigned)printable;
    }
    static bool auction(const MxFlags &flags) {
      return flags & 2;
    }
    static void auction(MxFlags &flags, bool auction) {
      flags = ((unsigned)flags & ~2) | (((unsigned)auction)<<1);
    }
    static void print(MxMDFlagsStr &out, MxFlags flags) {
      if (auction(flags)) out << 'A';
      if (printable(flags)) out << 'P';
    }
    static void scan(const MxMDFlagsStr &in, MxFlags &flags) {
      flags = 0;
      for (unsigned i = 0; i < in.length(); i++) {
	if (in[i] == 'A') auction(flags, true);
	if (in[i] == 'P') printable(flags, true);
      }
    }
  };
};

// venue mapping

ZuDeclTuple(MxMDVenueMapKey, (MxID, venue), (MxID, segment));

struct MxMDVenueMapping {
  MxID		venue;
  MxID		segment;
  unsigned	rank = 0;

  bool operator !() const { return !venue; }
  ZuOpBool
};

// tick size table

class MxMDAPI MxMDTickSizeTbl : public ZmObject {
  MxMDTickSizeTbl(const MxMDTickSizeTbl &) = delete;
  MxMDTickSizeTbl &operator =(const MxMDTickSizeTbl &) = delete;

friend MxMDLib;
friend MxMDVenue;

  struct TickSizes_HeapID {
    static constexpr const char *id() { return "MxMDTickSizeTbl.TickSizes"; }
  };
  using TickSizes =
    ZmRBTree<MxMDTickSize,
      ZmRBTreeIndex<MxMDTickSize_MinPxAccessor,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeLock<ZmRWLock,
	    ZmRBTreeHeapID<TickSizes_HeapID> > > > >;

  struct IDAccessor {
    static const MxIDString &get(const MxMDTickSizeTbl *tbl) {
      return tbl->id();
    }
  };

  template <typename ID>
  MxMDTickSizeTbl(MxMDVenue *venue, const ID &id, MxNDP pxNDP) :
    m_venue(venue), m_id(id), m_pxNDP(pxNDP) { }

public:
  ~MxMDTickSizeTbl() { }

  MxMDVenue *venue() const { return m_venue; }
  const MxIDString &id() const { return m_id; }
  MxNDP pxNDP() const { return m_pxNDP; }

  void reset();
  void addTickSize(MxValue minPrice, MxValue maxPrice, MxValue tickSize);
  MxValue tickSize(MxValue price) {
    auto i = m_tickSizes.readIterator<ZmRBTreeGreaterEqual>(price);
    TickSizes::Node *node = i.iterate();
    if (!node) return MxValue();
    return node->key().tickSize();
  }
  template <typename L> // (const MxMDTickSize &) -> bool
  bool allTickSizes(L l) const {
    auto i = m_tickSizes.readIterator();
    while (const TickSizes::Node *node = i.iterate())
      if (!l(node->key())) return false;
    return true;
  }

private:
  void reset_();
  void addTickSize_(MxValue minPrice, MxValue maxPrice, MxValue tickSize);

  MxMDVenue	*m_venue;
  MxIDString	m_id;
  MxNDP		m_pxNDP;
  TickSizes	m_tickSizes;
};

// trades

struct MxMDTradeData {
  MxIDString	tradeID;
  MxDateTime	transactTime;
  MxValue	price;
  MxValue	qty;
};

struct MxMDTrade_HeapID {
  static constexpr const char *id() { return "MxMDTrade"; }
};
template <class Heap> class MxMDTrade_ : public Heap, public ZmObject {
  MxMDTrade_(const MxMDTrade_ &) = delete;
  MxMDTrade_ &operator =(const MxMDTrade_ &) = delete;

friend MxMDOrderBook;
friend MxMDLib;

  template <typename TradeID>
  MxMDTrade_(MxMDInstrument *instrument, MxMDOrderBook *ob,
      const TradeID &tradeID, MxDateTime transactTime,
      MxValue price, MxValue qty) :
      m_instrument(instrument), m_orderBook(ob),
      m_data{tradeID, transactTime, price, qty} { }

public:
  ~MxMDTrade_() { }

  MxMDInstrument *instrument() const { return m_instrument; }
  MxMDOrderBook *orderBook() const { return m_orderBook; }

  const MxMDTradeData &data() const { return m_data; }

private:
  MxMDInstrument	*m_instrument;
  MxMDOrderBook		*m_orderBook;
  MxMDTradeData		m_data;
};
typedef ZmHeap<MxMDTrade_HeapID, sizeof(MxMDTrade_<ZuNull>)> MxMDTrade_Heap;
typedef MxMDTrade_<MxMDTrade_Heap> MxMDTrade;

// orders

struct MxMDOrderFlags : public MxMDFlags<MxMDOrderFlags> {
  struct OMX : public Default {
    static bool implied(const MxFlags &flags) {
      return flags & 1;
    }
    static void implied(MxFlags &flags, bool implied) {
      flags = ((unsigned)flags & ~1) | (unsigned)implied;
    }
    static void print(MxMDFlagsStr &out, MxFlags flags) {
      if (implied(flags)) out << 'I';
    }
    static void scan(const MxMDFlagsStr &out, MxFlags &flags) {
      flags = 0;
      for (unsigned i = 0; i < out.length(); i++) {
	if (out[i] == 'I') implied(flags, true);
      }
    }
  };
  typedef OMX XOSE;
};

struct MxMDOrderData {
  MxDateTime	transactTime;
  MxEnum	side;
  MxUInt	rank;
  MxValue	price;
  MxValue	qty;
  MxFlags	flags;		// MxMDOrderFlags
};

ZuDeclTuple(MxMDOrderID2,
    (MxInstrKey, obKey),
    (MxIDString, orderID));
ZuDeclTuple(MxMDOrderID2Ref,
    (const MxInstrKey &, obKey),
    (const MxIDString &, orderID));
ZuDeclTuple(MxMDOrderID3,
    (MxInstrKey, obKey),
    (MxEnum, side),
    (MxIDString, orderID));
ZuDeclTuple(MxMDOrderID3Ref,
    (const MxInstrKey &, obKey),
    (const MxEnum &, side),
    (const MxIDString &, orderID));

class MxMDAPI MxMDOrder_ {
  MxMDOrder_(const MxMDOrder_ &) = delete;
  MxMDOrder_ &operator =(const MxMDOrder_ &) = delete;

friend MxMDVenue;
friend MxMDVenueShard;
friend MxMDOrderBook;
friend MxMDOBSide;
friend MxMDPxLevel_;

private:
  static MxMDOBSide *bids_(const MxMDOrderBook *);
  static MxMDOBSide *asks_(const MxMDOrderBook *);

protected:
  template <typename ID>
  MxMDOrder_(MxMDOrderBook *ob,
      ID &&id, MxDateTime transactTime, MxEnum side,
      MxUInt rank, MxValue price, MxValue qty, MxFlags flags) :
      m_orderBook(ob),
      m_id(ZuFwd<ID>(id)),
      m_data{transactTime, side, rank, price, qty, flags} { }

public:
  ~MxMDOrder_() { }

  struct OrderID3Accessor;
friend OrderID3Accessor;
  struct OrderID3Accessor {
    static MxMDOrderID3Ref get(const MxMDOrder_ *o);
  };
  struct OrderID2Accessor;
friend OrderID2Accessor;
  struct OrderID2Accessor {
    static MxMDOrderID2Ref get(const MxMDOrder_ *o);
  };
  struct OrderID1Accessor;
friend OrderID1Accessor;
  struct OrderID1Accessor {
    static const MxIDString &get(const MxMDOrder_ *o) { return o->m_id; }
  };

  MxMDOrderBook *orderBook() const { return m_orderBook; }
  MxMDOBSide *obSide() const;
  MxMDPxLevel_ *pxLevel() const { return m_pxLevel; }

  const MxIDString &id() const { return m_id; }
  const MxMDOrderData &data() const { return m_data; }

  template <typename T = uintptr_t>
  T appData() const { return static_cast<T>(m_appData.load_()); }
  template <typename T>
  void appData(T v) { m_appData.store_(static_cast<uintptr_t>(v)); }

private:
  MxMDOrderData &data_() { return m_data; }

  void update_(MxUInt rank, MxValue price, MxValue qty, MxFlags flags) {
    m_data.rank = rank;
    if (*price) m_data.price = price;
    if (*qty) m_data.qty = qty;
    m_data.flags = flags;
  }
  void updateQty_(MxValue qty) {
    if (*qty) m_data.qty = qty;
  }
  void updateNDP(
      MxNDP oldPxNDP, MxNDP oldQtyNDP, MxNDP pxNDP, MxNDP qtyNDP) {
    if (*pxNDP && pxNDP != oldPxNDP)
      m_data.price = MxValNDP{m_data.price, oldPxNDP}.adjust(pxNDP);
    if (*qtyNDP && qtyNDP != oldQtyNDP)
      m_data.qty = MxValNDP{m_data.qty, oldQtyNDP}.adjust(qtyNDP);
  }

  void pxLevel(MxMDPxLevel_ *l) { m_pxLevel = l; }

  MxMDOrderBook		*m_orderBook;
  MxMDPxLevel_		*m_pxLevel = 0;

  MxIDString		m_id;
  MxMDOrderData		m_data;

  ZmAtomic<uintptr_t>	m_appData = 0;
};

struct MxMDOrder_RankAccessor {
  static MxUInt get(const MxMDOrder_ &o) { return o.data().rank; }
};

struct MxMDOrder_HeapID {
  static constexpr const char *id() { return "MxMDOrder"; }
};
using MxMDOrders =
	ZmRBTree<MxMDOrder_,
	  ZmRBTreeIndex<MxMDOrder_RankAccessor,
	    ZmRBTreeNodeIsKey<true,
	      ZmRBTreeObject<ZmObject,
		ZmRBTreeLock<ZmNoLock,
		  ZmRBTreeHeapID<MxMDOrder_HeapID> > > > > >;
using MxMDOrder = MxMDOrders::Node;

struct MxMDOrders1_HeapID {
  static constexpr const char *id() { return "MxMDOrders1"; }
};
struct MxMDOrders2_HeapID {
  static constexpr const char *id() { return "MxMDOrders2"; }
};
struct MxMDOrders3_HeapID {
  static constexpr const char *id() { return "MxMDOrders3"; }
};

// order, oldPxNDP, oldQtyNDP, pxNDP, qtyNDP
using MxMDOrderNDPFn = ZmFn<void(MxMDOrder *, MxNDP, MxNDP, MxNDP, MxNDP)>;

// price levels

struct MxMDPxLvlData {
  MxMDPxLvlData &operator +=(const MxMDPxLvlData &data) {
    if (transactTime < data.transactTime) transactTime = data.transactTime;
    qty += data.qty;
    nOrders += data.nOrders;
    return *this;
  }

  MxDateTime		transactTime;
  MxValue		qty;
  MxUInt		nOrders;
  MxFlags		flags;		// MxMDL2Flags
};

struct MxMDL2Flags : public MxMDFlags<MxMDL2Flags> {
  // FIXME - XASX
  struct XTKS : public Default {
    struct QuoteType {
      enum _ { Empty = 0, General = 1, SQ = 3, CQ = 4, Unknown = 7 };
    };
    static unsigned quoteType(const MxFlags &flags) {
      return (unsigned)flags & 7;
    }
    static void quoteType(MxFlags &flags, unsigned quoteType) {
      if (quoteType > 7) quoteType = QuoteType::Unknown;
      flags = ((unsigned)flags & ~7) | quoteType;
    }
    struct MatchType {
      enum _ { Normal = 0, InsideSQ = 1, OutsideSQ = 2, Unknown = 3 };
    };
    static unsigned matchType(const MxFlags &flags) {
      return ((unsigned)flags>>3) & 3;
    }
    static void matchType(MxFlags &flags, unsigned matchType) {
      if (matchType > 3) matchType = MatchType::Unknown;
      flags = ((unsigned)flags & ~(3<<3)) | (matchType<<3);
    }
    static bool closing(const MxFlags &flags) {
      return ((unsigned)flags>>5) & 1;
    }
    static void closing(MxFlags &flags, bool closing) {
      flags = ((unsigned)flags & ~(1<<5)) | (((unsigned)closing)<<5);
    }

    static void print(MxMDFlagsStr &out, MxFlags flags) {
      if (closing(flags)) out << 'C';
      switch (quoteType(flags)) {
	case QuoteType::Empty: break;
	case QuoteType::General: out << 'G'; break;
	case QuoteType::SQ: out << 'S'; break;
	case QuoteType::CQ: out << 'K'; break;		// per TSE terminal
      }
      switch (matchType(flags)) {
	case MatchType::Normal: break;
	case MatchType::InsideSQ: out << '#'; break;	// per TSE terminal
	case MatchType::OutsideSQ: out << '*'; break;	// per TSE terminal
      }
    }
    static void scan(const MxMDFlagsStr &out, MxFlags &flags) {
      flags = 0;
      for (unsigned i = 0; i < out.length(); i++) {
	switch ((int)out[i]) {
	  case 'C': closing(flags, true); break;
	  case 'G': quoteType(flags, QuoteType::General); break;
	  case 'S': quoteType(flags, QuoteType::SQ); break;
	  case 'K': quoteType(flags, QuoteType::CQ); break;
	  case '#': matchType(flags, MatchType::InsideSQ); break;
	  case '*': matchType(flags, MatchType::OutsideSQ); break;
	}
      }
    }
  };

  struct Liffe : public Default {
    static bool rfqx(const MxFlags &flags) {
      return flags & 1;
    }
    static void rfqx(MxFlags &flags, bool rfqx) {
      flags = ((unsigned)flags & ~1) | (unsigned)rfqx;
    }
    static bool implied(const MxFlags &flags) {
      return flags & 2;
    }
    static void implied(MxFlags &flags, bool implied) {
      flags = ((unsigned)flags & ~2) | (((unsigned)implied)<<1);
    }

    static void print(MxMDFlagsStr &out, MxFlags flags) {
      if (rfqx(flags)) out << 'R';
      if (implied(flags)) out << 'I';
    }
    static void scan(const MxMDFlagsStr &out, MxFlags &flags) {
      flags = 0;
      for (unsigned i = 0; i < out.length(); i++) {
	switch ((int)out[i]) {
	  case 'R': rfqx(flags, true); break;
	  case 'I': implied(flags, true); break;
	}
      }
    }
  };
  typedef Liffe XTKD;
};

// order, time stamp
typedef ZmFn<void(MxMDOrder *, MxDateTime)> MxMDOrderFn;

class MxMDPxLevel_ : public ZmObject {
  MxMDPxLevel_(const MxMDPxLevel_ &) = delete;
  MxMDPxLevel_ &operator =(const MxMDPxLevel_ &) = delete;

friend MxMDOrderBook;
friend MxMDOBSide;

protected:
  MxMDPxLevel_(MxMDOBSide *obSide,
      MxDateTime transactTime, MxNDP pxNDP, MxNDP qtyNDP,
      MxValue price, MxValue qty, MxUInt nOrders, MxFlags flags) :
    m_obSide(obSide), m_pxNDP(pxNDP), m_qtyNDP(qtyNDP),
    m_price(price), m_data{transactTime, qty, nOrders, flags} { }

public:
  ~MxMDPxLevel_() { }

  MxMDOBSide *obSide() const { return m_obSide; }
  MxEnum side() const;
  MxNDP pxNDP() const { return m_pxNDP; }
  MxNDP qtyNDP() const { return m_qtyNDP; }
  MxValue price() const { return m_price; }
  const MxMDPxLvlData &data() const { return m_data; }

  template <typename L> // (MxMDOrder *) -> bool
  bool allOrders(L l) const {
    auto i = m_orders.readIterator();
    while (MxMDOrder *order = i.iterate())
      if (!l(order)) return false;
    return true;
  }

private:
  void reset(MxDateTime transactTime, MxMDOrderFn);

  void updateAbs(
      MxDateTime transactTime, MxValue qty, MxUInt nOrders, MxFlags flags,
      MxValue &d_qty, MxUInt &d_nOrders);
  void updateAbs_(MxValue qty, MxUInt nOrders, MxFlags flags,
      MxValue &d_qty, MxUInt &d_nOrders);
  void updateDelta(
      MxDateTime transactTime, MxValue qty, MxUInt nOrders, MxFlags flags);
  void updateDelta_(MxValue qty, MxUInt nOrders, MxFlags flags);
  void update(MxDateTime transactTime, bool delta,
      MxValue qty, MxUInt nOrders, MxFlags flags,
      MxValue &d_qty, MxUInt &d_nOrders);

  void addOrder(MxMDOrder *order);
  void delOrder(MxUInt rank);

  template <typename Fill>
  bool match(MxDateTime transactTime,
      MxValue &qty, MxValue &cumQty, Fill fill);

  void updateLast(MxDateTime stamp,
      MxValue lastQty, MxValue nv, MxValue openQty);

  void deletedOrder_(MxMDOrder *order, MxDateTime transactTime);

  void updateNDP(
      MxNDP oldPxNDP, MxNDP oldQtyNDP, MxNDP pxNDP, MxNDP qtyNDP,
      const MxMDOrderNDPFn &);

  MxMDOBSide		*m_obSide;
  MxNDP			m_pxNDP;
  MxNDP			m_qtyNDP;
  MxValue		m_price;
  MxMDPxLvlData	  	m_data;
  MxMDOrders		m_orders;
};

// FIXME
struct MxMDPxLevels_HeapID : public ZmHeapSharded {
  static constexpr const char *id() { return "MxMDPxLevel"; }
};

struct MxMDPxLevel_PxAccessor {
  static MxValue get(const MxMDPxLevel_ &p) { return p.price(); }
};

typedef ZmRBTree<MxMDPxLevel_,
	  ZmRBTreeKey<MxMDPxLevel_PxAccessor,
	    ZmRBTreeObject<ZmObject,
	      ZmRBTreeNodeDerive<true,
		ZmRBTreeHeapID<MxMDPxLevels_HeapID,
		  ZmRBTreeLock<ZmNoLock> > > > > > MxMDPxLevels;

typedef MxMDPxLevels::Node MxMDPxLevel;

// event handlers (callbacks)

typedef ZmFn<void(MxMDLib *)> MxMDLibFn;
typedef ZmFn<void(const MxMDLib *, ZmRef<ZeEvent> )> MxMDExceptionFn;
typedef ZmFn<void(MxMDFeed *)> MxMDFeedFn;
typedef ZmFn<void(MxMDVenue *)> MxMDVenueFn;

typedef ZmFn<void(MxMDTickSizeTbl *)> MxMDTickSizeTblFn;
typedef ZmFn<void(MxMDTickSizeTbl *, const MxMDTickSize &)> MxMDTickSizeFn;

// venue, segment data
typedef ZmFn<void(MxMDVenue *, MxMDSegment)> MxMDTradingSessionFn;

typedef ZmFn<void(MxMDInstrument *, MxDateTime)> MxMDInstrumentFn;
typedef ZmFn<void(MxMDOrderBook *, MxDateTime)> MxMDOrderBookFn;

// order book, data
typedef ZmFn<void(MxMDOrderBook *, const MxMDL1Data &)> MxMDLevel1Fn;
// price level, time stamp
typedef ZmFn<void(MxMDPxLevel *, MxDateTime)> MxMDPxLevelFn;
// trade, time stamp
typedef ZmFn<void(MxMDTrade *, MxDateTime)> MxMDTradeFn;
// time stamp, next time stamp
typedef ZmFn<void(MxDateTime, MxDateTime &)> MxMDTimerFn;

struct MxMDLibHandler : public ZmObject {
#define MxMDLibHandler_Fn(Type, member) \
  template <typename Arg> \
  MxMDLibHandler && member##Fn(Arg &&arg) { \
    member = ZuFwd<Arg>(arg); \
    return ZuMv(*this); \
  } \
  Type	member
  MxMDLibHandler_Fn(MxMDExceptionFn,		exception);
  MxMDLibHandler_Fn(MxMDFeedFn,			connected);
  MxMDLibHandler_Fn(MxMDFeedFn,			disconnected);
  MxMDLibHandler_Fn(MxMDLibFn,			eof);
  MxMDLibHandler_Fn(MxMDVenueFn,		addVenue);
  MxMDLibHandler_Fn(MxMDVenueFn,		refDataLoaded);
  MxMDLibHandler_Fn(MxMDTickSizeTblFn,		addTickSizeTbl);
  MxMDLibHandler_Fn(MxMDTickSizeTblFn,		resetTickSizeTbl);
  MxMDLibHandler_Fn(MxMDTickSizeFn,		addTickSize);
  MxMDLibHandler_Fn(MxMDInstrumentFn,		addInstrument);
  MxMDLibHandler_Fn(MxMDInstrumentFn,		updatedInstrument);
  MxMDLibHandler_Fn(MxMDOrderBookFn,		addOrderBook);
  MxMDLibHandler_Fn(MxMDOrderBookFn,		updatedOrderBook);
  MxMDLibHandler_Fn(MxMDOrderBookFn,		deletedOrderBook);
  MxMDLibHandler_Fn(MxMDTradingSessionFn,	tradingSession);
  MxMDLibHandler_Fn(MxMDTimerFn,		timer);
#undef MxMDLibHandler_Fn
};

struct MxMDInstrHandler : public ZuObject {
#define MxMDInstrHandler_Fn(Type, member) \
  template <typename Arg> \
  MxMDInstrHandler && member##Fn(Arg &&arg) { \
    member = ZuFwd<Arg>(arg); \
    return ZuMv(*this); \
  } \
  Type	member
  MxMDInstrHandler_Fn(MxMDInstrumentFn,	updatedInstrument); // ref. data changed
  MxMDInstrHandler_Fn(MxMDOrderBookFn,	updatedOrderBook); // ''
  MxMDInstrHandler_Fn(MxMDLevel1Fn,	l1);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	addMktLevel);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	updatedMktLevel);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	deletedMktLevel);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	addPxLevel);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	updatedPxLevel);
  MxMDInstrHandler_Fn(MxMDPxLevelFn,	deletedPxLevel);
  MxMDInstrHandler_Fn(MxMDOrderBookFn,	l2);
  MxMDInstrHandler_Fn(MxMDOrderFn,	addOrder);
  MxMDInstrHandler_Fn(MxMDOrderFn,	modifiedOrder);
  MxMDInstrHandler_Fn(MxMDOrderFn,	deletedOrder);
  MxMDInstrHandler_Fn(MxMDTradeFn,	addTrade);
  MxMDInstrHandler_Fn(MxMDTradeFn,	correctedTrade);
  MxMDInstrHandler_Fn(MxMDTradeFn,	canceledTrade);
#undef MxMDInstrHandler_Fn
};

// order books

struct MxMDOBSideData {
  MxValue	nv;
  MxValue	qty;
};

class MxMDAPI MxMDOBSide : public ZmObject {
  MxMDOBSide(const MxMDOBSide &) = delete;
  MxMDOBSide &operator =(const MxMDOBSide &) = delete;

friend MxMDOrderBook;
friend MxMDPxLevel_;

private:
  MxMDOBSide(MxMDOrderBook *ob, MxEnum side) :
    m_orderBook(ob), m_side(side), m_data{0, 0} { }

public:
  MxMDOrderBook *orderBook() const { return m_orderBook; }
  MxEnum side() const { return m_side; }

  const MxMDOBSideData &data() const { return m_data; }
  MxValue vwap() const {
    return !m_data.qty ? MxValue() :
      (MxValNDP{m_data.nv, pxNDP()} /
       MxValNDP{m_data.qty, qtyNDP()}).value;
  }

  MxMDPxLevel *mktLevel() { return m_mktLevel; }

  // iterate over all price levels, best -> worst
  template <typename L> // (MxMDPxLevel *) -> bool
  bool allPxLevels(L l) const {
    if (m_side == MxSide::Buy) {
      auto i = m_pxLevels.readIterator<ZmRBTreeLessEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate())
	if (!l(pxLevel)) return false;
    } else {
      auto i = m_pxLevels.readIterator<ZmRBTreeGreaterEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate())
	if (!l(pxLevel)) return false;
    }
    return true;
  }

  // iterate over price levels in range, best -> worst
  template <typename L> // (MxMDPxLevel *)-> bool
  bool pxLevels(MxValue minPrice, MxValue maxPrice, L l) {
    if (m_side == MxSide::Buy) {
      auto i = m_pxLevels.readIterator<ZmRBTreeLessEqual>(maxPrice);
      while (MxMDPxLevel *pxLevel = i.iterate()) {
	if (pxLevel->price() <= minPrice) break;
	if (!l(pxLevel)) return false;
      }
    } else {
      auto i = m_pxLevels.readIterator<ZmRBTreeGreaterEqual>(minPrice);
      while (MxMDPxLevel *pxLevel = i.iterate()) {
	if (pxLevel->price() >= maxPrice) break;
	if (!l(pxLevel)) return false;
      }
    }
    return true;
  }

  MxNDP pxNDP() const;
  MxNDP qtyNDP() const;

  MxMDPxLevel *minimum() { return m_pxLevels.minimumPtr(); }
  MxMDPxLevel *maximum() { return m_pxLevels.maximumPtr(); }

  // returns effective limit Px for matching up to qty (market orders)
  MxValue matchPx(MxValue qty) {
    MxValue px;
    MxNDP pxNDP = this->pxNDP();
    MxNDP qtyNDP = this->qtyNDP();
    auto l = [&px, &qty, pxNDP, qtyNDP](MxMDPxLevel *pxLevel) -> bool {
      MxValue lvlQty = pxLevel->data().qty;
      if (qty < lvlQty) lvlQty = qty;
      px = pxLevel->price();
      return qty -= lvlQty;
    };
    if (m_side == MxSide::Buy) {
      auto i = m_pxLevels.readIterator<ZmRBTreeLessEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate())
	if (!l(pxLevel)) break;
    } else {
      auto i = m_pxLevels.readIterator<ZmRBTreeGreaterEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate())
	if (!l(pxLevel)) break;
    }
    return px;
  }

  // returns qty available for matching at limit price or better (limit orders)
  MxValue matchQty(MxValue px) {
    MxValue qty = 0;
    if (m_side == MxSide::Buy) {
      auto i = m_pxLevels.readIterator<ZmRBTreeLessEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate()) {
	MxValue cPx = pxLevel->price();
	if (px > cPx) break;
	qty += pxLevel->data().qty;
      }
    } else {
      auto i = m_pxLevels.readIterator<ZmRBTreeGreaterEqual>();
      while (MxMDPxLevel *pxLevel = i.iterate()) {
	MxValue cPx = pxLevel->price();
	if (px < cPx) break;
	qty += pxLevel->data().qty;
      }
    }
    return qty;
  }

private:
  template <int Direction, typename Fill, typename Limit>
  bool match(
      MxDateTime transactTime, MxValue px, MxValue &qty, MxValue &cumQty,
      Fill &&fill, Limit &&limit) {
    auto i = m_pxLevels.iterator<Direction>();
    while (MxMDPxLevel *pxLevel = i.iterate()) {
      if (!limit(px, pxLevel->price())) break;
      bool v = pxLevel->match(transactTime, qty, cumQty, fill);
      if (!pxLevel->data().qty) i.del();
      if (!v) return false;
      if (!qty) break;
    }
    return true;
  }

private:
  bool updateL1Bid(MxMDL1Data &l1Data, MxMDL1Data &delta);
  bool updateL1Ask(MxMDL1Data &l1Data, MxMDL1Data &delta);

  void updateLast(MxDateTime stamp,
      MxValue lastPx, MxValue lastQty, MxValue nv, MxValue openQty);

  void matched(MxValue price, MxValue qty);

  void pxLevel_(
      MxDateTime transactTime, bool delta,
      MxValue price, MxValue qty, MxUInt nOrders, MxFlags flags,
      const MxMDInstrHandler *handler,
      MxValue &d_qty, MxUInt &d_nOrders,
      const MxMDPxLevelFn *&, ZmRef<MxMDPxLevel> &);

  void addOrder_(
      MxMDOrder *order, MxDateTime transactTime,
      const MxMDInstrHandler *handler,
      const MxMDPxLevelFn *&fn, ZmRef<MxMDPxLevel> &pxLevel);
  void delOrder_(
      MxMDOrder *order, MxDateTime transactTime,
      const MxMDInstrHandler *handler,
      const MxMDPxLevelFn *&fn, ZmRef<MxMDPxLevel> &pxLevel);

  void reset(MxDateTime transactTime, MxMDOrderFn);

  void updateNDP(
      MxNDP oldPxNDP, MxNDP oldQtyNDP, MxNDP pxNDP, MxNDP qtyNDP,
      const MxMDOrderNDPFn &);

  MxMDOrderBook		*m_orderBook;
  MxEnum		m_side;
  MxMDOBSideData	m_data;
  ZmRef<MxMDPxLevel>	m_mktLevel;
  MxMDPxLevels		m_pxLevels;
};

MxEnum MxMDPxLevel_::side() const
{
  return m_obSide->side();
}

struct MxMDFeedOB : public ZmPolymorph {
  virtual ~MxMDFeedOB() { }
  virtual void subscribe(MxMDOrderBook *, MxMDInstrHandler *) { }
  virtual void unsubscribe(MxMDOrderBook *, MxMDInstrHandler *) { }
};

class MxMDAPI MxMDOrderBook : public ZmObject, public MxMDSharded {
  MxMDOrderBook(const MxMDOrderBook &) = delete;
  MxMDOrderBook &operator =(const MxMDOrderBook &) = delete;

friend MxMDLib;
friend MxMDShard;
friend MxMDVenue;
friend MxMDInstrument;
friend MxMDOBSide;
friend MxMDPxLevel_;

  struct KeyAccessor {
    static const MxInstrKey &get(const MxMDOrderBook *ob) {
      return ob->key();
    }
  };

  struct VenueSegmentAccessor {
    static uint128_t get(const MxMDOrderBook *ob) {
      return venueSegment(ob->venueID(), ob->segment());
    }
  };

  MxMDOrderBook( // single leg
    MxMDShard *shard, 
    MxMDVenue *venue,
    MxID segment, ZuString id,
    MxMDInstrument *instrument,
    MxMDTickSizeTbl *tickSizeTbl,
    const MxMDLotSizes &lotSizes,
    MxMDInstrHandler *handler);

  MxMDOrderBook( // multi-leg
    MxMDShard *shard, 
    MxMDVenue *venue,
    MxID segment, ZuString id, MxNDP pxNDP, MxNDP qtyNDP,
    MxUInt legs, const ZmRef<MxMDInstrument> *instruments,
    const MxEnum *sides, const MxRatio *ratios,
    MxMDTickSizeTbl *tickSizeTbl,
    const MxMDLotSizes &lotSizes);

public:
  MxMDLib *md() const;

  MxMDVenue *venue() const { return m_venue; };
  MxMDVenueShard *venueShard() const { return m_venueShard; };

  MxMDInstrument *instrument() const { return m_instruments[0]; }
  MxMDInstrument *instrument(MxUInt leg) const {
    if (ZuUnlikely(!*leg)) leg = 0;
    return m_instruments[leg];
  }

  MxMDOrderBook *out() const { return m_out; };

  MxID venueID() const { return m_key.venue; }
  MxID segment() const { return m_key.segment; }
  const MxIDString &id() const { return m_key.id; };
  const MxInstrKey &key() const { return m_key; }

  MxUInt legs() const { return m_legs; };
  MxEnum side(unsigned leg) const { return m_sides[leg]; }
  MxRatio ratio(unsigned leg) const { return m_ratios[leg]; }

  unsigned pxNDP() const { return m_l1Data.pxNDP; }
  unsigned qtyNDP() const { return m_l1Data.qtyNDP; }

  MxMDTickSizeTbl *tickSizeTbl() const { return m_tickSizeTbl; }

  const MxMDLotSizes &lotSizes() const { return m_lotSizes; }

  const MxMDL1Data &l1Data() const { return m_l1Data; }

  MxMDOBSide *bids() const { return m_bids.ptr(); };
  MxMDOBSide *asks() const { return m_asks.ptr(); };

  void l1(MxMDL1Data &data);
  void pxLevel(MxEnum side, MxDateTime transactTime,
      bool delta, MxValue price, MxValue qty, MxUInt nOrders,
      MxFlags flags = MxFlags());
  void l2(MxDateTime stamp, bool updateL1 = false);

  ZmRef<MxMDOrder> addOrder(
    ZuString orderID, MxDateTime transactTime,
    MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags);
  ZmRef<MxMDOrder> modifyOrder(
    ZuString orderID, MxDateTime transactTime,
    MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags);
  ZmRef<MxMDOrder> reduceOrder(
    ZuString orderID, MxDateTime transactTime,
    MxEnum side, MxValue reduceQty);
  ZmRef<MxMDOrder> cancelOrder(
    ZuString orderID, MxDateTime transactTime, MxEnum side);

  void reset(MxDateTime transactTime, MxMDOrderFn = MxMDOrderFn());

  void addTrade(ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);
  void correctTrade(ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);
  void cancelTrade(ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);

  void update(
      const MxMDTickSizeTbl *tickSizeTbl, const MxMDLotSizes &lotSizes,
      MxDateTime transactTime);

  void subscribe(MxMDInstrHandler *);
  void unsubscribe();

  const ZmRef<MxMDInstrHandler> &handler() const { return m_handler; }

  template <typename T = uintptr_t>
  T libData() const { return (T)m_libData.load_(); }
  template <typename T>
  void libData(T v) { m_libData.store_((uintptr_t)v); }

  template <typename T = uintptr_t>
  T appData() const { return static_cast<T>(m_appData.load_()); }
  template <typename T>
  void appData(T v) { m_appData.store_(static_cast<uintptr_t>(v)); }

  template <typename T = MxMDFeedOB>
  ZuIs<MxMDFeedOB, T, ZmRef<T> &> feedOB() {
    ZmRef<T> *ZuMayAlias(ptr) = (ZmRef<T> *)&m_feedOB;
    return *ptr;
  }

private:
  template <int Direction, typename Fill, typename Leave,
	   typename Limit, typename Side>
  bool match_(MxDateTime transactTime, MxValue px, MxValue qty,
      Fill &&fill, Leave &&leave, Limit &&limit, Side &&side) {
    MxValue cumQty = 0;
    auto l = [transactTime, px, &qty, &cumQty, &fill, &limit, &side](
	     auto &l, MxMDOrderBook *ob) mutable {
      if (ZuLikely(!ob->m_in))
	return side(ob)->template match<Direction>(
	    transactTime, px, qty, cumQty, fill, limit);
      for (MxMDOrderBook *inOB = ob->m_in; inOB; inOB = inOB->m_next) {
	if (!l(l, inOB)) return false;
	if (!qty) break;
      }
      return true;
    };
    bool v = l(l, this); // recursive lambda
    leave(qty, cumQty);
    return v;
  }

public:
  // fill(leavesQty, cumQty, px, qty, MxMDOrder *contra) -> bool
  //   /* fill(...) is called repeatedly for each contra order */
  //   /* Note: leavesQty/cumQty are pre-fill, not post-fill */
  // leave(leavesQty, cumQty) /* called on completion */

  // match order (CLOB)
  template <typename Fill, typename Leave>
  bool match(
      MxDateTime transactTime, MxEnum side, MxValue px, MxValue qty,
      Fill &&fill, Leave &&leave) {
    if (ZuUnlikely(!*px)) {
      if (side == MxSide::Buy) {
	return match_<ZmRBTreeGreaterEqual>(
	    transactTime, px, qty, fill, leave,
	    [](MxValue, MxValue) { return true; },
	    [](MxMDOrderBook *ob) { return ob->m_asks; });
      } else {
	return match_<ZmRBTreeLessEqual>(
	    transactTime, px, qty, fill, leave,
	    [](MxValue, MxValue) { return true; },
	    [](MxMDOrderBook *ob) { return ob->m_bids; });
      }
    } else {
      if (side == MxSide::Buy) {
	return match_<ZmRBTreeGreaterEqual>(
	    transactTime, px, qty, fill, leave,
	    [](MxValue px, MxValue cPx) { return px >= cPx; },
	    [](MxMDOrderBook *ob) { return ob->m_asks; });
      } else {
	return match_<ZmRBTreeLessEqual>(
	    transactTime, px, qty, fill, leave,
	    [](MxValue px, MxValue cPx) { return px <= cPx; },
	    [](MxMDOrderBook *ob) { return ob->m_bids; });
      }
    }
  }

  void updateLast(MxDateTime stamp, MxEnum makerSide,
      MxValue lastPx, MxValue lastQty, MxValue nv, MxValue openQty);

private:
  static uint128_t venueSegment(MxID venue, MxID segment) {
    return (((uint128_t)venue)<<64U) | (uint128_t)segment;
  }

  void pxLevel_(
      MxEnum side, MxDateTime transactTime, bool delta,
      MxValue price, MxValue qty, MxUInt nOrders, MxFlags flags,
      MxValue *d_qty, MxUInt *d_nOrders);

  void reduceOrder_(MxMDOrder *order, MxDateTime transactTime,
      MxValue reduceQty);
  void modifyOrder_(MxMDOrder *order, MxDateTime transactTime,
      MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags);
  void cancelOrder_(MxMDOrder *order, MxDateTime transactTime);

  void addOrder_(
    MxMDOrder *order, MxDateTime transactTime,
    const MxMDInstrHandler *handler,
    const MxMDPxLevelFn *&fn, ZmRef<MxMDPxLevel> &pxLevel);
  void delOrder_(
    MxMDOrder *order, MxDateTime transactTime,
    const MxMDInstrHandler *handler,
    const MxMDPxLevelFn *&fn, ZmRef<MxMDPxLevel> &pxLevel);

  void reset_(MxMDPxLevel *pxLevel, MxDateTime transactTime);

  void deletedOrder_(MxMDOrder *order, MxDateTime transactTime) {
    if (m_handler) m_handler->deletedOrder(order, transactTime);
  }
  void deletedPxLevel_(MxMDPxLevel *pxLevel, MxDateTime transactTime) {
    if (m_handler) m_handler->deletedPxLevel(pxLevel, transactTime);
  }

  void updateNDP(MxNDP pxNDP, MxNDP qtyNDP, const MxMDOrderNDPFn &);

  void map(unsigned inRank, MxMDOrderBook *outOB);

private:
  MxMDVenue			*m_venue;
  MxMDVenueShard		*m_venueShard;

  MxMDOrderBook			*m_in = 0;	// head of input list
  unsigned			m_rank = 0;	// rank in output list
  MxMDOrderBook			*m_next = 0;	// next in output list
  MxMDOrderBook			*m_out = 0;	// output order book

  MxInstrKey			m_key;
  MxUInt			m_legs;
  MxMDInstrument		*m_instruments[MxMDNLegs];
  MxEnum			m_sides[MxMDNLegs];
  MxRatio			m_ratios[MxMDNLegs];

  ZmRef<MxMDFeedOB>		m_feedOB;

  MxMDTickSizeTbl		*m_tickSizeTbl;
  MxMDLotSizes		 	m_lotSizes;

  MxMDL1Data			m_l1Data;

  ZmRef<MxMDOBSide>		m_bids;
  ZmRef<MxMDOBSide>		m_asks;

  ZmRef<MxMDInstrHandler>	m_handler;

  ZmAtomic<uintptr_t>		m_libData = 0;
  ZmAtomic<uintptr_t>		m_appData = 0;
};

MxMDOBSide *MxMDOrder_::bids_(const MxMDOrderBook *ob)
{
  if (ZuLikely(ob)) return ob->bids();
  return 0;
}
MxMDOBSide *MxMDOrder_::asks_(const MxMDOrderBook *ob)
{
  if (ZuLikely(ob)) return ob->asks();
  return 0;
}

MxMDOBSide *MxMDOrder_::obSide() const
{
  return m_data.side == MxSide::Buy ?
    m_orderBook->bids() : m_orderBook->asks();
}

inline MxMDOrderID2Ref MxMDOrder_::OrderID2Accessor(const MxMDOrder_ *o)
{
  return MxMDOrderID2Ref(o->orderBook()->key(), o->m_id);
}
inline MxMDOrderID3Ref MxMDOrder_::OrderID3Accessor(const MxMDOrder_ *o)
{
  return MxMDOrderID3Ref(o->orderBook()->key(), o->m_data.side, o->m_id);
}

MxNDP MxMDOBSide::pxNDP() const { return m_orderBook->pxNDP(); }
MxNDP MxMDOBSide::qtyNDP() const { return m_orderBook->qtyNDP(); }

void MxMDPxLevel_::updateLast(
    MxDateTime stamp, MxValue lastQty, MxValue nv, MxValue openQty)
{
  m_obSide->updateLast(stamp, m_price, lastQty, nv, openQty);
}

void MxMDOBSide::updateLast(MxDateTime stamp,
    MxValue lastPx, MxValue lastQty, MxValue nv, MxValue openQty)
{
  m_orderBook->updateLast(stamp, m_side, lastPx, lastQty, nv, openQty);
}

// instruments

class MxMDAPI MxMDDerivatives : public ZmObject {
  MxMDDerivatives(const MxMDDerivatives &) = delete;
  MxMDDerivatives &operator =(const MxMDDerivatives &) = delete;

friend MxMDInstrument;

  struct Futures_HeapID {
    static constexpr const char *id() { return "MxMDLib.Futures"; }
  };
  using Futures =
    ZmRBTree<MxFutKey,			// mat
      ZmRBTreeVal<MxMDInstrument *,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeHeapID<Futures_HeapID,
	    ZmRBTreeLock<ZmPLock> > > > >;
  struct Options_HeapID {
    static constexpr const char *id() { return "MxMDLib.Options"; }
  };
  using MxOptKey =
    ZmRBTree<MxOptKey,			// mat, putCall, strike
      ZmRBTreeVal<MxMDInstrument *,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeHeapID<Options_HeapID,
	    ZmRBTreeLock<ZmPLock> > > > >;

  MxMDDerivatives() { }

public:
  MxMDInstrument *future(const MxFutKey &key) const
    { return m_futures.findVal(key); }
  bool allFutures(ZmFn<void(MxMDInstrument *)>) const;

  MxMDInstrument *option(const MxOptKey &key) const
    { return m_options.findVal(key); }
  bool allOptions(ZmFn<void(MxMDInstrument *)>) const;

private:
  void add(MxMDInstrument *);
  void del(MxMDInstrument *);

  Futures		m_futures;
  Options		m_options;
};

class MxMDAPI MxMDInstrument : public ZmObject, public MxMDSharded {
  MxMDInstrument(const MxMDInstrument &) = delete;
  MxMDInstrument &operator =(const MxMDInstrument &) = delete;

friend MxMDLib;
friend MxMDShard;

  struct KeyAccessor {
    static const MxInstrKey &get(const MxMDInstrument *instrument) {
      return instrument->key();
    }
  };

  // FIXME
  struct OrderBooks_HeapID : public ZmHeapSharded {
    static constexpr const char *id() { return "MxMDInstrument.OrderBooks"; }
  };
  typedef ZmRBTree<ZmRef<MxMDOrderBook>,
	    ZmRBTreeKey<MxMDOrderBook::VenueSegmentAccessor,
	      ZmRBTreeUnique<true,
		ZmRBTreeObject<ZuNull,
		  ZmRBTreeLock<ZmNoLock,
		    ZmRBTreeHeapID<OrderBooks_HeapID> > > > > > OrderBooks;

  MxMDInstrument(MxMDShard *shard, const MxInstrKey &key,
      const MxMDInstrRefData &refData);

public:
  MxMDLib *md() const;

  MxID primaryVenue() const { return m_key.venue; }
  MxID primarySegment() const { return m_key.segment; }
  const MxIDString &id() const { return m_key.id; }
  const MxInstrKey &key() const { return m_key; }

  const MxMDInstrRefData &refData() const { return m_refData; }

  MxMDInstrument *underlying() const { return m_underlying; }
  MxMDDerivatives *derivatives() const { return m_derivatives; }

  void update(
      const MxMDInstrRefData &refData, MxDateTime transactTime,
      MxMDOrderNDPFn = MxMDOrderNDPFn());

  void subscribe(MxMDInstrHandler *);
  void unsubscribe();

  const ZmRef<MxMDInstrHandler> &handler() const { return m_handler; }

  ZmRef<MxMDOrderBook> orderBook(MxID venue) const {
    if (ZmRef<MxMDOrderBook> ob =
	m_orderBooks.readIterator<ZmRBTreeGreaterEqual>(
	  MxMDOrderBook::venueSegment(venue, MxID())).iterateKey())
      if (ob && ob->venueID() == venue) return ob;
    return (MxMDOrderBook *)nullptr;
  }
  ZmRef<MxMDOrderBook> orderBook(MxID venue, MxID segment) const {
    if (ZmRef<MxMDOrderBook> ob = m_orderBooks.findKey(
	  MxMDOrderBook::venueSegment(venue, segment)))
      return ob;
    return (MxMDOrderBook *)nullptr;
  }
  template <typename L> // (MxMDOrderBook *) -> bool
  bool allOrderBooks(L l) const {
    auto i = m_orderBooks.readIterator();
    while (const ZmRef<MxMDOrderBook> &ob = i.iterateKey())
      if (!l(ob)) return false;
    return true;
  }

  ZmRef<MxMDOrderBook> addOrderBook(const MxInstrKey &key,
    MxMDTickSizeTbl *tickSizeTbl, const MxMDLotSizes &lotSizes,
    MxDateTime transactTime);
  void delOrderBook(MxID venue, MxID segment, MxDateTime transactTime);

private:
  template <typename L>
  L keys_(L l) const {
    l(MxUniKey{.id = m_key.id, .venue = m_key.venue, .segment = m_key.segment});
    if (*m_refData.idSrc)
      l(MxUniKey{.id = m_refData.symbol, .src = m_refData.idSrc});
    if (*m_refData.altIDSrc)
      l(MxUniKey{.id = m_refData.altSymbol, .src = m_refData.altIDSrc});
    return l;
  }
public:
  template <typename L> // (MxUniKey)
  void keys(L l_) const {
    auto l = keys_(ZuMv(l_));
    if (m_underlying && *m_refData.mat) {
      if (*m_refData.strike)
	m_underlying->keys_([l = ZuMv(l),
	    refData = &m_refData](MxUniKey key) mutable {
	  key.mat = refData->mat;
	  key.putCall = refData->putCall;
	  key.strike = refData->strike;
	  l(key);
	});
      else
	m_underlying->keys_([l = ZuMv(l),
	    refData = &m_refData](MxUniKey key) mutable {
	  key.mat = refData->mat;
	  l(key);
	});
    }
  }

  template <typename T = uintptr_t>
  T libData() const { return (T)m_libData.load_(); }
  template <typename T>
  void libData(T v) { m_libData.store_((uintptr_t)v); }

  template <typename T = uintptr_t>
  T appData() const { return static_cast<T>(m_appData.load_()); }
  template <typename T>
  void appData(T v) { m_appData.store_(static_cast<uintptr_t>(v)); }

private:
  void addOrderBook_(MxMDOrderBook *);
  ZmRef<MxMDOrderBook> findOrderBook_(MxID venue, MxID segment);
  ZmRef<MxMDOrderBook> delOrderBook_(MxID venue, MxID segment);

  void underlying(MxMDInstrument *u) { m_underlying = u; }
  void addDerivative(MxMDInstrument *d) {
    if (!m_derivatives) m_derivatives = new MxMDDerivatives();
    m_derivatives->add(d);
  }
  void delDerivative(MxMDInstrument *d) {
    if (!m_derivatives) return;
    m_derivatives->del(d);
  }

  void update_(const MxMDInstrRefData &, const MxMDOrderNDPFn &);

  MxInstrKey			m_key;

  MxMDInstrRefData		m_refData;

  MxMDInstrument		*m_underlying = 0;
  ZmRef<MxMDDerivatives>	m_derivatives;

  OrderBooks			m_orderBooks;

  ZmRef<MxMDInstrHandler>  	m_handler;

  ZmAtomic<uintptr_t>		m_libData = 0;
  ZmAtomic<uintptr_t>		m_appData = 0;
};

// feeds

class MxMDAPI MxMDFeed : public ZmPolymorph {
  MxMDFeed(const MxMDFeed &) = delete;
  MxMDFeed &operator =(const MxMDFeed &) = delete;

friend MxMDCore;
friend MxMDLib;

public:
  MxMDFeed(MxMDLib *md, MxID id, unsigned level);

  struct IDAccessor {
    static MxID get(const MxMDFeed *f) { return f->id(); }
  };

  MxMDLib *md() const { return m_md; }
  MxID id() const { return m_id; }
  unsigned level() const { return m_level; }

  void connected();
  void disconnected();

protected:
  virtual void start();
  virtual void stop();
  virtual void final();

  virtual void addOrderBook(MxMDOrderBook *, MxDateTime transactTime);
  virtual void delOrderBook(MxMDOrderBook *, MxDateTime transactTime);

private:
  MxMDLib	*m_md;
  MxID		m_id;
  uint8_t	m_level;	// 1, 2 or 3
};

// venues

class MxMDAPI MxMDVenueShard : public ZuObject {
friend MxMDVenue;
friend MxMDOrderBook;
friend MxMDOBSide;
friend MxMDPxLevel_;

  using Orders2 =
    ZmHash<ZmRef<MxMDOrder>,
      ZmHashKey<MxMDOrder::OrderID2Accessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmNoLock,
	    ZmHashHeapID<MxMDOrders2_HeapID>>>>>;
  using Orders3 =
    ZmHash<ZmRef<MxMDOrder>,
      ZmHashKey<MxMDOrder::OrderID3Accessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmNoLock,
	    ZmHashHeapID<MxMDOrders3_HeapID>>>>>;

  MxMDVenueShard(MxMDVenue *venue, MxMDShard *shard);

public:
  MxMDLib *md() const;
  MxMDVenue *venue() const { return m_venue; }
  MxMDShard *shard() const { return m_shard; }
  unsigned id() const;
  MxEnum orderIDScope() const { return m_orderIDScope; }

  ZmRef<MxMDOrderBook> addCombination(
      MxID segment, ZuString orderBookID,
      MxNDP pxNDP, MxNDP qtyNDP,
      MxUInt legs, const ZmRef<MxMDInstrument> *instruments,
      const MxEnum *sides, const MxRatio *ratios,
      MxMDTickSizeTbl *tickSizeTbl, const MxMDLotSizes &lotSizes,
      MxDateTime transactTime);
  void delCombination(MxID segment, ZuString orderBookID,
      MxDateTime transactTime);

private:
  void addOrder(MxMDOrder *order);
  template <typename OrderID>
  ZmRef<MxMDOrder> findOrder(
      const MxInstrKey &obKey, MxEnum side, OrderID &&orderID);
  template <typename OrderID>
  ZmRef<MxMDOrder> delOrder(
      const MxInstrKey &obKey, MxEnum side, OrderID &&orderID);

  void addOrder2(MxMDOrder *order) { m_orders2->add(order); }
  template <typename OrderID> ZmRef<MxMDOrder> findOrder2(
      const MxInstrKey &obKey, OrderID &&orderID) {
    return m_orders2->findKey(ZuFwdTuple(obKey, ZuFwd<OrderID>(orderID)));
  }
  template <typename OrderID> ZmRef<MxMDOrder> delOrder2(
      const MxInstrKey &obKey, OrderID &&orderID) {
    return m_orders2->delKey(ZuFwdTuple(obKey, ZuFwd<OrderID>(orderID)));
  }

  void addOrder3(MxMDOrder *order) { m_orders3->add(order); }
  template <typename OrderID> ZmRef<MxMDOrder> findOrder3(
      const MxInstrKey &obKey, MxEnum side, OrderID &&orderID) {
    return m_orders3->findKey(ZuFwdTuple(obKey, side, ZuFwd<OrderID>(orderID)));
  }
  template <typename OrderID> ZmRef<MxMDOrder> delOrder3(
      const MxInstrKey &obKey, MxEnum side, OrderID &&orderID) {
    return m_orders3->delKey(ZuFwdTuple(obKey, side, ZuFwd<OrderID>(orderID)));
  }

  MxMDVenue		*m_venue;
  MxMDShard		*m_shard;
  MxEnum		m_orderIDScope;
  ZmRef<Orders2>	m_orders2;
  ZmRef<Orders3>	m_orders3;
};

class MxMDAPI MxMDVenue : public ZmObject {
  MxMDVenue(const MxMDVenue &) = delete;
  MxMDVenue &operator =(const MxMDVenue &) = delete;

friend MxMDLib;
friend MxMDVenueShard;
friend MxMDOrderBook;

  typedef ZtArray<ZuRef<MxMDVenueShard> > Shards;

  struct TickSizeTbls_HeapID {
    static constexpr const char *id() { return "MxMDVenue.TickSizeTbls"; }
  };
  typedef ZmRBTree<ZmRef<MxMDTickSizeTbl>,
	    ZmRBTreeIndex<MxMDTickSizeTbl::IDAccessor,
	      ZmRBTreeUnique<true,
		ZmRBTreeObject<ZuNull,
		  ZmRBTreeLock<ZmPRWLock,
		    ZmRBTreeHeapID<TickSizeTbls_HeapID> > > > > > TickSizeTbls;
  struct Segment_IDAccessor {
    static MxID get(const MxMDSegment &s) { return s.id; }
  };
  struct Segments_ID {
    static constexpr const char *id() { return "MxMDVenue.Segments"; }
  };
  using Segments =
    ZmHash<MxMDSegment,
      ZmHashKey<Segment_IDAccessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmNoLock,
	    ZmHashID<Segments_ID>>>>>;
  typedef ZmPLock SegmentsLock;
  typedef ZmGuard<SegmentsLock> SegmentsGuard;
  typedef ZmReadGuard<SegmentsLock> SegmentsReadGuard;

  using Orders1 =
    ZmHash<ZmRef<MxMDOrder>,
      ZmHashKey<MxMDOrder::OrderID1Accessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmPLock,
	    ZmHashHeapID<MxMDOrders1_HeapID>>>>>;

public:
  MxMDVenue(MxMDLib *md, MxMDFeed *feed, MxID id,
      MxEnum orderIDScope = MxMDOrderIDScope::Default,
      MxFlags flags = 0);

  struct IDAccessor {
    static MxID get(const MxMDVenue *v) { return v->id(); }
  };

  MxMDLib *md() const { return m_md; }
  MxMDFeed *feed() const { return m_feed; }
  MxID id() const { return m_id; }
  MxEnum orderIDScope() const { return m_orderIDScope; }
  MxFlags flags() const { return m_flags; }

  MxMDVenueShard *shard(const MxMDShard *shard) const;

private:
  void loaded_(bool b) { m_loaded = b; }
public:
  bool loaded() const { return m_loaded; }

  ZmRef<MxMDTickSizeTbl> addTickSizeTbl(ZuString id, MxNDP pxNDP);
  ZmRef<MxMDTickSizeTbl> tickSizeTbl(ZuString id) const {
    return m_tickSizeTbls.findKey(id);
  }
  bool allTickSizeTbls(ZmFn<void(MxMDTickSizeTbl *)>) const;

  bool allSegments(ZmFn<void(const MxMDSegment &)>) const;

  MxMDSegment tradingSession(MxID segmentID = MxID()) const {
    SegmentsReadGuard guard(m_segmentsLock);
    Segments::Node *node = m_segments->find(segmentID);
    if (!node) return MxMDSegment();
    return node->key();
  }
  void tradingSession(MxMDSegment segment);

  void modifyOrder(ZuString orderID, MxDateTime transactTime,
      MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags,
      ZmFn<void(MxMDOrder *)> fn = ZmFn<void(MxMDOrder *)>());
  void reduceOrder(ZuString orderID,
      MxDateTime transactTime, MxValue reduceQty,
      ZmFn<void(MxMDOrder *)> fn = ZmFn<void(MxMDOrder *)>()); // qty -= reduceQty
  void cancelOrder(ZuString orderID, MxDateTime transactTime,
      ZmFn<void(MxMDOrder *)> fn = ZmFn<void(MxMDOrder *)>());

private:
  ZmRef<MxMDTickSizeTbl> addTickSizeTbl_(ZuString id, MxNDP pxNDP);

  void tradingSession_(const MxMDSegment &segment) {
    SegmentsGuard guard(m_segmentsLock);
    Segments::Node *node = m_segments->find(segment.id);
    if (!node) {
      m_segments->add(segment);
      return;
    }
    node->key() = segment;
  }

  void addOrder(MxMDOrder *order) { m_orders1->add(order); }
  template <typename OrderID>
  ZmRef<MxMDOrder> findOrder(OrderID &&orderID) {
    return m_orders1->findKey(ZuFwd<OrderID>(orderID));
  }
  template <typename OrderID>
  ZmRef<MxMDOrder> delOrder(OrderID &&orderID) {
    return m_orders1->delKey(ZuFwd<OrderID>(orderID));
  }

  MxMDVenueShard *shard_(unsigned i) const { return m_shards[i]; }

  MxMDLib		*m_md;
  MxMDFeed		*m_feed;
  MxID			m_id;
  MxEnum		m_orderIDScope;
  MxFlags		m_flags;
  Shards		m_shards;

  TickSizeTbls	 	m_tickSizeTbls;
  SegmentsLock		m_segmentsLock;
    ZmRef<Segments>	  m_segments;
  ZmAtomic<unsigned>	m_loaded = 0;

  ZmRef<Orders1>	m_orders1;
};

inline void MxMDVenueShard::addOrder(MxMDOrder *order)
{
  switch ((int)m_orderIDScope) {
    case MxMDOrderIDScope::Venue: m_venue->addOrder(order); break;
    case MxMDOrderIDScope::OrderBook: addOrder2(order); break;
    case MxMDOrderIDScope::OBSide: addOrder3(order); break;
  }
}
template <typename OrderID>
inline ZmRef<MxMDOrder> MxMDVenueShard::findOrder(
    const MxInstrKey &obKey, MxEnum side, OrderID &&orderID)
{
  switch ((int)m_orderIDScope) {
    case MxMDOrderIDScope::Venue:
      return m_venue->findOrder(ZuFwd<OrderID>(orderID));
    case MxMDOrderIDScope::OrderBook:
      return findOrder2(obKey, ZuFwd<OrderID>(orderID));
    case MxMDOrderIDScope::OBSide:
      return findOrder3(obKey, side, ZuFwd<OrderID>(orderID));
  }
  return 0;
}
template <typename OrderID>
inline ZmRef<MxMDOrder> MxMDVenueShard::delOrder(
    const MxInstrKey &obKey, MxEnum side, OrderID &&orderID)
{
  switch ((int)m_orderIDScope) {
    case MxMDOrderIDScope::Venue:
      return m_venue->delOrder(ZuFwd<OrderID>(orderID));
    case MxMDOrderIDScope::OrderBook:
      return delOrder2(obKey, ZuFwd<OrderID>(orderID));
    case MxMDOrderIDScope::OBSide:
      return delOrder3(obKey, side, ZuFwd<OrderID>(orderID));
  }
  return 0;
}

// shard

class MxMDAPI MxMDShard : public ZuObject, public ZmShard {
friend MxMDLib;

  // FIXME
  struct Instruments_HeapID : public ZmHeapSharded {
    static constexpr const char *id() { return "MxMDShard.Instruments"; }
  };
  using Instruments =
    ZmHash<MxMDInstrument *,
      ZmHashKey<MxMDInstrument::KeyAccessor,
	ZmHashObject<ZuObject,
	  ZmHashLock<ZmNoLock,
	    ZmHashHeapID<Instruments_HeapID>>>>>;

  // FIXME
  struct OrderBooks_HeapID : public ZmHeapSharded {
    static constexpr const char *id() { return "MxMDShard.OrderBooks"; }
  };
  using OrderBooks =
    ZmHash<MxMDOrderBook *,
      ZmHashKey<MxMDOrderBook::KeyAccessor,
	ZmHashObject<ZuObject,
	  ZmHashLock<ZmNoLock,
	    ZmHashHeapID<OrderBooks_HeapID>>>>>;

  MxMDShard(MxMDLib *md, ZmScheduler *sched, unsigned id, unsigned sid) :
      ZmShard(sched, sid), m_md(md), m_id(id) {
    m_instruments = new Instruments();
    m_orderBooks = new OrderBooks();
  }

public:
  MxMDLib *md() const { return m_md; }
  unsigned id() const { return m_id; }

  ZmRef<MxMDInstrument> instrument(const MxInstrKey &key) const {
    return m_instruments->findKey(key);
  }
  bool allInstruments(ZmFn<void(MxMDInstrument *)>) const;
  ZmRef<MxMDInstrument> addInstrument(ZmRef<MxMDInstrument> instr,
      const MxInstrKey &key, const MxMDInstrRefData &refData,
      MxDateTime transactTime);

  ZmRef<MxMDOrderBook> orderBook(const MxInstrKey &key) const {
    return m_orderBooks->findKey(key);
  }
  bool allOrderBooks(ZmFn<void(MxMDOrderBook *)>) const;

private:
  void addInstrument(MxMDInstrument *instrument) {
    m_instruments->add(instrument);
  }
  void delInstrument(MxMDInstrument *instrument) {
    m_instruments->del(instrument->key());
  }
  void addOrderBook(MxMDOrderBook *ob) {
    m_orderBooks->add(ob);
  }
  void delOrderBook(MxMDOrderBook *ob) {
    m_orderBooks->del(ob->key());
  }

  MxMDLib		*m_md = nullptr;
  unsigned		m_id;
  ZmRef<Instruments>	m_instruments;
  ZmRef<OrderBooks>	m_orderBooks;
};

MxMDLib *MxMDOrderBook::md() const { return shard()->md(); }

MxMDLib *MxMDInstrument::md() const { return shard()->md(); }

unsigned MxMDVenueShard::id() const { return m_shard->id(); }

typedef ZmHandle<MxMDInstrument> MxMDInstrHandle;
typedef ZmHandle<MxMDOrderBook> MxMDOBHandle;

// library

class MxMDLib_JNI;
class MxMDAPI MxMDLib : public ZmPolymorph {
  MxMDLib(const MxMDLib &) = delete;
  MxMDLib &operator =(const MxMDLib &) = delete;

friend MxMDTickSizeTbl;
friend MxMDPxLevel_;
friend MxMDOrderBook;
friend MxMDInstrument;
friend MxMDFeed;
friend MxMDVenue;
friend MxMDVenueShard;
friend MxMDShard;

friend MxMDLib_JNI;

protected:
  MxMDLib(ZmScheduler *);

  void init_(void *);

  static MxMDLib *init(ZuString cf, ZmFn<void(ZmScheduler *)> schedInitFn);

public:
  static MxMDLib *instance();

  static MxMDLib *init(ZuString cf) {
    return init(cf, ZmFn<void(ZmScheduler *)>());
  }

  virtual void start() = 0;
  virtual void stop() = 0;
  virtual void final() = 0;

  virtual bool record(ZuString path) = 0;
  virtual ZtString stopRecording() = 0;

  virtual bool replay(ZuString path,
      MxDateTime begin = MxDateTime(),
      bool filter = true) = 0;
  virtual ZtString stopReplaying() = 0;

  virtual void startTimer(MxDateTime begin = MxDateTime()) = 0;
  virtual void stopTimer() = 0;

  virtual void dumpTickSizes(ZuString path, MxID venue = MxID()) = 0;
  virtual void dumpInstruments(
      ZuString path, MxID venue = MxID(), MxID segment = MxID()) = 0;
  virtual void dumpOrderBooks(
      ZuString path, MxID venue = MxID(), MxID segment = MxID()) = 0;

  unsigned nShards() const { return m_shards.length(); }
  MxMDShard *shard(unsigned i) const { return m_shards[i]; }
  template <typename L>
  ZuNotMutableFn<L> shard(unsigned i, L l) const {
    MxMDShard *shard = m_shards[i];
    m_scheduler->invoke(shard->sid(), [l = ZuMv(l), shard]() { l(shard); });
  }
  template <typename L>
  ZuMutableFn<L> shard(unsigned i, L l) const {
    MxMDShard *shard = m_shards[i];
    m_scheduler->invoke(shard->sid(),
	[l = ZuMv(l), shard]() mutable { l(shard); });
  }

  // shardRun(), shardInvoke() do not inject the initial MxMDShard * parameter
  template <typename ...Args>
  void shardRun(unsigned i, Args &&...args)
    { m_shards[i]->run(ZuFwd<Args>(args)...); }
  template <typename ...Args>
  void shardInvoke(unsigned i, Args &&...args)
    { m_shards[i]->invoke(ZuFwd<Args>(args)...); }

  void sync(); // synchronize with all shards

  void raise(ZmRef<ZeEvent> e) const;

  void addFeed(MxMDFeed *feed);
  void addVenue(MxMDVenue *venue);

  void addVenueMapping(MxMDVenueMapKey key, MxMDVenueMapping map);
  MxMDVenueMapping venueMapping(MxMDVenueMapKey key);

  void loaded(MxMDVenue *venue);

  void subscribe(MxMDLibHandler *);
  void unsubscribe();

  ZmRef<MxMDLibHandler> handler() const {
    SubReadGuard guard(m_subLock);
    return m_handler;
  }

  // commands

  // add command
  virtual void addCmd(ZuString name, ZuString syntax,
      ZcmdFn fn, ZtString brief, ZtString usage) = 0;

  // single instrument / order book lookup
  static ZuString lookupSyntax();
  static ZuString lookupOptions();

  MxUniKey parseInstrument(ZvCf *args, unsigned index) const;
  bool lookupInstrument(
      const MxUniKey &key, bool instrRequired, ZmFn<void(MxMDInstrument *)> fn) const;
  MxUniKey parseOrderBook(ZvCf *args, unsigned index) const;
  bool lookupOrderBook(
      const MxUniKey &key, bool instrRequired, bool obRequired,
      ZmFn<void(MxMDInstrument *, MxMDOrderBook *)> fn) const;

  // CLI time format (using local timezone)
  typedef ZuBoxFmt<ZuBox<unsigned>, ZuFmt::Right<6> > TimeFmt;
  TimeFmt timeFmt(MxDateTime t) {
    return ZuBox<unsigned>((t + m_tzOffset).hhmmss()).fmt(ZuFmt::Right<6>());
  }

private:
  typedef ZmPRWLock RWLock;
  typedef ZmGuard<RWLock> Guard;
  typedef ZmReadGuard<RWLock> ReadGuard;

  typedef ZmPLock SubLock;
  typedef ZmGuard<SubLock> SubGuard;
  typedef ZmReadGuard<SubLock> SubReadGuard;

friend ZmShard;
  template <typename ...Args>
  void run(unsigned sid, Args &&...args) {
    m_scheduler->run(sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(unsigned sid, Args &&...args) {
    m_scheduler->invoke(sid, ZuFwd<Args>(args)...);
  }

  typedef ZtArray<ZuRef<MxMDShard> > Shards;

  MxMDShard *shard_(unsigned i) const { return m_shards[i]; }

  ZmRef<MxMDInstrument> addInstrument(
      MxMDShard *shard, ZmRef<MxMDInstrument> instr,
      const MxInstrKey &key, const MxMDInstrRefData &refData,
      MxDateTime transactTime);

  ZmRef<MxMDTickSizeTbl> addTickSizeTbl(
      MxMDVenue *venue, ZuString id, MxNDP pxNDP);
  void resetTickSizeTbl(MxMDTickSizeTbl *tbl);
  void addTickSize(MxMDTickSizeTbl *tbl,
      MxValue minPrice, MxValue maxPrice, MxValue tickSize);

  void updateInstrument(
      MxMDInstrument *instrument, const MxMDInstrRefData &refData,
      MxDateTime transactTime, const MxMDOrderNDPFn &);
  void addInstrIndices(
      MxMDInstrument *, const MxMDInstrRefData &, MxDateTime transactTime);
  void delInstrIndices(
      MxMDInstrument *, const MxMDInstrRefData &);

  ZmRef<MxMDOrderBook> addOrderBook(
      MxMDInstrument *instrument, MxInstrKey key,
      MxMDTickSizeTbl *tickSizeTbl, MxMDLotSizes lotSizes,
      MxDateTime transactTime);
      
  ZmRef<MxMDOrderBook> addCombination(
      MxMDVenueShard *venueShard, MxID segment, ZuString id,
      MxNDP pxNDP, MxNDP qtyNDP,
      MxUInt legs, const ZmRef<MxMDInstrument> *instruments,
      const MxEnum *sides, const MxRatio *ratios,
      MxMDTickSizeTbl *tickSizeTbl, const MxMDLotSizes &lotSizes,
      MxDateTime transactTime);

  void updateOrderBook(MxMDOrderBook *ob,
      const MxMDTickSizeTbl *tickSizeTbl, const MxMDLotSizes &lotSizes,
      MxDateTime transactTime);

  void delOrderBook(MxMDInstrument *instrument, MxID venue, MxID segment,
      MxDateTime transactTime);
  void delCombination(MxMDVenueShard *venueShard, MxID segment, ZuString id,
      MxDateTime transactTime);

  void tradingSession(MxMDVenue *venue, MxMDSegment segment);

  void l1(const MxMDOrderBook *ob, const MxMDL1Data &l1Data);

  void pxLevel(const MxMDOrderBook *ob, MxEnum side, MxDateTime transactTime,
      bool delta, MxValue price, MxValue qty, MxUInt nOrders, MxFlags flags);

  void l2(const MxMDOrderBook *ob, MxDateTime stamp, bool updateL1);

  void addOrder(const MxMDOrderBook *ob,
      ZuString orderID, MxDateTime transactTime,
      MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags);
  void modifyOrder(const MxMDOrderBook *ob,
      ZuString orderID, MxDateTime transactTime,
      MxEnum side, MxUInt rank, MxValue price, MxValue qty, MxFlags flags);
  void cancelOrder(const MxMDOrderBook *ob,
      ZuString orderID, MxDateTime transactTime, MxEnum side);

  void resetOB(const MxMDOrderBook *ob, MxDateTime transactTime);

  void addTrade(const MxMDOrderBook *ob, ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);
  void correctTrade(const MxMDOrderBook *ob, ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);
  void cancelTrade(const MxMDOrderBook *ob, ZuString tradeID,
      MxDateTime transactTime, MxValue price, MxValue qty);

  // primary indices

  struct AllInstruments_HeapID {
    static constexpr const char *id() { return "MxMDLib.AllInstruments"; }
  };
  using AllInstruments =
    ZmHash<ZmRef<MxMDInstrument>,
      ZmHashKey<MxMDInstrument::KeyAccessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmPLock,
	    ZmHashHeapID<AllInstruments_HeapID>>>>>;

  struct AllOrderBooks_HeapID {
    static constexpr const char *id() { return "MxMDLib.AllOrderBooks"; }
  };
  using AllOrderBooks =
    ZmHash<ZmRef<MxMDOrderBook>,
      ZmHashKey<MxMDOrderBook::KeyAccessor,
	ZmHashObject<ZuNull,
	  ZmHashLock<ZmPLock,
	    ZmHashHeapID<AllOrderBooks_HeapID>>>>>;

  // secondary indices

  struct Instruments_HeapID {
    static constexpr const char *id() { return "MxMDLib.Instruments"; }
  };
  using Instruments =
    ZmHashKV<MxSymKey, MxMDInstrument *,
      ZmHashObject<ZuNull,
	ZmHashLock<ZmPLock,
	  ZmHashHeapID<Instruments_HeapID>>>>;

  // feeds, venues

  struct Feeds_HeapID {
    static constexpr const char *id() { return "MxMDLib.Feeds"; }
  };
  using Feeds =
    ZmRBTree<ZmRef<MxMDFeed>,
      ZmRBTreeIndex<MxMDFeed::IDAccessor,
	ZmRBTreeUnique<true,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmPLock,
	      ZmRBTreeHeapID<Feeds_HeapID>>>>>>;

  struct Venues_HeapID {
    static constexpr const char *id() { return "MxMDLib.Venues"; }
  };
  using Venues =
    ZmRBTree<ZmRef<MxMDVenue>,
      ZmRBTreeIndex<MxMDVenue::IDAccessor,
	ZmRBTreeUnique<true,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmPLock,
	      ZmRBTreeHeapID<Venues_HeapID>>>>>>;

  struct VenueMap_HeapID {
    static constexpr const char *id() { return "MxMDLib.VenueMap"; }
  };
  using VenueMap =
    ZmRBTree<MxMDVenueMapKey,
      ZmRBTreeVal<MxMDVenueMapping,
	ZmRBTreeUnique<true,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmPLock,
	      ZmRBTreeHeapID<VenueMap_HeapID>>>>>>;

public:
  MxMDInstrHandle instrument(const MxInstrKey &key) const {
    if (ZmRef<MxMDInstrument> instr = m_allInstruments->findKey(key))
      return MxMDInstrHandle{ZuMv(instr)};
    return MxMDInstrHandle{};
  }
private:
  ZmRef<MxMDInstrument> instrument_(const MxInstrKey &key) const {
    return m_allInstruments->findKey(key);
  }
public:
  MxMDInstrHandle instrument(const MxSymKey &key) const {
    if (ZmRef<MxMDInstrument> instr = m_instruments->findVal(key))
      return MxMDInstrHandle{ZuMv(instr)};
    return MxMDInstrHandle{};
  }
private:
  ZmRef<MxMDInstrument> instrument_(const MxUniKey &key) const {
    ZmRef<MxMDInstrument> instr;
    if (*key.src)
      instr = m_instruments->findVal(MxSymKey{key.id, key.src});
    else
      instr = m_allInstruments->findKey(
	  MxInstrKey{key.id, key.venue, key.segment});
    if (instr && *key.mat && instr->derivatives()) {
      if (*key.strike)
	instr = instr->derivatives()->option(
	    MxOptKey{key.mat, key.putCall, key.strike});
      else
	instr = instr->derivatives()->future(MxFutKey{key.mat});
    }
    return ZuMv(instr);
  }
public:
  MxMDInstrHandle instrument(const MxUniKey &key) const {
    ZmRef<MxMDInstrument> instr = instrument_(key);
    if (instr) return MxMDInstrHandle{ZuMv(instr)};
    return MxMDInstrHandle{};
  }
  MxMDInstrHandle instrument(
      const MxInstrKey &key, unsigned shardID) const {
    if (ZmRef<MxMDInstrument> instr = m_allInstruments->findKey(key))
      return MxMDInstrHandle{ZuMv(instr)};
    return MxMDInstrHandle{m_shards[shardID % m_shards.length()]};
  }
  template <typename L> void instrInvoke(
      const MxInstrKey &key, L l) const {
    if (ZmRef<MxMDInstrument> instr = m_allInstruments->findKey(key))
      instr->shard()->invoke(
	  [l = ZuMv(l), instr = ZuMv(instr)]() mutable { l(instr); });
    else
      l((MxMDInstrument *)nullptr);
  }
  template <typename L> void instrInvoke(
      const MxSymKey &key, L l) const {
    if (ZmRef<MxMDInstrument> instr = m_instruments->findVal(key))
      instr->shard()->invoke(
	  [l = ZuMv(l), instr = ZuMv(instr)]() mutable { l(instr); });
    else
      l((MxMDInstrument *)nullptr);
  }
  template <typename L>
  void instrInvoke(const MxUniKey &key, L l) const {
    if (*key.mat) {
      auto l_ = [key = key, l = ZuMv(l)](MxMDInstrument *instr) mutable {
	if (ZuLikely(instr && instr->derivatives())) {
	  if (*key.strike)
	    instr = instr->derivatives()->option(
		MxOptKey{key.mat, key.putCall, key.strike});
	  else
	    instr = instr->derivatives()->future(MxFutKey{key.mat});
	}
	l(instr);
      };
      if (*key.src)
	instrInvoke(MxSymKey{key.id, key.src}, ZuMv(l_));
      else
	instrInvoke(MxInstrKey{key.id, key.venue, key.segment}, ZuMv(l_));
    } else {
      if (*key.src)
	instrInvoke(MxSymKey{key.id, key.src}, ZuMv(l));
      else
	instrInvoke(MxInstrKey{key.id, key.venue, key.segment}, ZuMv(l));
    }
  }
  bool allInstruments(ZmFn<void(MxMDInstrument *)>) const;
  unsigned instrCount() const { return m_allInstruments->count_(); }

  MxMDOBHandle orderBook(const MxInstrKey &key) const {
    if (ZmRef<MxMDOrderBook> ob = m_allOrderBooks->findKey(key))
      return MxMDOBHandle{ob};
    return MxMDOBHandle{};
  }
private:
  ZmRef<MxMDOrderBook> orderBook_(const MxInstrKey &key) const {
    return m_allOrderBooks->findKey(key);
  }
public:
  MxMDOBHandle orderBook(const MxUniKey &key) const {
    ZmRef<MxMDInstrument> instr = instrument_(key);
    if (!instr) return MxMDOBHandle{};
    if (ZmRef<MxMDOrderBook> ob = instr->orderBook(key.venue, key.segment))
      return MxMDOBHandle{ob};
    return MxMDOBHandle{};
  }
  MxMDOBHandle orderBook(
      const MxInstrKey &key, unsigned shardID) const {
    if (ZmRef<MxMDOrderBook> ob = m_allOrderBooks->findKey(key))
      return MxMDOBHandle{ob};
    return MxMDOBHandle{m_shards[shardID % m_shards.length()]};
  }
  template <typename L>
  void obInvoke(const MxInstrKey &key, L l) const {
    if (ZmRef<MxMDOrderBook> ob = m_allOrderBooks->findKey(key))
      ob->shard()->invoke([l = ZuMv(l), ob = ZuMv(ob)]() mutable { l(ob); });
    else
      l((MxMDOrderBook *)nullptr);
  }
  template <typename L>
  void obInvoke(const MxUniKey &key, L l) const {
    instrInvoke(key, [key = key, l = ZuMv(l)](MxMDInstrument *instr) mutable {
	if (!instr)
	  l((MxMDOrderBook *)nullptr);
	else
	  l(instr->orderBook(key.venue, key.segment));
      });
  }
  bool allOrderBooks(ZmFn<void(MxMDOrderBook *)>) const;

  ZmRef<MxMDFeed> feed(MxID id) const {
    return m_feeds.findKey(id);
  }
  bool allFeeds(ZmFn<void(MxMDFeed *)>) const;

  ZmRef<MxMDVenue> venue(MxID id) const {
    return m_venues.findKey(id);
  }
  bool allVenues(ZmFn<void(MxMDVenue *)>) const;

  template <typename T = uintptr_t>
  T libData() const { return (T)m_libData.load_(); }
  template <typename T>
  void libData(T v) { m_libData.store_((uintptr_t)v); }

  template <typename T = uintptr_t>
  T appData() const { return static_cast<T>(m_appData.load_()); }
  template <typename T>
  void appData(T v) { m_appData.store_(static_cast<uintptr_t>(v)); }

private:
  ZmScheduler		*m_scheduler = 0;
  Shards		m_shards;

  ZmRef<AllInstruments>	m_allInstruments;
  ZmRef<AllOrderBooks>	m_allOrderBooks;
  ZmRef<Instruments>	m_instruments;
  Feeds			m_feeds;
  Venues		m_venues;
  VenueMap		m_venueMap;

  RWLock		m_refDataLock;	// serializes updates to containers

  int			m_tzOffset = 0;

  SubLock		m_subLock;
    ZmRef<MxMDLibHandler> m_handler;

  ZmAtomic<uintptr_t>	m_libData = 0;
  ZmAtomic<uintptr_t>	m_appData = 0;
};

inline void MxMDFeed::connected() {
  m_md->handler()->connected(this);
}
inline void MxMDFeed::disconnected() {
  if (m_md) m_md->handler()->disconnected(this);
}

inline MxMDVenueShard *MxMDVenue::shard(const MxMDShard *shard) const {
  return m_shards[shard->id()];
}

inline MxMDLib *MxMDVenueShard::md() const {
  return m_venue->md();
}

template <typename Fill>
inline bool MxMDPxLevel_::match(MxDateTime transactTime,
    MxValue &qty, MxValue &cumQty, Fill fill)
{
  auto ob = m_obSide->orderBook();
  int side = m_obSide->side();
  auto md = ob->md();
  MxValue oldQty = m_data.qty;
  MxValue oldNOrders = m_data.nOrders;
  bool v = true;

  auto i = this->m_orders.iterator();
  while (MxMDOrder *contra = i.iterate()) {
    MxValue cQty = contra->data().qty;
    MxValue nv;
    if (cQty <= qty) {
      if (!(v = !fill(qty, cumQty, m_price, cQty, contra))) break;
      cumQty += cQty;
      nv = (MxValNDP{m_price, m_pxNDP} * MxValNDP{cQty, m_qtyNDP}).value;
      deletedOrder_(contra, transactTime);
      --m_data.nOrders;
      m_data.qty -= cQty;
      qty -= cQty;
      updateLast(transactTime, cQty, nv, qty);
      md->cancelOrder(ob, contra->id(), transactTime, side);
      i.del();
      if (!qty) break;
    } else {
      if (!(v = !fill(qty, cumQty, m_price, qty, contra))) break;
      cumQty += qty;
      nv = (MxValNDP{m_price, m_pxNDP} * MxValNDP{qty, m_qtyNDP}).value;
      contra->updateQty_(cQty -= qty);
      m_data.qty -= qty;
      updateLast(transactTime, qty, nv, 0);
      {
	const auto &contraData = contra->data();
	md->modifyOrder(ob, contra->id(), transactTime,
	    side, contraData.rank, m_price, cQty, contraData.flags);
      }
      qty = 0;
      break;
    }
  }
  if (m_data.qty != oldQty) {
    m_obSide->matched(m_price, m_data.qty - oldQty);
    if (MxMDOrderBook *out = ob->out())
      out->pxLevel_(
	  side, transactTime, true, m_price,
	  m_data.qty - oldQty, m_data.nOrders - oldNOrders, m_data.flags, 0, 0);
  }
  return v;
}

#endif /* MxMD_HH */
