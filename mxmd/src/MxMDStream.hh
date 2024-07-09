//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD library internal API

#ifndef MxMDStream_HH
#define MxMDStream_HH

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuLargest.hh>
#include <zlib/ZuPOD.hh>

#include <zlib/ZmFn.hh>

#include <zlib/ZiFile.hh>
#include <zlib/ZiMultiplex.hh>

#include <lz4.h>

#include <mxbase/MxBase.hh>
#include <mxbase/MxQueue.hh>

#include <mxmd/MxMDTypes.hh>

// App must conform to the following interface:
#if 0
struct App {
  // Rx

  // begin message shift - return ptr to message buffer
  const Frame *shift();
  // complete message shift
  void shift2();

  // Tx

  // begin message push - allocate message buffer
  void *push(unsigned size);
  // fill message header, return ptr to body
  void *out(void *ptr, unsigned length, unsigned type,
      int shardID, ZuTime stamp);
  // complete message push
  void push2();
};
#endif

#pragma pack(push, 1)

namespace MxMDStream {

  namespace Type {
    MxEnumValues("MxMDStream.Type",
	AddVenue,
	AddTickSizeTbl, ResetTickSizeTbl, AddTickSize,
	TradingSession,
	AddInstrument, UpdateInstrument,
	AddOrderBook, DelOrderBook,
	AddCombination, DelCombination,
	UpdateOrderBook,
	L1, PxLevel, L2,
	AddOrder, ModifyOrder, CancelOrder,
	ResetOB,
	AddTrade, CorrectTrade, CancelTrade,
	RefDataLoaded,

	// control events follow
	HeartBeat,		// heart beat
	Wake,			// ITC wake-up
	EndOfSnapshot,		// end of snapshot
	Login,			// TCP login
	ResendReq);		// UDP resend request

    using CSVMap = Map;
  }

  struct FileHdr {
    enum {
      Magic = 0xc0def17e		// "code file"
    };

    FileHdr(ZuString id_, uint16_t vmajor_, uint16_t vminor_) :
	magic(Magic), vmajor(vmajor_), vminor(vminor_) {
      unsigned n = id_.length();
      if (n >= sizeof(id)) n = sizeof(id) - 1;
      memcpy(id, id_.data(), n);
      memset(&id[n], 0, sizeof(id) - n);
    }

    struct IOError { };
    struct InvalidFmt { };
    FileHdr(ZiFile &file, ZeError *e) {
      int n;
      if ((n = file.read(this, sizeof(FileHdr), e)) < (int)sizeof(FileHdr)) {
	if (n != Zi::EndOfFile) throw IOError();
	file.seek(0);
	new (this) FileHdr("RMD", 6, 0);
	return;
      }
      if (magic == Magic) return;
      throw InvalidFmt();
    }

    uint32_t	magic;	// must be Magic
    char	id[12];	// "RMD"
    uint16_t	vmajor;	// version - major number
    uint16_t	vminor;	// version - minor number
  };

  struct HdrData {
    uint64_t	seqNo = 0;
    uint32_t	nsec = 0;
    uint16_t	len = 0;	// exclusive of HdrData
    uint8_t	type = 0;
    uint8_t	shard = 0xff;	// 0xff is null
  };
  struct Hdr : public HdrData {
    template <typename ...Args>
    Hdr(Args &&...args) : HdrData{ZuFwd<Args>(args)...} { }

    void *body() { return (void *)&this[1]; }
    const void *body() const { return (const void *)&this[1]; }

    template <typename T> const T &as() const {
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(body());
      return *ptr;
    }
    template <typename T> T &as() {
      T *ZuMayAlias(ptr) = reinterpret_cast<T *>(body());
      return *ptr;
    }

    template <typename T> void pad() {
      if (ZuUnlikely(len < sizeof(T)))
	memset(((char *)body()) + len, 0, sizeof(T) - len);
    }

    bool scan(unsigned length) const {
      return sizeof(Hdr) + len > length;
    }
  };

  typedef MxInstrKey Key;

  struct AddVenue {
    enum { Code = Type::AddVenue };
    MxID		id;
    MxFlags		flags;
    MxEnum		orderIDScope;
  };

  struct AddTickSizeTbl {
    enum { Code = Type::AddTickSizeTbl };
    MxID		venue;
    MxIDString		id;
    MxNDP		pxNDP;
  };

  struct ResetTickSizeTbl {
    enum { Code = Type::ResetTickSizeTbl };
    MxID		venue;
    MxIDString		id;
  };

  struct AddTickSize {
    enum { Code = Type::AddTickSize };
    MxID		venue;
    MxValue		minPrice;
    MxValue		maxPrice;
    MxValue		tickSize;
    MxIDString		id;
    MxNDP		pxNDP;
  };

  struct TradingSession {
    enum { Code = Type::TradingSession };
    MxDateTime		stamp;
    MxID		venue;
    MxID		segment;
    MxEnum		session;
  };

  struct AddInstrument {
    enum { Code = Type::AddInstrument };
    MxDateTime		transactTime;
    Key			key;
    MxMDInstrRefData	refData;
  };

  struct UpdateInstrument {
    enum { Code = Type::UpdateInstrument };
    MxDateTime		transactTime;
    Key			key;
    MxMDInstrRefData	refData;
  };

  struct AddOrderBook {
    enum { Code = Type::AddOrderBook };
    MxDateTime		transactTime;
    Key			key;
    Key			instrument;
    MxMDLotSizes	lotSizes;
    MxIDString		tickSizeTbl;
    MxNDP		qtyNDP;
  };

  struct DelOrderBook {
    enum { Code = Type::DelOrderBook };
    MxDateTime		transactTime;
    Key			key;
  };

  struct AddCombination {
    enum { Code = Type::AddCombination };
    MxDateTime		transactTime;
    Key			key;
    MxUInt		legs;
    Key			instruments[MxMDNLegs];
    MxRatio		ratios[MxMDNLegs];
    MxMDLotSizes	lotSizes;
    MxIDString		tickSizeTbl;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
    MxEnum		sides[MxMDNLegs];
  };

  struct DelCombination {
    enum { Code = Type::DelCombination };
    MxDateTime		transactTime;
    Key			key;
  };

  struct UpdateOrderBook {
    enum { Code = Type::UpdateOrderBook };
    MxDateTime		transactTime;
    Key			key;
    MxMDLotSizes	lotSizes;
    MxIDString		tickSizeTbl;
  };

  struct L1 {
    enum { Code = Type::L1 };
    Key			key;
    MxMDL1Data		data;
  };

  struct PxLevel {
    enum { Code = Type::PxLevel };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxUInt		nOrders;
    MxFlags		flags;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
    MxEnum		side;
    uint8_t		delta;
  };

  struct L2 {
    enum { Code = Type::L2 };
    MxDateTime		stamp;
    Key			key;
    uint8_t		updateL1;
  };

  struct AddOrder {
    enum { Code = Type::AddOrder };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxInt		rank;
    MxFlags		flags;
    MxIDString		orderID;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
    MxEnum		side;
  };

  struct ModifyOrder {
    enum { Code = Type::ModifyOrder };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxInt		rank;
    MxFlags		flags;
    MxIDString		orderID;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
    MxEnum		side;
  };

  struct CancelOrder {
    enum { Code = Type::CancelOrder };
    MxDateTime		transactTime;
    Key			key;
    MxIDString		orderID;
    MxEnum		side;
  };

  struct ResetOB {
    enum { Code = Type::ResetOB };
    MxDateTime		transactTime;
    Key			key;
  };

  struct AddTrade {
    enum { Code = Type::AddTrade };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxIDString		tradeID;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
  };

  struct CorrectTrade {
    enum { Code = Type::CorrectTrade };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxIDString		tradeID;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
  };

  struct CancelTrade {
    enum { Code = Type::CancelTrade };
    MxDateTime		transactTime;
    Key			key;
    MxValue		price;
    MxValue		qty;
    MxIDString		tradeID;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
  };

  struct RefDataLoaded {
    enum { Code = Type::RefDataLoaded };
    MxID		venue;
  };

  // transport / session control messages follow

  struct HeartBeat { // heart beat / time synchronization
    enum { Code = Type::HeartBeat };
    MxDateTime		stamp;
  };

  struct Wake { // ITC wake-up
    enum { Code = Type::Wake };
    MxID		id;
  };

  struct EndOfSnapshot { // end of snapshot
    enum { Code = Type::EndOfSnapshot };
    MxID		id;
    MxSeqNo		seqNo;
    uint8_t		ok;
  };

  struct Login { // TCP login
    enum { Code = Type::Login };
    MxIDString		username;
    MxIDString		password;
  };
  
  struct ResendReq { // UDP resend request
    enum { Code = Type::ResendReq };
    MxSeqNo		seqNo;
    MxUInt		count;
  };
  
  typedef ZuLargest<
      AddVenue,
      AddTickSizeTbl,
      AddTickSize,
      ResetTickSizeTbl,
      TradingSession,
      AddInstrument,
      UpdateInstrument,
      AddOrderBook,
      DelOrderBook,
      AddCombination,
      DelCombination,
      UpdateOrderBook,
      L1,
      PxLevel,
      L2,
      AddOrder,
      ModifyOrder,
      CancelOrder,
      ResetOB,
      AddTrade,
      CorrectTrade,
      CancelTrade,
      RefDataLoaded,
      HeartBeat,
      Wake,
      EndOfSnapshot,
      Login,
      ResendReq> Largest;

  struct Buf {
    char	data[sizeof(Hdr) + sizeof(Largest)];
  };
}

#pragma pack(pop)

namespace MxMDStream {

  struct Msg_HeapID {
    static constexpr const char *id() { return "MxMDStream.Msg"; }
  };

  struct MsgData : public ZuPolymorph {
    ZiSockAddr	addr;
  };
  template <typename Heap>
  struct Msg_ : public Heap, public ZuPOD_<Buf, MsgData> {
    Msg_() { }

    Hdr &hdr() { return this->template as<Hdr>(); }
    const Hdr &hdr() const { return this->template as<Hdr>(); }

    void *body() { return hdr().body(); }
    const void *body() const { return hdr().body(); }

    unsigned length() const { return sizeof(Hdr) + hdr().len; }
  };

  typedef Msg_<ZmHeap<Msg_HeapID, sizeof(Msg_<ZuNull>)> > Msg;

  template <typename T, typename App>
  void *push(App &app, int shardID) {
    void *ptr = app.push(sizeof(Hdr) + sizeof(T));
    if (ZuUnlikely(!ptr)) return 0;
    return app.out(ptr, sizeof(T), T::Code, shardID);
  }

#ifdef DeclFn
#undef DeclFn
#endif
#define DeclFn(Fn, Type) \
  template <typename App, typename ...Args> \
  bool Fn(App &app, Args &&...args) { \
    void *ptr = push<Type>(app, -1); \
    if (ZuUnlikely(!ptr)) return false; \
    new (ptr) Type{ZuFwd<Args>(args)...}; \
    app.push2(); \
    return true; \
  }

  DeclFn(addVenue, AddVenue)

  DeclFn(addTickSizeTbl, AddTickSizeTbl)
  DeclFn(resetTickSizeTbl, ResetTickSizeTbl)
  DeclFn(addTickSize, AddTickSize)

  DeclFn(tradingSession, TradingSession)

  DeclFn(refDataLoaded, RefDataLoaded)

  DeclFn(heartBeat, HeartBeat)
  DeclFn(wake, Wake)
  DeclFn(endOfSnapshot, EndOfSnapshot)
  DeclFn(login, Login)
  DeclFn(resendReq, ResendReq)

#undef DeclFn
#define DeclFn(Fn, Type) \
  template <typename App, typename ...Args> \
  bool Fn(App &app, MxInt shardID, Args &&...args) { \
    void *ptr = push<Type>(app, shardID); \
    if (ZuUnlikely(!ptr)) return false; \
    new (ptr) Type{ZuFwd<Args>(args)...}; \
    app.push2(); \
    return true; \
  }

  DeclFn(addInstrument, AddInstrument)
  DeclFn(updateInstrument, UpdateInstrument)
  DeclFn(addOrderBook, AddOrderBook)
  DeclFn(delOrderBook, DelOrderBook)

  // special processing for AddCombination's instruments/sides/ratios fields
  template <typename App,
	   typename Key_, typename Instrument, typename TickSizeTbl>
  bool addCombination(App &app, MxInt shardID, MxDateTime transactTime,
      const Key_ &key, unsigned legs, const Instrument *instruments,
      const MxRatio *ratios, const MxMDLotSizes &lotSizes,
      const TickSizeTbl &tickSizeTbl,
      MxNDP pxNDP, MxNDP qtyNDP, const MxEnum *sides) {
    void *ptr = push<AddCombination>(app, shardID);
    if (ZuUnlikely(!ptr)) return false;
    AddCombination *data = new (ptr) AddCombination{
      transactTime, key, legs, {},
      {}, lotSizes, tickSizeTbl, pxNDP, qtyNDP, {}};
    for (unsigned i = 0; i < legs; i++) {
      data->instruments[i] = instruments[i];
      data->ratios[i] = ratios[i];
      data->sides[i] = sides[i];
    }
    app.push2();
    return true;
  }

  DeclFn(delCombination, DelCombination)
  DeclFn(updateOrderBook, UpdateOrderBook)
  DeclFn(l1, L1)
  DeclFn(pxLevel, PxLevel)
  DeclFn(l2, L2)
  DeclFn(addOrder, AddOrder)
  DeclFn(modifyOrder, ModifyOrder)
  DeclFn(cancelOrder, CancelOrder)
  DeclFn(resetOB, ResetOB)

  DeclFn(addTrade, AddTrade)
  DeclFn(correctTrade, CorrectTrade)
  DeclFn(cancelTrade, CancelTrade)

#undef DeclFn

  // ensure passed lambdas are stateless and match required signature
  template <typename Cxn, typename L> struct IOLambda_ {
    typedef void (*Fn)(Cxn *, ZmRef<MxQMsg>, ZiIOContext &);
    enum { OK = ZuInspect<L, Fn>::Converts };
  };
  template <typename Cxn, typename L, bool = IOLambda_<Cxn, L>::OK>
  struct IOLambda;
  template <typename Cxn, typename L> struct IOLambda<Cxn, L, true> {
    typedef void T;
    static void invoke(ZiIOContext &io) {
      // no, this->x does not imply evaluating (*this).x; the reverse is true
      (*reinterpret_cast<const L *>(0))(
	  static_cast<Cxn *>(io.cxn), io.fn.mvObject<MxQMsg>(), io);
    }
  };

  namespace UDP {
    template <typename Cxn>
    void send(Cxn *cxn, ZmRef<MxQMsg> msg, const ZiSockAddr &addr) {
      msg->ptr<Msg>()->addr = addr;
      cxn->send(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<MxQMsg>(),
	  [](MxQMsg *msg, ZiIOContext &io) {
	    if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	  }}, msg->ptr<Msg>()->ptr(), msg->length, 0,
	  msg->ptr<Msg>()->addr);
      }});
    }
    template <typename Cxn, typename L>
    typename IOLambda<Cxn, L>::T send(
	Cxn *cxn, ZmRef<MxQMsg> msg, const ZiSockAddr &addr, L l) {
      msg->ptr<Msg>()->addr = addr;
      cxn->send(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<MxQMsg>(),
	  [](MxQMsg *msg, ZiIOContext &io) {
	    if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	    IOLambda<Cxn, L>::invoke(io);
	  }}, msg->ptr<Msg>()->ptr(), msg->length, 0,
	  msg->ptr<Msg>()->addr);
      }});
    }
    template <typename Cxn, typename L>
    typename IOLambda<Cxn, L>::T recv(
	ZmRef<MxQMsg> msg, ZiIOContext &io, L l) {
      MxQMsg *msg_ = msg.ptr();
      io.init(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) {
	msg->length = (io.offset += io.length);
	msg->ptr<Msg>()->addr = io.addr;
	IOLambda<Cxn, L>::invoke(io);
      }},
      msg_->ptr<Msg>()->ptr(), msg_->ptr<Msg>()->size(), 0);
    }
  }

  namespace TCP {
    template <typename Cxn>
    void send(Cxn *cxn, ZmRef<MxQMsg> msg) {
      cxn->send(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<MxQMsg>(),
	  [](MxQMsg *msg, ZiIOContext &io) {
	    io.offset += io.length;
	  }}, msg->ptr<Msg>()->ptr(), msg->length, 0);
      }});
    }
    template <typename Cxn, typename L>
    typename IOLambda<Cxn, L>::T send(
	Cxn *cxn, ZmRef<MxQMsg> msg, L l) {
      cxn->send(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<MxQMsg>(),
	  [](MxQMsg *msg, ZiIOContext &io) {
	    if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	    IOLambda<Cxn, L>::invoke(io);
	  }}, msg->ptr<Msg>()->ptr(), msg->length, 0);
      }});
    }

    // FIXME - use ZiRx
    template <typename Cxn, typename L>
    typename IOLambda<Cxn, L>::T recv(
	ZmRef<MxQMsg> msg, ZiIOContext &io, L l) {
      MxQMsg *msg_ = msg.ptr();
      io.init(ZiIOFn{ZuMv(msg), [](MxQMsg *msg, ZiIOContext &io) -> uintptr_t {
	unsigned len = io.offset += io.length;
	io.length = 0;
	if (ZuUnlikely(len < sizeof(Hdr))) return 0;
	Hdr &hdr = msg->ptr<Msg>()->as<Hdr>();
	unsigned msgLen = sizeof(Hdr) + hdr.len;
	if (ZuUnlikely(msgLen > msg->ptr<Msg>()->size())) {
	  ZeLOG(Error, "MxMDStream::recv TCP message too big / corrupt");
	  io.disconnect();
	  return 0;
	}
	if (ZuLikely(len < msgLen)) return 0;
	msg->length = msgLen;
	IOLambda<Cxn, L>::invoke(io);
	if (io.offset = len - msgLen) {
	  memmove(io.ptr, io.ptr + msgLen, io.offset);
	  return io.offset;
	}
	return 0;
      }}, msg_->ptr<Msg>()->ptr(), msg_->ptr<Msg>()->size(), 0);
    }
  }
}

#endif /* MxMDStream_HH */
