//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series
// - chunked into blocks
// - compressed (see ZdfCompress)
// - fixed (ZuFixed) and floating point (64bit double)
// - indexable (monotonically increasing, e.g. time series)
// - support archiving of old data with purge()
// - in-memory or file-backed (see ZdfMem / ZdfFile)

#ifndef ZdfSeries_HH
#define ZdfSeries_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZuUnion.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/ZuSort.hh>
#include <zlib/ZuSearch.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmPQueue.hh>

#include <zlib/ZtEnum.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfBlk.hh>

namespace Zdf {

template <typename> class Series;

namespace RdrState {
  ZtEnumValues(RdrState, int8_t,
    Stopped	= 0,	// seek / find completed
    Loading	= 1,	// read called, loading block data
    Reading	= 2,	// reading historical data
    Live	= 3,	// reading, waiting for live data
    Stopping	= 4);	// stopping while reading

  constexpr const bool stopped(T v) { return v == Stopped; }
  constexpr const bool reading(T v) { return v >= Loading; }
  constexpr const bool live(T v) { return v == Live; }
}

// from an application perspective a reader is either stopped() or reading()
// - internally there are additional transient states
//   (Loading, Stopping), and a further distinction is made between
//   readers of live and historical data (Live)
// - a reader can also be failed in any state due to error

// Reader internals - possible state paths:
//
// (*) - permitted while failed
//
// null > Stopped		ctor
// Stopped > null		dtor (*)
//
// Stopped > Stopped		seek, find (via init) - clears failed flag (*)
// Stopped > Stopped		stop - idempotent
// Stopped > Loading		read - block data not cached
// Stopped > Reading		read - block data remains cached
// Loading > Reading		loaded - block data loaded, now cached
// Reading > Reading		next - read callback returns true
// Reading > Stopping		stop - stop called during read callback
// Reading > Loading		nextBlk - read hits end of block (not EOS)
// Reading > Live		live - read hits end of stream (EOS)
// Live > Live			notify - writer appends new value
// Live > Live			notifyBlk - writer appends new block
// Live > Stopping		stop - stop called while live
// Stopping > Stopped		stopped (*)
//
// error paths (failed flag is set):
//
// Loading > Stopping		loadFail - load failed
// Reading > Stopping		readFail - fatal error while Reading
// Live > Stopping		liveFail - fatal error while Live
//
// blkData pin/unpin (block data is pinned in cache while being read/written):
//
// pin		loaded
// unpin	stop | nextBlk | readFail | liveFail 
//
// historical / live reader (de-)registration:
//
// addHistReader	seek, find, liveFail
// delHistReader	live, stop
// addLiveReader	live
// delLiveReader	stop, liveFail

// RdrResult is used internally for processing reader state transitions
namespace RdrResult {
  ZtEnumValues(RdrResult, int8_t,
    OK = 0,
    Load,	// load performed
    EOS,	// end of series
    Stopped,
    Failed);
}

// the decoder determines the value type (fixed or floating point)
template <typename Decoder_>
class Reader : public ZmObject {
public:
  using Decoder = Decoder_;
  using Series = Zdf::Series<Decoder>;
  using PValue = typename Decoder::Value; // primitive value
  enum { Fixed = ZuIsExact<PValue, int64_t>{} };
  using Value = ZuIf<Fixed, ZuFixed, double>;
  using Fn = ZmFn<bool(Reader *, Value)>;
  using ErrorFn = ZmFn<void()>;
  using StopFn = ZmFn<void()>;

private:
friend Series;

  ZuAssert((!ZuIsExact<Offset, Value>{})); // ensure distinct
  using Target = ZuUnion<void, Offset, Value>;

  Reader() = delete;
  Reader(const Reader &) = delete;
  Reader &operator =(const Reader &) = delete;
  Reader(Reader &&) = delete;
  Reader &operator =(Reader &&) = delete;

protected:
  Reader(
    const Series *series, BlkOffset blkOffset, const Blk *blk, Target target)
  :
    m_series{series}, m_blkOffset{blkOffset}, m_blk{blk}, m_target{target}
  {
    ZeAssert(m_blk, (), "internal error - null blk", return);
  }

public:
  ~Reader() { }

  const Series *series() const { return m_series; }
  bool stopped() const { return RdrState::stopped(m_state); }
  bool reading() const { return RdrState::reading(m_state); }
  bool live() const { return RdrState::live(m_state); }
  bool failed() const { return m_failed; }
  NDP ndp() const {
    return m_blk ? m_blk->ndp() : NDP(0);
  }
  Offset offset() const;

  // to use seekFwd/seekRev/findFwd/findRev, the Reader must be stopped;
  // these functions return false if the Reader is actively reading

  // seek forward to offset
  bool seekFwd(Offset offset);
  // seek reverse to offset
  bool seekRev(Offset offset);

  // series must monotonically increase to use find*() (e.g. time series)

  // seek forward to >= value
  bool findFwd(Value value);
  // seek backwards to >= value
  bool findRev(Value value);

  // read values (returns false if already reading)
  bool read(Fn, ErrorFn = {});

  // stop reading (idempotent - can always be called)
  void stop(StopFn = {});

  // purge historical data up to current read position
  void purge();

private:
  bool init(BlkOffset, Blk *, Target);

  void loadBlk();
  void loaded(const Blk *blk);
  int nextBlk();		// OK|Load|EOS|Stopped|Failed
  int nextValue();		// ''

  void stopped();

  void fail();

private:
  const Series		*m_series = nullptr;
  BlkOffset		m_blkOffset = 0;
  const Blk		*m_blk = nullptr;
  RdrState::T		m_state = RdrState::Stopped;
  Target		m_target;
  Decoder		m_decoder;
  Fn			m_fn;
  ErrorFn		m_errorFn;
  StopFn		m_stopFn;
  bool			m_failed = false;
};

inline constexpr const char *Reader_HeapID() { return "Zdf.Reader"; }
template <typename Decoder>
using ReaderList_ =
  ZmList<Reader<Decoder>,
    ZmListNode<Reader<Decoder>,
      ZmListHeapID<Reader_HeapID>>>;
template <typename Decoder>
struct ReaderList : public ReaderList_<Decoder> {
  using Base = ReaderList_<Decoder>;
  using Base::Base;
};

template <typename Decoder>
using RdrListNode = typename ReaderList<Decoder>::Node;

template <typename Decoder>
inline auto node(Reader<Decoder> *ptr) {
  return static_cast<RdrListNode<Decoder> *>(ptr);
}

// RdRef is a move-only ZmRef<Reader>-derived smart pointer,
// with a RAII dtor that calls reader->stop()
template <typename Decoder>
class RdRef : public ZmRef<RdrListNode<Decoder>> {
  RdRef(const RdRef &) = delete;
  RdRef &operator =(const RdRef &) = delete;

public:
  using RdrListNode = Zdf::RdrListNode<Decoder>;
  using Ref = ZmRef<RdrListNode>;

  using Ref::operator RdrListNode *;
  using Ref::operator ->;

  RdRef() = default;
  RdRef(RdRef &&h) : Ref{ZuMv(h)} { }
  RdRef &operator =(RdRef &&h) {
    stop_();
    Ref::operator =(ZuMv(h));
    return *this;
  }
  template <typename Arg>
  RdRef(Arg &&arg) : Ref{ZuFwd<Arg>(arg)} { }
  template <typename Arg>
  RdRef &operator =(Arg &&arg) {
    stop_();
    Ref::operator =(ZuFwd<Arg>(arg));
    return *this;
  }
  ~RdRef() { stop_(); }

private:
  void stop_() {
    if (auto ptr = this->ptr_()) ptr->stop();
  }
};

// Writer main class
template <typename Decoder, typename Heap, typename PValue_>
class Writer__ : public Heap, public ZmObject {
public:
  using Encoder = Zdf::Encoder<Decoder>;
  using Series = Zdf::Series<Decoder>;
  using PValue = PValue_;

private:
  friend Series;

public:
  Writer__(Series *series) : m_series{series} { }

  Series *series() const { return m_series; }

  // append value to series, notifying any live readers
  bool write(PValue);

  // stop writing (idempotent)
  void stop();

private:
  template <typename L> void encoder(L l) { m_encoder = l(); }
  bool encode(PValue value) { return m_encoder.write(value); }
  void finish(Blk *lastBlk) {
    m_encoder.finish();
    lastBlk->sync(m_encoder, m_encoder.last());
  }

  Series	*m_series = nullptr;
  Encoder	m_encoder;
};
template <typename Decoder, typename Heap, typename PValue = Decoder::Value>
class Writer_ : public Writer__<Decoder, Heap, PValue> { };
template <typename Decoder, typename Heap>
class Writer_<Decoder, Heap, int64_t> :
  public Writer__<Decoder, Heap, int64_t>
{
  using Base = Writer__<Decoder, Heap, int64_t>;

public:
  using typename Base::Series;

  Writer_(Series *series, NDP ndp) : Base{series}, m_ndp{ndp} { }

  NDP ndp() const { return m_ndp; }

private:
  NDP		m_ndp;
};
inline constexpr const char *Writer_HeapID() { return "Zdf.Writer"; }
template <typename Decoder>
using Writer_Heap = ZmHeap<Writer_HeapID, Writer_<Decoder, ZuEmpty>>;
template <typename Decoder>
using Writer = Writer_<Decoder, Writer_Heap<Decoder>>;

// WrRef is a move-only ZmRef<Writer>-derived smart pointer,
// with a RAII dtor that calls writer->stop()
template <typename Decoder>
class WrRef : public ZmRef<Writer<Decoder>> {
  WrRef(const WrRef &) = delete;
  WrRef &operator =(const WrRef &) = delete;

public:
  using Writer = Zdf::Writer<Decoder>;
  using Ref = ZmRef<Writer>;

  using Ref::operator Writer *;
  using Ref::operator ->;

  WrRef() = default;
  WrRef(WrRef &&h) : Ref{ZuMv(h)} { }
  WrRef &operator =(WrRef &&h) {
    stop_();
    Ref::operator =(ZuMv(h));
    return *this;
  }
  template <typename Arg>
  WrRef(Arg &&arg) : Ref{ZuFwd<Arg>(arg)} { }
  template <typename Arg>
  WrRef &operator =(Arg &&arg) {
    stop_();
    Ref::operator =(ZuFwd<Arg>(arg));
    return *this;
  }
  ~WrRef() { stop_(); }

private:
  void stop_() {
    if (auto ptr = this->ptr_()) ptr->stop();
  }
};

class Store;
// template <typename O, bool TimeIndex> class DataFrame;

// each IndexBlk contains Blk[512]
inline constexpr const unsigned IndexBlkShift() { return 9; }
inline constexpr const unsigned IndexBlkSize() { return 1<<IndexBlkShift(); }
inline constexpr const unsigned IndexBlkMask() {
  return ~((~0U)<<IndexBlkShift());
}

// the series index is a skiplist (ZmPQueue) of IndexBlks
struct IndexBlk_ : public ZuObject {
  Offset	offset;			// block offset
  Blk		blks[IndexBlkSize()];

  IndexBlk_(Offset offset_) : offset{offset_} { }
};
struct IndexBlk_Fn {
  IndexBlk_	&indexBlk;

  using Key = Offset;
  static Key KeyAxor(const IndexBlk_ &indexBlk) { return indexBlk.offset; }

  ZuInline Offset key() const { return indexBlk.offset; }
  static constexpr const unsigned length() { return IndexBlkSize(); }
};
inline constexpr const char *IndexBlk_HeapID() { return "Zdf.IndexBlk"; }
using Index_ =
  ZmPQueue<IndexBlk_,
    ZmPQueueFn<IndexBlk_Fn,
      ZmPQueueNode<IndexBlk_,
	ZmPQueueStats<false,
	  ZmPQueueOverlap<false,
	    ZmPQueueBits<3,
	      ZmPQueueLevels<3,
		ZmPQueueHeapID<IndexBlk_HeapID>>>>>>>>;
struct Index : public Index_ { using Index_::Index_; };
using IndexBlk = Index::Node;

struct InternalError { };	// internal exception

template <typename Decoder_>
class Series : public ZmObject {
public:
  using Decoder = Decoder_;
  using Encoder = Zdf::Encoder<Decoder>;
  using Reader = Zdf::Reader<Decoder>;
  using RdRef = Zdf::RdRef<Decoder>;
  using Writer = Zdf::Writer<Decoder>;
  using WrRef = Zdf::WrRef<Decoder>;
  using PValue = typename Decoder::Value;
  enum { Fixed = ZuIsExact<PValue, int64_t>{} };
  using Value = ZuIf<Fixed, ZuFixed, double>;

private:
  using ReaderList = Zdf::ReaderList<Decoder>;
  using RdrListNode = Zdf::RdrListNode<Decoder>;

  using DBSeries = ZuIf<Fixed, DB::SeriesFixed, DB::SeriesFloat>;
  using DBBlk = ZuIf<Fixed, DB::BlkFixed, DB::BlkFloat>;

friend Store;
template <typename> friend class Zdf::Reader;
template <typename, typename, typename> friend class Writer__;

using ID = uint32_t;

private:
  Series(Store *store, ZdbObjRef<DBSeries> dbSeries) :
    m_store{store}, m_dbSeries{ZuMv(dbSeries)}
  {
    m_dbSeries->pin();
    // these are immutable and frequently accessed, so cache them
    m_shard = m_dbSeries->shard();
    m_id = m_dbSeries->data().id;
    m_name = m_dbSeries->data().name;
    m_epoch = m_dbSeries->data().epoch.as_time();
  }

public:
  ~Series() { m_dbSeries->unpin(); }

  Store *store() const { return m_store; }
  bool opened() const { return m_opened; }
  ZdbObject<DBSeries> *dbSeries() const { return m_dbSeries; }
  unsigned shard() const { return m_shard; }
  SeriesID id() const { return m_id; }
  ZuCSpan name() const { return m_name; }
  ZuTime epoch() const { return m_epoch; }

  // time relative to epoch (creation time of series)
private:
  static constexpr const uint64_t pow10_9() { return 1000000000UL; }
public:
  ZuFixed nsecs(ZuTime t) {
    t -= m_epoch;
    return ZuFixed{static_cast<uint64_t>(t.sec()) * pow10_9() + t.nsec(), 9};
  }
  ZuTime time(ZuFixed v) {
    auto n = v.adjust(9);
    auto p = pow10_9();
    return ZuTime{int64_t(n / p), int32_t(n % p)} + m_epoch;
  }

  // run/invoke on shard
  template <typename ...Args> void run(Args &&...args) const;
  template <typename ...Args> void invoke(Args &&...args) const;
  bool invoked() const;

  // first blkOffset (will be non-zero following a purge())
  BlkOffset head() const { return m_index.head(); }

  // number of blocks in index
  unsigned blkCount() const { return (m_lastBlkOffset + 1) - head(); }

  // check if blk is last block in series
  bool lastBlk(const Blk *blk) const { return blk == m_lastBlk; }

  // value count (length of series in #values)
  uint64_t count() const {
    if (ZuUnlikely(!blkCount())) return 0;
    auto blk = lastBlk();
    if (ZuUnlikely(!blk)) return 0;
    return blk->offset() + blk->count();
  }

  // length in bytes (compressed)
  // - estimated value - intentionally prone to mild overestimation
  // - will not return a value less than the actual value
  uint64_t length() const {
    unsigned n = blkCount();
    if (ZuUnlikely(!n)) return 0;
    auto blk = lastBlk();
    if (ZuUnlikely(!blk)) return 0;
    ZeAssert(blk->blkData,
      (id = data().id, head = m_index.head(), tail = m_index.tail()),
      "id=" << id << " head=" << head << " tail=" << tail, return n * BlkSize);
    return (n - 1) * BlkSize + blk->blkData->data().buf.length();
  }

  // Reader seek functions
public:
  RdRef seek(Offset offset = 0) const {
    uint64_t result;
    try {
      result = ZuInterSearch(blkCount(), seekFn(m_index.head(), offset));
    } catch (InternalError) {
      return {};
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    RdRef handle =
      new RdrListNode{this, blkOffset, getBlk(blkOffset), offset};
    addHistReader(handle);
    return handle;
  }
private:
  void seekFwd(Reader *reader, BlkOffset blkOffset, Offset offset) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(m_lastBlkOffset + 1) - blkOffset, seekFn(blkOffset, offset));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), offset);
  }
  void seekRev(Reader *reader, BlkOffset blkOffset, Offset offset) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(blkOffset + 1) - m_index.head(), seekFn(m_index.head(), offset));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), offset);
  }

  // Reader find functions
public:
  RdRef find(Value value) const {
    uint64_t result;
    try {
      result = ZuInterSearch(blkCount(), findFn(m_index.head(), value));
    } catch (InternalError) {
      return {};
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    RdRef handle =
      new RdrListNode{this, blkOffset, getBlk(blkOffset), value};
    addHistReader(handle);
    return handle;
  }
private:
  void findFwd(Reader *reader, BlkOffset blkOffset, Value value) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(m_lastBlkOffset + 1) - blkOffset, findFn(blkOffset, value));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), value);
  }
  void findRev(Reader *reader, BlkOffset blkOffset, Value value) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(blkOffset + 1) - m_index.head(), findFn(m_index.head(), value));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), value);
  }

private:
  // open() - open series and query blk table to fill index
  void open(ZmFn<void(ZmRef<Series>)> fn) {
    m_index.head(m_dbSeries->data().blkOffset);
    blkTbl()->template selectRows<0>(
      ZuFwdTuple(m_dbSeries->data().id), IndexBlkSize(), ZuLambda{[
	this_ = ZmMkRef(this), fn = ZuMv(fn), rowRcvd = false
      ](auto &&self, auto result, unsigned) mutable {
	if (this_->m_opened) return; // index already filled
	using Row = ZuFieldTuple<DBBlk>;
	if (result.template is<Row>()) { // fill index
	  if (!this_->open_loadBlk(result.template p<Row>())) goto opened;
	  rowRcvd = true;
	} else { // complete
	  if (!rowRcvd) goto opened;
	  rowRcvd = false;
	  this_->blkTbl()->template nextRows<0>(
	    ZuFwdTuple(this_->m_dbSeries->data().id, this_->m_lastBlkOffset),
	    false, IndexBlkSize(), ZuMv(self));
	}
	return;
      opened:
	this_->m_opened = true;
	fn(ZuMv(this_));
      }});
  }
  // load an individual block header into the index during open
  template <typename Row>
  bool open_loadBlk(const Row &row) {
    BlkOffset blkOffset = row.template p<1>();
    auto blk = setBlk(blkOffset);
    if (!blk) return false;
    blk->init(
      row.template p<2>(), row.template p<4>(),
      row.template p<5>(), row.template p<3>());
    m_lastBlk = blk;
    m_lastBlkOffset = blkOffset;
    return true;
  }

  ZdbTable<DB::BlkData> *blkDataTbl() const;
  ZuIf<Fixed, ZdbTable<DB::SeriesFixed> *, ZdbTable<DB::SeriesFloat> *>
  seriesTbl() const;
  ZuIf<Fixed, ZdbTable<DB::BlkFixed> *, ZdbTable<DB::BlkFloat> *>
  blkTbl() const;

  BlkData *newBlkData(BlkOffset blkOffset) const {
    return newBlkData(blkOffset, blkDataTbl());
  }
  BlkData *newBlkData(BlkOffset blkOffset, ZdbTable<DB::BlkData> *tbl) const {
    auto blkData = new BlkData{
      BlkData::EvictFn{this, [](auto *this_, BlkData *blkData) {
	const_cast<Series *>(this_)->unloadBlkData(blkData);
      }}, tbl, m_shard};
    new (blkData->ptr_()) DB::BlkData{
      .blkOffset = blkOffset,
      .seriesID = m_id
    };
    return blkData;
  }

public:
  template <typename ...NDP>
  ZuIfT<sizeof...(NDP) == Fixed, void>
  write(ZmFn<void(WrRef)> fn, NDP... ndp) {
    // if this is a fixed-point series, validate the ndp parameter
    if constexpr (Fixed) ZuAssert(ZuTraits<NDP...>::IsIntegral);

    if (m_writer) { fn(WrRef{}); return; }
    m_writer = new Writer{this, ndp...};
    if (!m_lastBlk) { // new series - append first block
      pushFirstBlk();
      write_newWriter(ZuMv(fn), ndp...);
    } else if (m_lastBlk->count()) { // last block is not empty - load the data
      if (!m_lastBlk->blkData) {
	ZmRef<IndexBlk> indexBlk = m_index.find(m_lastBlkOffset);
	loadBlk(m_lastBlkOffset, [
	  this, fn = ZuMv(fn), ndp..., indexBlk = ZuMv(indexBlk)
	](ZmRef<BlkData> blkData) mutable {
	  if (ZuUnlikely(!blkData)) { fn(WrRef{}); return; }
	  blkData->pin();
	  m_lastBlk->blkData = ZuMv(blkData);
	  write_loadedBlk(ZuMv(fn), ndp...);
	});
      }
    } else { // last block is empty - allocate the data
      if (!m_lastBlk->blkData)
	m_lastBlk->blkData = newBlkData(m_lastBlkOffset);
      m_lastBlk->blkData->pin();
      write_newWriter(ZuMv(fn), ndp...);
    }
  }

private:
  template <typename ...NDP>
  ZuIfT<sizeof...(NDP) == Fixed, void>
  write_loadedBlk(ZmFn<void(WrRef)> fn, NDP... ndp) {
    if (!m_lastBlk->count()) {
      write_newWriter(ZuMv(fn), ndp...);
      return;
    }
    bool newBlk = m_lastBlk->space() < 3;		// need >3 bytes' space
    if constexpr (Fixed)
      if (!newBlk && m_lastBlk->ndp() != (..., ndp))	// NDP must coincide
	newBlk = true;
    if (newBlk) {
      pushBlk();
      write_newWriter(ZuMv(fn), ndp...);
      return;
    }
    // continue writing to partially-full last block
    auto &buf = m_lastBlk->blkData->data().buf;
    Decoder decoder{buf.data(), buf.data() + buf.length()};
    while (decoder.skip()); // skip to end
    // construct encoder from decoder to continue writing block
    m_writer->encoder([&decoder, &buf]{
      return Encoder{decoder, buf.data() + BlkSize};
    });
    fn(WrRef{m_writer});
  }
  template <typename... NDP>
  ZuIfT<sizeof...(NDP) == Fixed, void>
  write_newWriter(ZmFn<void(WrRef)> fn, NDP... ndp) {
    if constexpr (Fixed) m_lastBlk->ndp(ndp...);
    m_writer->encoder([this]{ return m_lastBlk->encoder<Decoder>(this); });
    // call async to constrain stack depth
    run([this_ = ZmMkRef(this), fn = ZuMv(fn)]() mutable {
      ZuMv(fn)(WrRef{this_->m_writer});
    });
  }

  // add first block to series
  void pushFirstBlk() {
    m_lastBlkOffset = 0;
    IndexBlk *indexBlk = new IndexBlk{0};
    m_index.add(indexBlk);
    m_lastBlk = &indexBlk->blks[0];
    // m_lastBlk->offset(0); // redundant
    m_lastBlk->blkData = newBlkData(m_lastBlkOffset);
    m_lastBlk->blkData->pin();
  }
  // add subsequent block to series
  void pushBlk() {
    auto offset = m_lastBlk->offset() + m_lastBlk->count();
    ++m_lastBlkOffset;
    IndexBlk *indexBlk = m_index.find(m_lastBlkOffset);
    if (!indexBlk)
      m_index.add(indexBlk = new IndexBlk{m_lastBlkOffset & ~IndexBlkMask()});
    m_lastBlk = &indexBlk->blks[m_lastBlkOffset - indexBlk->offset];
    m_lastBlk->offset(offset);
    m_lastBlk->blkData = newBlkData(m_lastBlkOffset);
    m_lastBlk->blkData->pin();
  }
  // get Blk from index
  const Blk *getBlk(BlkOffset blkOffset) const {
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) return nullptr;
    return &indexBlk->blks[blkOffset - indexBlk->offset];
  }
  // set Blk in index
  Blk *setBlk(BlkOffset blkOffset) {
    if (ZuUnlikely(blkOffset < m_index.head())) return nullptr;
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk)
      m_index.add(indexBlk = new IndexBlk{blkOffset & ~IndexBlkMask()});
    return &indexBlk->blks[blkOffset - indexBlk->offset];
  }
  // purge index up to, but not including, blkOffset
  void purgeBlks(BlkOffset blkOffset) {
    blkOffset &= ~IndexBlkMask();
    if (blkOffset < m_index.head()) return; // prevent inadvertent reset
    if (blkOffset >= m_lastBlkOffset) return; // prevent lastBlk removal
    m_index.head(blkOffset);
  }

  // store the first value in a new series (used for subsequent searching)
  void write_firstValue(Value value) {
    auto &data = m_dbSeries->data();
    if constexpr (Fixed) {
      data.first = value.mantissa;
      data.ndp = value.ndp;
    } else {
      data.first = value;
    }
    seriesTbl()->template update<>(m_dbSeries,
      [](ZdbObject<DBSeries> *dbSeries) {
	if (dbSeries) dbSeries->commit();
      });
  }
  // main write function
  bool write_(Writer *writer, PValue value) {
    if (ZuUnlikely(!m_lastBlkOffset && !m_lastBlk->offset())) {
      if constexpr (Fixed) {
	auto ndp = writer->ndp();
	write_firstValue(ZuFixed{value, ndp});
	m_lastBlk->ndp(ndp);
      } else {
	write_firstValue(value);
      }
    }
    if (writer->encode(value)) return true;
    writer->finish(m_lastBlk);
    saveBlk();
    pushBlk();
    writer->encoder([this]() { return m_lastBlk->encoder<Decoder>(this); });
    if constexpr (Fixed) m_lastBlk->ndp(writer->ndp());
    return writer->encode(value);
  }
  // notify live readers
  void write_notify() {
    auto i = m_liveReaders.iterator();
    while (auto reader = i.iterate())
      switch (reader->nextValue()) {
	case RdrResult::Stopped:
	case RdrResult::Failed:
	  m_histReaders.pushNode(reader);
	  i.del();
      }
  }
  // called from Writer_::write
  bool write(Writer *writer, PValue value) {
    bool ok = write_(writer, value);
    if (ok) write_notify();
    return ok;
  }

  // called from Writer::stop
  void stop(Encoder &encoder) {
    encoder.finish();
    m_lastBlk->sync(encoder, encoder.last());
    saveBlk();
    encoder = {};
    m_writer = {}; // do this last to ensure +ve ref count
  }

  // save block to database (always m_lastBlk)
  void saveBlk() {
    ZeAssert(m_lastBlk, (), "internal error - null lastBlk", return);
    ZeAssert(m_lastBlk->blkData, (), "internal error - null blkData", return);

    static auto lastFn = [](const Blk *blk) {
      if constexpr (Fixed)
	return blk->last.fixed;
      else
	return blk->last.float_;
    };

    if (m_lastBlk->blkData->state() == ZdbObjState::Undefined) {
      ZdbObjRef<DBBlk> dbBlk = new ZdbObject<DBBlk>{blkTbl(), m_shard};
      new (dbBlk->ptr_()) DBBlk{
	.blkOffset = m_lastBlkOffset,
	.offset = m_lastBlk->offset(),
	.last = lastFn(m_lastBlk),
	.seriesID = id(),
	.count = m_lastBlk->count(),
	.ndp = m_lastBlk->ndp()
      };
      blkTbl()->insert(
	shard(), ZuMv(dbBlk),
	[](ZdbObject<DBBlk> *dbBlk) { if (dbBlk) dbBlk->commit(); });
      blkDataTbl()->insert(
	shard(), m_lastBlk->blkData,
	[](ZdbObject<DB::BlkData> *blkData) mutable {
	  if (blkData) {
	    blkData->commit();
	    blkData->unpin();
	  }
	});
    } else {
      blkTbl()->template findUpd<0>(
	shard(), ZuFwdTuple(id(), m_lastBlkOffset),
	[this_ = ZmMkRef(this)](ZdbObject<DBBlk> *dbBlk) {
	  if (!dbBlk || !this_->m_lastBlk) return;
	  auto &data = dbBlk->data();
	  data.offset = this_->m_lastBlk->offset();
	  data.last = lastFn(this_->m_lastBlk);
	  data.count = this_->m_lastBlk->count();
	  data.ndp = this_->m_lastBlk->ndp();
	  dbBlk->commit();
	});
      blkDataTbl()->template update<>(
	m_lastBlk->blkData, [](ZdbObject<DB::BlkData> *blkData) mutable {
	  if (blkData) {
	    blkData->commit();
	    blkData->unpin();
	  }
	});
    }
  }

  // load blk from database
  template <typename L>
  void loadBlk(BlkOffset blkOffset, L l) const {
    blkDataTbl()->template find<0>(
      shard(), ZuFwdTuple(id(), blkOffset), ZuMv(l),
      [this, blkOffset](ZdbTable<DB::BlkData> *tbl) {
	return newBlkData(blkOffset, tbl);
      });
  }

  // load block data from store (idempotent)
  template <typename L>
  void loadBlkData(BlkOffset blkOffset, L l) const {
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) { l(nullptr); return; }
    auto blk = &indexBlk->blks[blkOffset - indexBlk->offset];
    if (blk->blkData) { l(blk); return; }
    loadBlk(blkOffset, [
      indexBlk = ZuMv(indexBlk), blk, l = ZuMv(l) // keep indexBlk in scope
    ](ZmRef<BlkData> blkData) mutable {
      if (ZuUnlikely(!blkData)) { l(nullptr); return; }
      blk->blkData = ZuMv(blkData);
      l(blk);
    });
  }

  // called from BlkData::evict() during cache eviction
  void unloadBlkData(BlkData *blkData) const {
    BlkOffset blkOffset = blkData->data().blkOffset;
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) return;
    auto blk = &indexBlk->blks[blkOffset - indexBlk->offset];
    blk->blkData = nullptr;
  }

  // Reader management
  void addHistReader(Reader *reader) const {
    m_histReaders.pushNode(static_cast<RdrListNode *>(reader));
  }
  void delHistReader(Reader *reader) const {
    m_histReaders.delNode(static_cast<RdrListNode *>(reader));
  }
  void addLiveReader(Reader *reader) const {
    m_liveReaders.pushNode(static_cast<RdrListNode *>(reader));
  }
  void delLiveReader(Reader *reader) const {
    m_liveReaders.delNode(static_cast<RdrListNode *>(reader));
  }

  // seek function used in interpolation search
  auto seekFn(BlkOffset blkOffset, Offset target) const {
    return [this, blkOffset, target](uint64_t i) -> double {
      i += blkOffset;
      auto blk = getBlk(i);
      ZeAssert(blk, (), "internal error - null blk", throw InternalError{});
      Offset offset = blk->offset();
      if (target < offset) return double(target) - double(offset);
      auto n = blk->count();
      ZeAssert(n, (), "internal error - empty blk", throw InternalError{});
      offset += (n - 1);
      if (target > offset) return double(target) - double(offset);
      return 0;
    };
  }
  // find function used in interpolation search
  auto findFn(uint64_t blkOffset, Value target) const {
    return [this, blkOffset, target](uint64_t i) -> double {
      // get last value from preceding blk
      Value value;
      i += blkOffset;
      if (i <= m_index.head()) {	// before first blk - use series first
	const auto &data = m_dbSeries->data();
	if constexpr (Fixed)
	  value = ZuFixed{data.first, data.ndp};
	else
	  value = data.first;
      } else {
	auto blk = getBlk(i - 1);
	ZeAssert(blk, (), "internal error - null blk", throw InternalError{});
	if constexpr (Fixed)
	  value = ZuFixed{blk->last.fixed, blk->ndp()};
	else
	  value = blk->last.float_;
      }
      if constexpr (Fixed) {
	double target_ = target.fp(), value_ = value.fp();
	if (target_ < value_) return target_ - value_;
      } else {
	if (target < value) return target - value;
      }
      // get last value from containing blk
      auto blk = getBlk(i);
      ZeAssert(blk, (), "internal error - null blk", throw InternalError{});
      value = ZuFixed{blk->last.fixed, blk->ndp()};
      if constexpr (Fixed) {
	double target_ = target.fp(), value_ = value.fp();
	if (target_ > value_) return target_ - value_;
      } else {
	if (target > value) return target - value;
      }
      return 0;
    };
  }

  // called from Reader::purge()
  void purge(Reader *reader, BlkOffset blkOffset) {
    blkOffset &= ~IndexBlkMask();
    if (ZuUnlikely(!blkOffset)) return;
    auto blk = getBlk(blkOffset - 1);
    if (ZuUnlikely(!blk)) return;
    if constexpr (Fixed)
      write_firstValue(ZuFixed{blk->last.fixed, blk->ndp()});
    else
      write_firstValue(blk->last.float_);
    purgeBlks(blkOffset);
  }

private:
  Store			*m_store = nullptr;
  ZdbObjRef<DBSeries>	m_dbSeries;
  Shard			m_shard;
  SeriesID		m_id;
  ZuCSpan		m_name;
  ZuTime		m_epoch;
  mutable Index		m_index;
  Blk			*m_lastBlk = nullptr;
  BlkOffset		m_lastBlkOffset = 0;
  mutable ReaderList	m_liveReaders;
  mutable ReaderList	m_histReaders;
  ZmRef<Writer>		m_writer;
  bool			m_opened = false;
};

// Reader implementation

template <typename Decoder>
inline Offset Reader<Decoder>::offset() const
{
  using namespace RdrState;

  switch (m_state) {
    case Loading:
      if (m_target.template is<Offset>())
	return m_target.template p<Offset>();
      ZeAssert(m_blk, (), "null blk", return 0);
      return m_blk->offset();
    case Stopped:
    case Stopping:
    case Reading:
    case Live:
      if (!m_decoder) return 0;
      return m_blk->offset() + m_decoder.offset();
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return 0);
  }
}

template <typename Decoder>
inline bool Reader<Decoder>::seekFwd(Offset offset)
{
  using namespace RdrState;

  if (m_state != Stopped) return false;
  m_series->seekFwd(static_cast<Reader<Decoder> *>(this), m_blkOffset, offset);
  return true;
}

template <typename Decoder>
inline bool Reader<Decoder>::seekRev(Offset offset)
{
  using namespace RdrState;

  if (m_state != Stopped) return false;
  m_series->seekRev(static_cast<Reader<Decoder> *>(this), m_blkOffset, offset);
  return true;
}

template <typename Decoder>
inline bool Reader<Decoder>::findFwd(Value value)
{
  using namespace RdrState;

  if (m_state != Stopped) return false;
  m_series->findFwd(static_cast<Reader<Decoder> *>(this), m_blkOffset, value);
  return true;
}

template <typename Decoder>
inline bool Reader<Decoder>::findRev(Value value)
{
  using namespace RdrState;

  if (m_state != Stopped) return false;
  m_series->findRev(static_cast<Reader<Decoder> *>(this), m_blkOffset, value);
  return true;
}

template <typename Decoder>
inline bool Reader<Decoder>::init(
  BlkOffset blkOffset, Blk *blk, Target target)
{
  using namespace RdrState;

  ZeAssert(blk, (), "null blk", return);

  m_failed = false;

  switch (m_state) {
    case Stopped:
      break;
    case Loading:
    case Reading:
    case Live:
    case Stopping:
      return false;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return false);
  }

  m_blkOffset = blkOffset;
  m_blk = blk;
  m_target = target;
  m_decoder = {};
  m_fn = Fn{};
  m_errorFn = ErrorFn{};
  return true;
}

template <typename Decoder>
inline bool Reader<Decoder>::read(Fn fn, ErrorFn errorFn)
{
  using namespace RdrState;

  if (m_failed) return false;

  switch (m_state) {
    case Stopped:
      break;
    case Loading:
    case Reading:
    case Live:
    case Stopping:
      return false;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return false);
  }

  ZeAssert(m_blk, (), "null blk", return false);

  m_fn = ZuMv(fn); m_errorFn = ZuMv(errorFn);

  loadBlk();
  return true;
}

template <typename Decoder>
inline void Reader<Decoder>::stop(StopFn fn)
{
  using namespace RdrState;

  auto unpin = [this] {
    ZeAssert(m_blk, (), "null blk", break);
    ZeAssert(m_blk->blkData, (), "null blkData", break);
    ZeAssert(m_decoder, (), "null decoder", (void)0);
    m_decoder = {};
    m_blk->blkData->unpin();
  };
  switch (m_state) {
    case Stopped:
    case Stopping:
      fn();
      return;
    case Loading:
      ZeAssert(m_blk, (), "null blk", break);
      break;
    case Live:
      unpin();
      m_series->delLiveReader(this);
      break;
    case Reading:
      unpin();
      m_series->delHistReader(this);
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return);
  }

  m_state = Stopping;
  m_stopFn = ZuMv(fn);
  m_fn = Fn{};
  m_errorFn = ErrorFn{};
  m_series->run([this_ = ZmMkRef(node(this))]() mutable {
    this_->stopped();
  });
}

template <typename Decoder>
inline void Reader<Decoder>::loadBlk()
{
  using namespace RdrState;

  if (ZuUnlikely(m_failed)) return;

  switch (m_state) {
    case Stopping:
      return;
    case Stopped:
    case Reading:
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, goto fail);
  }

  ZeAssert(m_blk, (), "null blk", goto fail);

  if (m_blk->blkData) {
    loaded(m_blk);
    return;
  }

  m_state = Loading;
  m_series->loadBlkData(m_blkOffset, [
    this_ = ZmMkRef(node(this))
  ](Blk *blk) mutable {
    this_->loaded(blk);
  });
}

template <typename Decoder>
inline void Reader<Decoder>::loaded(const Blk *blk)
{
  using namespace RdrState;

  if (ZuUnlikely(m_failed)) return;

  switch (m_state) {
    case Stopping:
      return;
    case Stopped:
    case Loading:
    case Reading:
    case Live:
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, goto fail);
  }

  ZeAssert(blk, (), "null blk", goto fail);
  ZeAssert(blk == m_blk, (), "inconsistent blk", goto fail);

  m_state = m_state == Live ? Live : Reading;
  m_blk->blkData->pin();
  m_decoder = m_blk->decoder<Decoder>();

  if (ZuUnlikely(!m_target.template is<void>())) {
    if (m_target.template is<Offset>()) {
      auto offset = m_blk->offset();
      auto targetOffset = m_target.template p<Offset>();
      if (targetOffset > offset)
	if (!m_decoder.seek(targetOffset - offset)) {
	  nextBlk();
	  return;
	}
    } else {
      auto value = m_target.template p<Value>();
      if (!m_decoder.search([
	value = value.adjust(m_blk->ndp())
      ](PValue skip, unsigned count) -> unsigned {
	return skip < value ? count : 0;
      })) {
	nextBlk();
	return;
      }
    }
    m_target = {};
  }

  nextValue(); // return value ignored
}

template <typename Decoder>
inline int Reader<Decoder>::nextBlk()
{
  using namespace RdrState;

  if (ZuUnlikely(m_failed)) return RdrResult::Failed;

  switch (m_state) {
    case Stopping:
      return RdrResult::Stopped;
    case Reading:
    case Live:
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return RdrResult::Failed);
  }

  ZeAssert(m_blk, (), "null blk", return RdrResult::Failed);
  ZeAssert(m_blk->blkData, (), "null blkData", return RdrResult::Failed);

  if (m_series->lastBlk(m_blk)) {
    if (m_state == Reading) {
      m_state = Live;
      m_series->delHistReader(this);
      m_series->addLiveReader(this);
    }
    return RdrResult::EOS;
  }
  m_decoder = {};
  m_blk->blkData->unpin();
  m_blk = m_series->getBlk(++m_blkOffset);
  if (ZuLikely(m_blk->blkData)) {
    m_blk->blkData->pin();
    m_decoder = m_blk->decoder<Decoder>();
    return RdrResult::OK;
  }
  m_series->run([this_ = ZmMkRef(node(this))]() mutable {
    this_->loadBlk();
  });
  return RdrResult::Load;
}

template <typename Decoder>
inline int Reader<Decoder>::nextValue()
{
  using namespace RdrState;

  if (ZuUnlikely(m_failed)) return RdrResult::Failed;

  switch (m_state) {
    case Stopping: // handled below
      break;
    case Reading:
    case Live:
      ZeAssert(m_blk, (), "null blk", goto fail);
      ZeAssert(m_blk->blkData, (), "null blkData", goto fail);
      ZeAssert(m_decoder, (), "null decoder", goto fail);
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, goto fail);
  }

  bool cont = true;

  do {
    if (m_state == Stopping) return RdrResult::Stopped;

    typename Decoder::Value value;
    if (ZuUnlikely(!m_decoder.read(value))) {
      int r = nextBlk();
      if (ZuLikely(r == RdrResult::OK)) continue;
      if (r != RdrResult::EOS) return r;
      if constexpr (ZuIsExact<Value, ZuFixed>{})
	cont = m_fn(this, ZuFixed{});
      else
	cont = m_fn(this, ZuFP<double>::nan());
      if (cont) return r;
      stop({});
      return RdrResult::Stopped;
    }

    if constexpr (ZuIsExact<Value, ZuFixed>{})
      cont = m_fn(this, ZuFixed{value, ndp()});
    else
      cont = m_fn(this, value);
  } while (cont);

  stop({});
  return RdrResult::Stopped;
}

template <typename Decoder>
inline void Reader<Decoder>::stopped()
{
  using namespace RdrState;

  ZeAssert(m_state == Stopping, (), "invalid state", return);

  m_state = Stopped;
  auto stopFn = ZuMv(m_stopFn);
  m_stopFn = StopFn{};
  stopFn();
}

template <typename Decoder>
inline void Reader<Decoder>::fail()
{
  using namespace RdrState;

  m_failed = true;

  if (m_state == Live) {
    m_series->delLiveReader(this);
    m_series->addHistReader(this);
  }

  if (m_blk && m_blk->blkData) m_blk->blkData->unpin();

  auto errorFn = ZuMv(m_errorFn);
  m_fn = Fn{};
  m_errorFn = ErrorFn{};
  errorFn();
}

template <typename Decoder>
inline void Reader<Decoder>::purge()
{
  const_cast<Series *>(m_series)->purge(this, m_blkOffset);
}

inline constexpr const char *closedWriter() {
  return "attempt to use closed Writer";
}

// Writer implementation

template <typename Decoder, typename Heap, typename Value>
inline bool Writer__<Decoder, Heap, Value>::write(Value value)
{
  ZeAssert(m_series, (), closedWriter(), return false);

  return m_series->write(static_cast<Writer<Decoder> *>(this), value);
}

template <typename Decoder, typename Heap, typename Value>
inline void Writer__<Decoder, Heap, Value>::stop()
{
  if (auto series = m_series) {
    m_series = nullptr;
    series->stop(m_encoder);
  }
}

} // namespace Zdf

#endif /* ZdfSeries_HH */
