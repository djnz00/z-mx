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

#include <zlib/ZmRef.hh>
#include <zlib/ZmPQueue.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtEnum.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfBlk.hh>

namespace Zdf {

template <typename> class Series;

namespace ReaderState {
  ZtEnumValues(ReaderState, int8_t,
    Invalid		= -1,	// returned to indicate an invalid transition
    Stopped		= 0,	// seek / find completed
    Loading		= 1,	// read called, loading block data
    Reading		= 2,	// reading historical data
    Live		= 3,	// reading, waiting for live data
    Stopping		= 4);	// stopping while reading

  constexpr const bool stopped(T v) { return v == Stopped; }
  constexpr const bool reading(T v) { return v >= Loading; }
  constexpr const bool live(T v) { return v == Live; }
}
// from an application perspective a reader is either stopped() or
// reading(); internally there are additional transient states
// (Loading, Stopping), and a further distinction is made between
// readers of live and historical data - as informed by live();
// a reader can also be failed() in any state due to error
//
// the reader m_state will never be Invalid - this is used in return
// values to indicate an incorrect attempted transition
//
// possible Reader state paths:
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
// FIXME - CHECK BELOW
// pin		loaded
// unpin	stop | nextBlk | readFail | liveFail 
//
// historical / live reader (de-)registration:
//
// addHist	seek, find, stop(Live), liveFail
// delHist	live, dtor
// addLive	live
// delLive	stop, liveFail

// the decoder determines the value type (fixed or floating point)
template <typename Decoder_>
class Reader_ : public ZmObject {
public:
  using Decoder = Decoder_;
  using Series = Zdf::Series<Decoder>;
  enum { Fixed = ZuIsExact<typename Decoder::Value, int64_t>{} };
  using Value = ZuIf<Fixed, ZuFixed, double>;
  using Fn = ZmFn<void(Value)>;
  using ErrorFn = ZmFn<void()>;
  using StopFn = ZmFn<void()>;

private:
friend Series;

  ZuAssert((!ZuIsExact<Offset, typename Decoder::Value>{})); // ensure distinct
  using Target = ZuUnion<void, Offset, typename Decoder::Value>;

  Reader_() = delete;
  Reader_(const Reader_ &) = delete;
  Reader_ &operator =(const Reader_ &) = delete;
  Reader_(Reader_ &&) = delete;
  Reader_ &operator =(Reader_ &&) = delete;

  Reader_(const Series *series, BlkOffset blkOffset, Blk *blk, Target target) :
    m_series{series}, m_blkOffset{blkOffset}, m_blk{blk}, m_target{target}
  {
    ZeAssert(m_blk, (), "null blk", return);
  }

  ~Reader_() { stop(); }

public:
  Series *series() const { return m_series; }
  bool stopped() const { return ReaderState::stopped(m_state); }
  bool reading() const { return ReaderState::reading(m_state); }
  bool live() const { return ReaderState::live(m_state); }
  bool failed() const { return m_failed; }

  NDP ndp() const;
  Offset offset() const;

  // to use seekFwd/seekRev/findFwd/findRev, the Reader must be stopped;
  // these functions return false if the Reader is actively reading

  // seek forward to offset
  bool seekFwd(uint64_t offset);
  // seek reverse to offset
  bool seekRev(uint64_t offset);

  // series must monotonically increase to use find*() (e.g. time series)

  // seek forward to >= value
  bool findFwd(Value value);
  // seek backwards to >= value
  bool findRev(Value value);

  // read values (returns false if already reading)
  bool read(Fn, ErrorFn = {});

  // stop reading (idempotent - can always be called)
  void stop(StopFn);

  // purge historical data up to current read position
  void purge();

private:
  bool init(BlkOffset, Blk *, Target);

  void loadBlk();
  void loaded(Blk *blk);
  bool nextBlk();
  bool nextValue();	// called for live Readers

  void stopped();

  void fail();

private:
  const Series		*m_series = nullptr;
  BlkOffset		m_blkOffset = 0;
  Blk			*m_blk = nullptr;
  Target		m_target;
  Decoder		m_decoder;
  Fn			m_fn;
  ErrorFn		m_errorFn;
  StopFn		m_stopFn;
  ReaderState::T	m_state = ReaderState::Stopped;
  bool			m_failed = false;
};

inline constexpr const char *Reader_HeapID() { return "Zdf.Reader"; }

template <typename Decoder>
using ReaderList =
  ZmList<Reader_<Decoder>,
    ZmListNode<Reader_<Decoder>,
      ZmListHeapID<Reader_HeapID>>>;

template <typename Decoder>
struct Reader : public ReaderList<Decoder>::Node;

// RdrHandle is a move-only ZmRef<Reader>-derived smart pointer,
// with a RAII dtor that calls reader->stop()
template <typename Decoder>
class RdrHandle : public ZmRef<Reader<Decoder>> {
  RdrHandle(const RdrHandle &) = delete;
  RdrHandle &operator =(const RdrHandle &) = delete;

public:
  using Reader = Zdf::Reader<Decoder>;
  using Ref = ZmRef<Reader>;

  using Ref::operator *;
  using Ref::operator ->;

  RdrHandle() = default;
  RdrHandle(Ref &&h) : Ref{ZuMv(h)} { }
  RdrHandle &operator =(Ref &&h) {
    stop();
    Ref::operator =(ZuMv(h));
    return *this;
  }
  RdrHandle(Reader *r) : Ref{r} { }
  RdrHandle &operator =(Reader *r) {
    stop();
    Ref::operator =(r);
    return *this;
  }
  ~RdrHandle() { stop(); }

  stop() {
    if (auto ptr = this->ptr_()) ptr->stop();
  }
};

// each IndexBlk contains Blk[512]
inline constexpr const unsigned IndexBlkShift() { return 9; }
inline constexpr const unsigned IndexBlkSize() { return 1<<IndexBlkShift(); }
inline constexpr const unsigned IndexBlkMask() {
  return ~((~0U)<<IndexBlkShift());
}

// the series index is a skiplist (ZmPQueue) of IndexBlks
struct IndexBlk_ : public ZuObject {
  uint64_t	offset;			// block offset
  Blk		blks[IndexBlkSize()];
};
struct IndexBlk_Fn {
  IndexBlk_	&indexBlk;

  using Key = uint64_t;
  ZuInline uint64_t key() const { return indexBlk.offset; }
  ZuInline unsigned length() const { return IndexBlkSize(); }
};
inline constexpr const char *IndexBlk_HeapID() { return "Zdf.IndexBlk"; }
using Index =
  ZmPQueue<IndexBlk_,
    ZmPQueueFn<IndexBlk_Fn,
      ZmPQueueNode<IndexBlk_,
	ZmPQueueStats<false,
	  ZmPQueueOverlap<false,
	    ZmPQueueBits<3,
	      ZmPQueueLevels<3,
		ZmPQueueHeapID<IndexBlk_HeapID>>>>>>>>;
using IndexBlk = Index::Node;

template <typename Decoder, typename Heap>
class Writer_ : public Heap, public ZmObject {
public:
  using Encoder = Zdf::Encoder<Decoder>;
  using Series = Zdf::Series<Decoder>;
  using Value = ValueMap<typename Decoder::Value>;

friend Series;

  Writer_(Series *series, Encoder encoder) :
    m_series{series}, m_encoder{ZuMv(encoder)} { }
  ~Writer_() { close(); }

  // append value to series, notifying any live readers
  bool write(Value);

  // close writer (idempotent)
  void close();

private:
  Encoder &encoder() { return m_encoder; }

private:
  Series	*m_series = nullptr;
  Encoder	m_encoder;
};
inline constexpr const char *Writer_HeapID() { return "Zdf.Writer"; }
template <typename Decoder>
using Writer_Heap = ZmHeap<Writer_HeapID, sizeof(Writer_<Decoder, ZuNull>)>;
template <typename Decoder>
using Writer = Writer_<Decoder, Writer_Heap<Decoder>>;

class DB;
class DataFrame;

struct InternalError { };	// internal exception

// a series is SWMR, the writer is built-in
template <typename Decoder_>
class Series {
  using Decoder = Decoder_;
  using Encoder = Encoder<Decoder>;
  using Reader = Zdf::Reader<Decoder>;
  using ReaderList = Zdf::ReaderList<Decoder>;
  enum { Fixed = ZuIsExact<typename Decoder::Value, int64_t>{} };
  using Value = ZuIf<Fixed, ZuFixed, double>;
  using DBType = ZuIf<Fixed, DB::SeriesFixed, DB::SeriesFloat>;
  using DBBlkHdr = ZuIf<Fixed, DB::BlkHdrFixed, DB::BlkHdrFloat>;

friend DB;
friend DataFrame;
friend Reader;
friend Writer;

using ID = uint32_t;

private:
  Series(DB *db, ZdbObjRef<DBType> dbSeries) :
    m_db{db}, m_dbSeries{ZuMv(dbSeries)}  { m_dbSeries->pin(); }
  ~Series() { m_dbSeries->unpin(); }

public:
  DB *db() const { return m_db; }
  ZdbObject<DBType> *dbSeries() const { return m_dbSeries; }
  unsigned shard() const { return m_dbSeries->shard(); }
  DataFrame *df() const { return m_df; }
  ID id() const { return data().id; }
  bool opened() const { return m_opened; }

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
  RdrHandle seek(Offset offset) const {
    uint64_t result;
    try {
      result =
	ZuInterSearch(m_index.blkCount(), seekFn(m_index.head(), offset));
    } catch (InternalError) {
      return nullptr;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    return new Reader{this, blkOffset, getBlk(blkOffset), offset};
  }
private:
  void seekFwd(Reader_ *reader, BlkOffset blkOffset, Offset offset) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(m_lastBlkOffset + 1) - blkOffset, seekFn(blkOffset, offset));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), offset);
  }
  void seekRev(Reader_ *reader, BlkOffset blkOffset, Offset offset) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(blkOffset + 1) - m_index.head(), seekFn(m_index.head(), offset));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), offset);
  }

  // Reader find functions
public:
  RdrHandle find(Value value) const {
    uint64_t result;
    try {
      result =
	ZuInterSearch(m_index.blkCount(), findFn(m_index.head(), value));
    } catch (InternalError) {
      return nullptr;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    return new Reader{this, blkOffset, getBlk(blkOffset), value};
  }
private:
  void findFwd(Reader_ *reader, BlkOffset blkOffset, Value value) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(m_lastBlkOffset + 1) - blkOffset, findFn(blkOffset, value));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), value);
  }
  void findRev(Reader_ *reader, BlkOffset blkOffset, Value value) const {
    uint64_t result;
    try {
      result = ZuInterSearch(
	(blkOffset + 1) - m_index.head(), findFn(m_index.head(), value));
    } catch (InternalError) {
      reader->failed();
      return;
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    reader->init(blkOffset, getBlk(blkOffset), value);
  }

private:
  void init(DataFrame *df) {
    m_df = df;
  }
  void final() {
    m_dbSeries->unpin();
    m_index.reset(0);
  }

  // open series and query blkHdr table to fill index
  void open(OpenSeriesFn fn) {
    if (m_fixed)
      openFixed(ZuMv(fn));
    else
      openFloat(ZuMv(fn));
  }
  // return Series DB table
  auto seriesTbl() {
    if constexpr (Fixed)
      return m_db->seriesFixedTbl();
    else
      return m_db->seriesFloatTbl();
  }
  // return BlkHdr DB table
  auto blkHdrTbl() {
    if constexpr (Fixed)
      return m_db->blkHdrFixedTbl();
    else
      return m_db->blkHdrFloatTbl();
  }
  // open() - open series
  void open(OpenSeriesFn fn) {
    m_index.head(m_dbSeries->data().blkOffset);
    blkHdrTbl()->selectRows<0>(
      ZuFwdTuple(data().id), IndexBlkSize(), ZuLambda{[
	this_ = ZmMkRef(this), fn = ZuMv(fn), rowRcvd = false
      ](auto &&self, auto result, unsigned) mutable {
	if (this_->m_opened) return; // index already filled
	using Row = ZuFieldTuple<DBBlkHdr>;
	if (result.template is<Row>()) { // fill index
	  if (!this_->open_loadBlkHdr(result.template p<Row>())) goto opened;
	  rowRcvd = true;
	} else { // complete
	  if (!rowRcvd) goto opened;
	  rowRcvd = false;
	  this_->blkHdrTbl()->nextRows<0>(
	    ZuFwdTuple(data().id, this_->m_lastBlkOffset), false,
	    IndexBlkSize(), ZuMv(self));
	}
	return;
      opened:
	this_->m_opened = true;
	fn(ZuMv(this_));
      }});
  }
  // load an individual block header into the index during open
  template <typename Row>
  bool open_loadBlkHdr(const Row &row) {
    BlkOffset blkOffset = row.p<1>();
    auto blk = setBlk(blkOffset);
    if (!blk) return false;
    blk->init(row.p<2>(), row.p<4>(), row.p<5>(), row.p<3>());
    m_lastBlk = blk;
    m_lastBlkOffset = blkOffset;
    return true;
  }

  void write(ZmFn<void(ZmRef<Writer>)> fn, unsigned ndp) {
    if (m_writer) return nullptr;
    if (!m_lastBlk) { // new series - append first block
      pushFirstBlk();
      write_newWriter(ZuMv(fn), ndp);
    } else if (m_lastBlk->count()) { // last block is not empty - load the data
      if (!m_lastBlk->blkData) {
	ZmRef<IndexBlk> indexBlk = m_index.find(m_lastBlkOffset);
	db()->loadBlk(this, m_lastBlkOffset, [
	  this, fn = ZuMv(fn), ndp, indexBlk = ZuMv(indexBlk),
	](ZmRef<BlkData> blkData) mutable {
	  blkData->pin();
	  m_lastBlk->blkData = ZuMv(blkData);
	  write_loadedBlk(ZuMv(fn), ndp);
	});
      }
    } else { // last block is empty - allocate the data
      if (!m_lastBlk->blkData) m_lastBlk->blkData = new BlkData{this};
      m_lastBlk->blkData->pin();
      write_newWriter(ZuMv(fn), ndp);
    }
  }

  void write_loadedBlk(ZmFn<void(ZmRef<Writer>)> fn, unsigned ndp) {
    if (!m_lastBlk->count()) {
      write_newWriter(ZuMv(fn), ndp);
      return;
    }
    if (m_lastBlk->ndp() != ndp ||	// NDP must coincide
	m_lastBlk->space() < 3) {	// need at least 3 bytes' space left
      pushBlk();
      write_newWriter(ZuMv(fn), ndp);
      return;
    }
    // continue writing to partially-full lask block
    const auto &buf = m_lastBlk->blkData->buf;
    Decoder decoder{buf.data(), buf.data() + buf.length()};
    while (decoder.skip()); // skip to end
    // construct encoder from decoder to continue writing block
    ZmRef<Writer> writer =
      new Writer{this, Encoder{decoder, buf.data() + BlkSize}};
    fn(ZuMv(writer));
  }
  void write_newWriter(ZmFn<void(ZmRef<Writer>)> fn, unsigned ndp) {
    m_lastBlk->ndp(ndp);
    ZmRef<Writer> writer = new Writer{this, m_lastBlk->encoder<Encoder>()};
    fn(ZuMv(writer));
  }

  // add first block to series
  void pushFirstBlk() {
    m_lastBlkOffset = 0;
    IndexBlk *indexBlk = new IndexBlk{0};
    m_index.add(indexBlk);
    m_lastBlk = &indexBlk->data[0];
    // m_lastBlk->offset(0); // redundant
    m_lastBlk->blkData = new BlkData{this};
    m_lastBlk->blkData->pin();
  }
  // add subsequent block to series
  void pushBlk() {
    auto offset = m_lastBlk->offset() + m_lastBlk->count();
    ++m_lastBlkOffset;
    IndexBlk *indexBlk = m_index.find(m_lastBlkOffset);
    if (!indexBlk)
      m_index.add(indexBlk = new IndexBlk{m_lastBlkOffset & ~IndexBlkMask()});
    m_lastBlk = &indexBlk->data[m_lastBlkOffset - indexBlk->offset];
    m_lastBlk->offset(offset);
    m_lastBlk->blkData = new BlkData{this};
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
    return &indexBlk->data[blkOffset - indexBlk->offset];
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
    m_dbSeries->data().first = value;
    seriesTbl()->update<>(m_dbSeries,
      [](ZdbObject<DB::Series> *dbSeries) {
	if (dbSeries) dbSeries->commit();
      });
  }
  // main write function
  bool write_(Writer *writer, Value value) {
    if (ZuUnlikely(!m_lastBlkOffset && !m_lastBlk->offset()))
      write_firstValue(value);
    auto &encoder = writer->encoder();
    if constexpr (Fixed) {
      if (value.ndp == m_ndp && encoder.write(value.mantissa)) return true;
    } else {
      if (encoder.write(value)) return true;
    }
    encoder.finish();
    m_lastBlk->sync(encoder, m_ndp, encoder.last());
    m_db->saveBlk(this, m_lastBlk,
      [](ZdbObject<DB::BlkData> *blkData) { blkData->unpin(); });
    pushBlk();
    encoder = m_lastBlk->encoder<Encoder>();
    if constexpr (Fixed) {
      m_ndp = value.ndp;
      return encoder.write(value.mantissa);
    } else {
      return encoder.write(value);
    }
  }
  // notify live readers
  template <typename Value>
  void write_notify() {
    auto i = m_liveReaders.iterator();
    while (auto reader = i.iterate())
      if (!reader->nextValue()) {
	m_histReaders.pushNode(reader);
	i.del();
      }
  }
  // called from Writer::write
  template <typename Value>
  bool write(Writer *writer, Value value) {
    bool ok = write_(writer, value);
    if (ok) write_notify<Value>(value);
    return result;
  }

  // called from Writer::close
  void close(Writer *writer) {
    auto &encoder = writer->encoder();
    encoder.finish();
    m_lastBlk->sync(encoder, 0, encoder.last());
    ZmAssert(m_lastBlk->blkData->pinned());
    m_db->saveBlk(this, m_lastBlk,
      [](ZdbObject<DB::BlkData> *blkData) { blkData->unpin(); });
  }

  // load block data from DB (idempotent)
  template <typename L>
  void loadBlkData(BlkOffset blkOffset, L l) const {
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) { l(nullptr); return; }
    auto blk = &indexBlk->blks[blkOffset - indexBlk->offset];
    if (blk->blkData) { l(blk); return; }
    db()->loadBlk(this, blkOffset, [
      indexBlk = ZuMv(indexBlk), blk, l = ZuMv(l) // keep indexBlk in scope
    ](ZmRef<BlkData> blkData) mutable {
      blk->blkData = ZuMv(blkData);
      l(blk);
    });
  }

  // called from BlkData::evict() during cache eviction
  void unloadBlkData(BlkData *blkData) {
    BlkOffset blkOffset = blkData->data().blkOffset;
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) return nullptr;
    auto blk = &indexBlk->blks[blkOffset - indexBlk->offset];
    blk->blkData = nullptr;
  }

  // number of blocks in index
  unsigned blkCount() const { return (m_lastBlkOffset + 1) - m_index.head(); }

  // first blkOffset (will be non-zero following a purge())
  BlkOffset head() const { return m_index.head(); }

  // Reader management
  void addHistReader(Reader *reader) { m_histReaders.push(reader); }
  void delHistReader(Reader *reader) { m_histReaders.delNode(reader); }
  void addLiveReader(Reader *reader) { m_liveReaders.push(reader); }
  void delLiveReader(Reader *reader) { m_liveReaders.delNode(reader); }

  // seek function used in interpolation search
  auto seekFn(uint64_t blkOffset, uint64_t offset) const {
    return [this, blkOffset, offset](uint64_t i) -> double {
      i += blkOffset;
      auto blk = getBlk(i);
      ZeAssert(blk, (), "internal error - null block", throw InternalError{});
      auto offset_ = blk->offset();
      if (offset < offset_) return double(offset - offset_);
      auto n = blk->count();
      ZeAssert(n, (), "internal error - empty block", throw InternalError{});
      offset_ += (n - 1);
      if (offset > offset_) return double(offset - offset_);
      return 0;
    };
  }
  // FIXME - fix for both ZuFixed and double
  // find function used in interpolation search
  auto findFn(uint64_t blkOffset, ZuFixed value) const {
    return [this, blkOffset, value](uint64_t i) -> double {
      // get last value from preceding blk
      ZuFixed value_;
      i += blkOffset;
      if (i <= m_index.head()) {
	value_ = first; // series first
	value_.mantissa = value_.adjust(value.ndp);
      } else {
	auto blk = getBlk(i - 1);
	ZeAssert(blk, (), "internal error - null block", throw InternalError{});
	value_ = ZuFixed{blk->last.fixed, blk->ndp()};
	value_.mantissa = value_.adjust(value.ndp) + 1;
      }
      if (value.mantissa < value_.mantissa)
	return (value - value_).fp();
      // get last value from containing blk
      auto blk = getBlk(i);
      ZeAssert(blk, (), "internal error - null block", throw InternalError{});
      value_ = ZuFixed{blk->last(), blk->ndp()};
      value_.mantissa = value_.adjust(value.ndp());
      if (value.mantissa > value_.mantissa)
	return (value - value_).fp();
      return 0;
    };
  }

  void purge(Reader *reader) {
    auto blkOffset = reader->blkOffset()
    blkOffset &= ~IndexBlkMask();
    if (ZuUnlikely(!blkOffset)) return;
    auto blk = getBlk(blkOffset - 1);
    if (ZuUnlikely(!blk)) return;
    if (m_fixed)
      write_firstFixed(blk->last.fixed);
    else
      write_firstFloat(blk->last.float_);
    purgeBlks(blkOffset);
  }

  bool lastBlk(Blk *blk) const { return blk == m_lastBlk; }

private:
  DB			*m_db = nullptr;
  ZdbObjRef<DBType>	m_dbSeries;
  DataFrame		*m_df = nullptr;
  Index			m_index;
  Blk			*m_lastBlk = nullptr;
  uint64_t		m_lastBlkOffset = 0;
  ReaderList		m_liveReaders;
  ReaderList		m_histReaders;
  ZmRef<Writer>		m_writer;
  bool			m_opened = false;
  bool			m_fixed = true;		// fixed point
};

inline void BlkData::evict()
{
  m_series->unloadBlkData(this);
  ZdbAnyObject::evict();
}

// Reader implementation

template <typename Decoder>
inline NDP Reader_<Decoder>::ndp() const
{
  return m_blk ? m_blk->ndp() : 0;
}
template <typename Decoder>
inline Offset Reader_<Decoder>::offset() const
{
  using namespace ReaderState:

  switch (m_state) {
    case Loading:
      if (m_target.is<Offset>()) return m_target.p<Offset>();
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
inline void Reader_<Decoder>::seekFwd(uint64_t offset)
{
  // FIXME - state management
  m_series->seekFwd(this, m_blkOffset, offset);
}

template <typename Decoder>
inline void Reader_<Decoder>::seekRev(uint64_t offset)
{
  // FIXME
  m_series->seekRev(this, m_blkOffset, offset);
}

template <typename Decoder>
inline void Reader_<Decoder>::findFwd(const ZuFixed &value)
{
  // FIXME
  m_series->findFwd(this, m_blkOffset, value);
}

template <typename Decoder>
inline void Reader_<Decoder>::findRev(const ZuFixed &value)
{
  // FIXME
  m_series->findRev(this, m_blkOffset, value);
}

template <typename Decoder>
inline bool Reader_<Decoder>::init(
  BlkOffset blkOffset, Blk *blk, Target target)
{
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
  m_fn = {};
  m_errorFn = {};
  return true;
}

template <typename Decoder>
inline bool Reader_<Decoder>::read(Fn fn, ErrorFn errorFn = {})
{
  using namespace ReaderState;

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
inline void Reader_<Decoder>::stop(StopFn fn)
{
  using namespace ReaderState;

  switch (m_state) {
    case Stopped:
    case Stopping:
      fn();
      return;
    case Loading:
      ZeAssert(m_blk, (), "null blk", break);
      break;
    case Reading:
    case Live:
      ZeAssert(m_blk, (), "null blk", break);
      ZeAssert(m_blk->blkData, (), "null blkData", break);
      ZeAssert(m_decoder, (), "null decoder", (void)0);
      m_decoder = {};
      m_blk->blkData->unpin();
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return);
  }

  m_state = Stopping;
  m_stopFn = ZuMv(fn);
  m_series->run([this_ = ZmMkRef(this)]() mutable {
    this_->stopped();
  });
}

template <typename Decoder>
inline void Reader_<Decoder>::loadBlk()
{
  using namespace ReaderState;

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
    this_ = ZmMkRef(this),
  ](Blk *blk) mutable {
    this_->loaded(blk);
  });
  return;

fail:
  this->fail();
}

template <typename Decoder>
inline void Reader_<Decoder>::loaded(Blk *blk)
{
  using namespace ReaderState;

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

  if (!m_target.is<void>()) {
    if (m_target.is<Offset>()) {
      auto offset = m_blk->offset();
      auto targetOffset = target.p<Offset>();
      if (targetOffset > offset)
	if (!decoder.seek(targetOffset - offset)) {
	  nextBlk();
	  return;
	}
    } else {
      auto value = target.p<typename Decoder::Value>();
      if (!decoder.search([
	value
      ](Value skip, unsigned count) -> unsigned {
	return skip < value ? count : 0;
      })) {
	nextBlk();
	return;
      }
    }
    m_target = {};
  }

  nextValue();
  return;

fail:
  this->fail();
}

template <typename Decoder>
inline bool Reader_<Decoder>::nextBlk()
{
  using namespace ReaderState;

  if (ZuUnlikely(m_failed)) return;

  switch (m_state) {
    case Stopping:
      return;
    case Reading:
    case Live:
      break;
    default:
      ZeAssert(false,
	(state = int(m_state)), "invalid state " << state, return);
  }

  ZeAssert(m_blk, (), "null blk", return);
  ZeAssert(m_blk->blkData, (), "null blkData", return);

  if (m_series->lastBlk(m_blk)) {
    if (m_state == Reading) {
      m_state == Live;
      m_series->live(this);
    }
    return false;
  }
  m_decoder = {};
  m_blk->blkData->unpin();
  m_blk = m_series->getBlk(++m_blkOffset);
  if (ZuLikely(m_blk->blkData)) {
    m_blk->blkData->pin();
    m_decoder = m_blk->decoder<Decoder>();
    return true;
  }
  m_series->run([this_ = ZmMkRef(this)]() mutable {
    this_->loadBlk();
  });
  return false;
}

template <typename Decoder>
inline bool Reader_<Decoder>::nextValue()
{
  using namespace ReaderState;

  if (ZuUnlikely(m_failed)) return false;

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
    if (m_state == Stopping) return false;

    Decoder::Value value;
    if (ZuUnlikely(!m_decoder.read(value))) {
      if (!nextBlk()) return true;
      continue;
    }

    if constexpr (ZuIsExact<Value, ZuFixed>{})
      cont = m_fn(ZuFixed{value, ndp()});
    else
      cont = m_fn(value);
  } while (cont);

stop:
  m_state = Stopping;
  m_fn = {};
  m_errorFn = {};
  m_series->run([this_ = ZmMkRef(this)]() mutable {
    this_->stopped();
  });
  return false;

fail:
  this->fail();
  goto stop;
}

template <typename Decoder>
inline void Reader_<Decoder>::stopped()
{
  using namespace ReaderState;

  ZeAssert(m_state == Stopping, (), "invalid state", return);

  m_state = Stopped;
  auto stopFn = ZuMv(m_stopFn);
  m_stopFn = {};
  stopFn();
}

template <typename Decoder>
inline void Reader_<Decoder>::fail()
{
  m_failed = true;

  if (m_blk && m_blk->blkData) m_blk->blkData->unpin();

  auto errorFn = ZuMv(m_errorFn);
  m_fn = {};
  m_errorFn = {};
  errorFn();
}

template <typename Decoder>
inline void Reader_<Decoder>::purge()
{
  m_series->purge(m_blkOffset);
}

inline constexpr const char *closedWriter() {
  return "attempt to use closed Writer";
}

// Writer implementation

template <typename Decoder>
inline bool Writer<Decoder>::write(ZuFixed value)
{
  ZeAssert(m_series, (), closedWriter(), return false);

  return m_series->write<ZuFixed>(this, value);
}

template <typename Decoder>
inline bool Writer<Decoder>::write(double value)
{
  ZeAssert(m_series, (), closedWriter(), return false);

  return m_series->write<double>(this, value);
}

template <typename Decoder>
inline void Writer<Decoder>::close()
{
  if (m_series) {
    m_series->close(this);
    m_series = nullptr;
  }
}

} // namespace Zdf

#endif /* ZdfSeries_HH */
