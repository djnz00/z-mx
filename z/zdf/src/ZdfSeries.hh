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

class AnySeries;
template <typename> class Series;

namespace ReaderState {
  ZtEnumValues(ReaderState, int8_t,
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
// addHistReader	seek, find, stop(Live), liveFail
// delHistReader	live, dtor
// addLiveReader	live
// delLiveReader	stop, liveFail

class AnyReader_ : public ZmObject {
public:
  using ErrorFn = ZmFn<void()>;
  using StopFn = ZmFn<void()>;

private:
friend AnySeries;

  AnyReader_() = delete;
  AnyReader_(const AnyReader_ &) = delete;
  AnyReader_ &operator =(const AnyReader_ &) = delete;
  AnyReader_(AnyReader_ &&) = delete;
  AnyReader_ &operator =(AnyReader_ &&) = delete;

  AnyReader_(const AnySeries *series, BlkOffset blkOffset, Blk *blk)
    m_series{series}, m_blkOffset{blkOffset}, m_blk{blk}
  {
    ZeAssert(m_blk, (), "null blk", return);
  }

  ~AnyReader_() { }

public:
  const AnySeries *series() const { return m_series; }
  bool stopped() const { return ReaderState::stopped(m_state); }
  bool reading() const { return ReaderState::reading(m_state); }
  bool live() const { return ReaderState::live(m_state); }
  bool failed() const { return m_failed; }

  NDP ndp() const {
    return m_blk ? m_blk->ndp() : 0;
  }
  virtual Offset offset() const = 0;

  // to use seekFwd/seekRev/findFwd/findRev, the Reader must be stopped;
  // these functions return false if the Reader is actively reading

  // seek forward to offset
  virtual bool seekFwd(Offset offset) = 0;
  // seek reverse to offset
  virtual bool seekRev(Offset offset) = 0;

  // series must monotonically increase to use find*() (e.g. time series)

  // seek forward to >= value
  virtual bool findFwd(ZuFixed) { return false; }
  virtual bool findFwd(double) { return false; }
  // seek backwards to >= value
  virtual bool findRev(ZuFixed) { return false; }
  virtual bool findRev(double) { return false; }

  // read values (returns false if already reading)
  virtual bool read(ZmFn<void(ZuFixed)>, ErrorFn) { return false; }
  virtual bool read(ZmFn<void(double)>, ErrorFn) { return false; }

  // stop reading (idempotent - can always be called)
  virtual void stop(StopFn) = 0;

  // purge historical data up to current read position
  void purge();

protected:
  const AnySeries	*m_series = nullptr;
  BlkOffset		m_blkOffset = 0;
  Blk			*m_blk = nullptr;
  ReaderState::T	m_state = ReaderState::Stopped;
  bool			m_failed = false;
};

template <typename Decoder>
using ReaderList =
  ZmList<AnyReader_,
    ZmListNode<AnyReader_,
      ZmListShadow<true>>>;

struct AnyReader : public ReaderList<AnyReader_>::Node;

// RdHandle is a move-only ZmRef<AnyReader>-derived smart pointer,
// with a RAII dtor that calls reader->stop()
class RdHandle : public ZmRef<AnyReader> {
  RdHandle(const RdHandle &) = delete;
  RdHandle &operator =(const RdHandle &) = delete;

public:
  using Ref = ZmRef<AnyReader>;

  using Ref::operator *;
  using Ref::operator ->;

  RdHandle() = default;
  RdHandle(RdHandle &&h) : Ref{ZuMv(h)} { }
  RdHandle &operator =(RdHandle &&h) {
    stop_();
    Ref::operator =(ZuMv(h));
    return *this;
  }
  template <typename Arg>
  RdHandle(Arg &&arg) : Ref{ZuFwd<Arg>(arg)} { }
  template <typename Arg>
  RdHandle &operator =(Arg &&arg) {
    stop_();
    Ref::operator =(ZuFwd<Arg>(arg));
    return *this;
  }
  ~RdHandle() { stop_(); }

private:
  void stop_() {
    if (!!*this)
      ptr->stop(StopFn{ZuMv(*static_cast<Ref *>(this)), [](AnyReader *reader) {
	reader->m_series->delHistReader(reader);
      }});
  }
};

// the decoder determines the value type (fixed or floating point)
template <typename Decoder_, typename Heap>
class Reader_ : public Heap, public AnyReader {
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
    AnyReader{series, blkOffset, blk}, m_target{target} { }

  ~Reader_() { }

public:
  const Series *series() const {
    return static_cast<const Series *>(AnyReader::series());
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
  void loaded(Blk *blk);
  bool nextBlk();
  bool nextValue();

  void stopped();

  void fail();

private:
  Target		m_target;
  Decoder		m_decoder;
  Fn			m_fn;
  ErrorFn		m_errorFn;
  StopFn		m_stopFn;
};
inline constexpr const char *Reader_HeapID() { return "Zdf.Reader"; }
template <typename Decoder>
using Reader_Heap = ZmHeap<Reader_HeapID, sizeof(Reader_<Decoder, ZuNull>)>;
template <typename Decoder>
using Reader = Reader_<Decoder, Reader_Heap<Decoder>>;

class AnyWriter {
friend AnySeries;

  AnyWriter(AnySeries *series) : m_series{series} { }

  AnyWriter() = delete;
  AnyWriter(const AnyWriter &) = delete;
  AnyWriter &operator =(const AnyWriter &) = delete;
  AnyWriter(AnyWriter &&) = delete;
  AnyWriter &operator =(AnyWriter &&) = delete;

public:
  ~AnyWriter() { }

  AnySeries *series() const { return m_series; }

  // append value to series, notifying any live readers
  virtual bool write(ZuFixed) { return false; }
  virtual bool write(double) { return false; }

  // stop writing (idempotent)
  virtual void stop() = 0;

private:
  Series	*m_series = nullptr;
};

// WrHandle is a move-only ZmRef<AnyWriter>-derived smart pointer,
// with a RAII dtor that calls writer->stop()
class WrHandle : public ZmRef<AnyWriter> {
  WrHandle(const WrHandle &) = delete;
  WrHandle &operator =(const WrHandle &) = delete;

public:
  using Ref = ZmRef<AnyWriter>;

  using Ref::operator *;
  using Ref::operator ->;

  WrHandle() = default;
  WrHandle(WrHandle &&h) : Ref{ZuMv(h)} { }
  WrHandle &operator =(WrHandle &&h) {
    stop_();
    Ref::operator =(ZuMv(h));
    return *this;
  }
  template <typename Arg>
  WrHandle(Arg &&arg) : Ref{ZuFwd<Arg>(arg)} { }
  template <typename Arg>
  WrHandle &operator =(Arg &&arg) {
    stop_();
    Ref::operator =(ZuFwd<Arg>(arg));
    return *this;
  }
  ~WrHandle() { stop_(); }

private:
  stop_() {
    if (auto ptr = this->ptr_()) ptr->stop();
  }
};

template <typename Decoder, typename Heap>
class Writer_ : public Heap, public AnyWriter {
  using Encoder = Zdf::Encoder<Decoder>;
  using Series = Zdf::Series<Decoder>;
  using Value = ValueMap<typename Decoder::Value>;

friend Series;

  Writer_(Series *series) : AnyWriter{series} { }

public:
  ~Writer_() { }

  Series *series() const {
    return static_cast<Series *>(AnyWriter::series());
  }

  // append value to series, notifying any live readers
  bool write(Value);

  // stop writing (idempotent)
  void stop();

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
};
struct IndexBlk_Fn {
  IndexBlk_	&indexBlk;

  using Key = Offset;
  ZuInline Offset key() const { return indexBlk.offset; }
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

using OpenSeriesFn = ZmFn<void(ZmRef<AnySeries>)>;

// a series is SWMR
class AnySeries {
friend DB;
friend AnyReader;
friend AnyWriter;

protected:
  AnySeries(DB *db) : m_db{db} { }

private:
  void init(DataFrame *df) { m_df = df; }

public:
  ~AnySeries() { }

  DB *db() const { return m_db; }
  DataFrame *df() const { return m_df; }
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
  virtual RdHandle seek(Offset) const = 0;

  // Reader find functions
  virtual RdHandle find(ZuFixed) const { return {}; }
  virtual RdHandle find(double) const { return {}; }

protected:
  // number of blocks in index
  unsigned blkCount() const { return (m_lastBlkOffset + 1) - m_index.head(); }

  // first blkOffset (will be non-zero following a purge())
  BlkOffset head() const { return m_index.head(); }

  // Reader management
  void addHistReader(AnyReader *reader) { m_histReaders.push(reader); }
  void delHistReader(AnyReader *reader) { m_histReaders.delNode(reader); }
  void addLiveReader(AnyReader *reader) { m_liveReaders.push(reader); }
  void delLiveReader(AnyReader *reader) { m_liveReaders.delNode(reader); }

  bool lastBlk(Blk *blk) const { return blk == m_lastBlk; }

protected:
  DB			*m_db = nullptr;
  DataFrame		*m_df = nullptr;
  Index			m_index;
  Blk			*m_lastBlk = nullptr;
  BlkOffset		m_lastBlkOffset = 0;
  ReaderList		m_liveReaders;
  ReaderList		m_histReaders;
  ZmRef<AnyWriter>	m_writer;
  bool			m_opened = false;
};

struct InternalError { };	// internal exception

template <typename Decoder_>
class Series : public AnySeries {
  using Decoder = Decoder_;
  using Encoder = Encoder<Decoder>;
  using Reader = Zdf::Reader<Decoder>;
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
    AnySeries{db}, m_dbSeries{ZuMv(dbSeries)}  { m_dbSeries->pin(); }
  ~Series() { m_dbSeries->unpin(); }

public:
  ZdbObject<DBType> *dbSeries() const { return m_dbSeries; }
  unsigned shard() const { return m_dbSeries->shard(); }
  ID id() const { return m_dbSeries->data().id; }

  // Reader seek functions
public:
  RdHandle seek(Offset offset) const {
    uint64_t result;
    try {
      result =
	ZuInterSearch(m_index.blkCount(), seekFn(m_index.head(), offset));
    } catch (InternalError) {
      return {};
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    RdHandle handle = new Reader{this, blkOffset, getBlk(blkOffset), offset};
    addHistReader(handle);
    return handle;
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
  RdHandle find(Value value) const {
    uint64_t result;
    try {
      result =
	ZuInterSearch(m_index.blkCount(), findFn(m_index.head(), value));
    } catch (InternalError) {
      return {};
    }
    BlkOffset blkOffset = ZuSearchPos(result);
    RdHandle handle = new Reader{this, blkOffset, getBlk(blkOffset), value};
    addHistReader(handle);
    return handle;
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
  // return BlkHdr DB table
  auto blkHdrTbl() {
    if constexpr (Fixed)
      return m_db->blkHdrFixedTbl();
    else
      return m_db->blkHdrFloatTbl();
  }
  // open() - open series and query blkHdr table to fill index
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

  void write(ZmFn<void(WrHandle)> fn, unsigned ndp) {
    if (m_writer) { fn{WrHandle{}}; return; }
    m_writer = new Writer{this};
    if (!m_lastBlk) { // new series - append first block
      pushFirstBlk();
      write_newWriter(ZuMv(fn), ndp);
    } else if (m_lastBlk->count()) { // last block is not empty - load the data
      if (!m_lastBlk->blkData) {
	ZmRef<IndexBlk> indexBlk = m_index.find(m_lastBlkOffset);
	loadBlk(this, m_lastBlkOffset, [
	  this, fn = ZuMv(fn), ndp, indexBlk = ZuMv(indexBlk),
	](ZmRef<BlkData> blkData) mutable {
	  if (ZuUnlikely(!blkData)) { write_failed(); return; }
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

  void write_failed() {
    // FIXME - failed to load last block
  }

  void write_loadedBlk(ZmFn<void(WrHandle)> fn, unsigned ndp) {
    if (!m_lastBlk->count()) {
      write_newWriter(ZuMv(fn), ndp);
      return;
    }
    if ((Fixed && m_lastBlk->ndp() != ndp) ||	// NDP must coincide
	m_lastBlk->space() < 3) {		// need >3 bytes' space
      pushBlk();
      write_newWriter(ZuMv(fn), ndp);
      return;
    }
    // continue writing to partially-full last block
    const auto &buf = m_lastBlk->blkData->buf;
    Decoder decoder{buf.data(), buf.data() + buf.length()};
    while (decoder.skip()); // skip to end
    // construct encoder from decoder to continue writing block
    m_writer->encoder() = Encoder{decoder, buf.data() + BlkSize};
    fn(WrHandle{m_writer});
  }
  void write_newWriter(ZmFn<void(ZmRef<Writer>)> fn, unsigned ndp) {
    m_lastBlk->ndp(ndp);
    m_writer->encoder() = m_lastBlk->encoder<Encoder>();
    fn(WrHandle{m_writer});
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
    static auto seriesTbl = [](const Series *this_) {
      if constexpr (Fixed)
	return this_->m_db->seriesFixedTbl();
      else
	return this_->m_db->seriesFloatTbl();
    };

    m_dbSeries->data().first = value;
    seriesTbl(this)->update<>(m_dbSeries,
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
    saveBlk();
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
  void write_notify() {
    auto i = m_liveReaders.iterator();
    while (auto reader = i.iterate())
      if (!reader->nextValue()) {
	m_histReaders.pushNode(reader);
	i.del();
      }
  }
  // called from Writer_::write
  bool write(Writer *writer, Value value) {
    bool ok = write_(writer, value);
    if (ok) write_notify<Value>(value);
    return result;
  }

  // called from Writer::stop
  void stop(Writer *writer) {
    auto &encoder = writer->encoder();
    encoder.finish();
    m_lastBlk->sync(encoder, 0, encoder.last());
    saveBlk();
    m_writer = {}; // do this last to ensure +ve ref count
  }

  // save block to database (always m_lastBlk)
  void saveBlk() {
    static auto lastFn = [](const Blk *blk) {
      if constexpr (Fixed)
	return blk->last.fixed;
      else
	return blk->last.float_;
    };
    if (m_lastBlk->blkData->state() == ZdbObjState::Undefined) {
      ZdbObjRef<DBBlkHdr> blkHdr = new ZdbObject<DBBlkHdr>{blkHdrTbl()};
      new (blkHdr->ptr()) DBBlkHdr{
	.blkOffset = m_lastBlkOffset,
	.offset = m_lastBlk->offset(),
	.last = lastFn(m_lastBlk),
	.seriesID = series->id(),
	.count = m_lastBlk->count(),
	.ndp = m_lastBlk->ndp()
      };
      blkHdrTbl()->insert(series->shard(), ZuMv(blkHdr),
	[](ZdbObject<DBBlkHdr> *blkHdr) { if (blkHdr) blkHdr->commit(); });
      m_db->blkDataTbl()->insert(
	series->shard(), m_lastBlk->blkData,
	[l = ZuMv(l)](ZdbObject<DB::BlkData> *blkData) mutable {
	  if (!blkData) { l(nullptr); return; }
	  blkData->commit();
	  blkData->unpin();
	});
    } else {
      auto updFn = [](ZdbObject<DBBlkHdr> *blkHdr) {
	if (!blkHdr) return;
	auto &data = blkHdr->data();
	data.offset = m_lastBlk->offset();
	data.last = lastFn(m_lastBlk);
	data.count = m_lastBlk->count();
	data.ndp = m_lastBlk->ndp();
	blkHdr->commit();
      };
      blkHdrTbl()->findUpd<0>(series->shard(),
	ZuFwdTuple(series->id(), m_lastBlkOffset), ZuMv(updFn));
      m_db->blkDataTbl()->update<>(m_lastBlk->blkData,
	[l = ZuMv(l)](ZdbObject<DB::BlkData> *blkData) mutable {
	  if (!blkData) { l(nullptr); return; }
	  blkData->commit();
	  blkData->unpin();
	});
    }
  }

  // load blk from database
  template <typename L>
  void loadBlk(BlkOffset blkOffset, L l) {
    m_db->blkDataTbl()->find<0>(
      shard(), ZuFwdTuple(id(), blkOffset), ZuMv(l),
      [this](ZdbTable<DB::BlkData> *tbl) { return new BlkData{this, tbl}; });
  }

  // load block data from DB (idempotent)
  template <typename L>
  void loadBlkData(BlkOffset blkOffset, L l) const {
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) { l(nullptr); return; }
    auto blk = &indexBlk->blks[blkOffset - indexBlk->offset];
    if (blk->blkData) { l(blk); return; }
    loadBlk(this, blkOffset, [
      indexBlk = ZuMv(indexBlk), blk, l = ZuMv(l) // keep indexBlk in scope
    ](ZmRef<BlkData> blkData) mutable {
      if (ZuUnlikely(!blkData)) { l(nullptr); return; }
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
  auto seekFn(BlkOffset blkOffset, Offset target) const {
    return [this, blkOffset, target](uint64_t i) -> double {
      i += blkOffset;
      auto blk = getBlk(i);
      ZeAssert(blk, (), "internal error - null block", throw InternalError{});
      Offset offset = blk->offset();
      if (target < offset) return double(target) - double(offset);
      auto n = blk->count();
      ZeAssert(n, (), "internal error - empty block", throw InternalError{});
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
	ZeAssert(blk, (), "internal error - null block", throw InternalError{});
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
      ZeAssert(blk, (), "internal error - null block", throw InternalError{});
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
  void purge(Reader *reader) {
    auto blkOffset = reader->blkOffset()
    blkOffset &= ~IndexBlkMask();
    if (ZuUnlikely(!blkOffset)) return;
    auto blk = getBlk(blkOffset - 1);
    if (ZuUnlikely(!blk)) return;
    if (Fixed)
      write_firstValue(ZuFixed{blk->last.fixed, blk->ndp()});
    else
      write_firstValue(blk->last.float_);
    purgeBlks(blkOffset);
  }

  bool lastBlk(Blk *blk) const { return blk == m_lastBlk; }

private:
  DB			*m_db = nullptr;
  ZdbObjRef<DBType>	m_dbSeries;
  DataFrame		*m_df = nullptr;
  Index			m_index;
  Blk			*m_lastBlk = nullptr;
  BlkOffset		m_lastBlkOffset = 0;
  ReaderList		m_liveReaders;
  ReaderList		m_histReaders;
  ZmRef<Writer>		m_writer;
  bool			m_opened = false;
};

inline void BlkData::evict()
{
  m_series->unloadBlkData(this);
  ZdbAnyObject::evict();
}

// Reader implementation

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
inline bool Reader_<Decoder>::seekFwd(Offset offset)
{
  using namespace ReaderState;

  if (m_state != Stopped) return false;
  m_series->seekFwd(this, m_blkOffset, offset);
  return true;
}

template <typename Decoder>
inline bool Reader_<Decoder>::seekRev(Offset offset)
{
  using namespace ReaderState;

  if (m_state != Stopped) return false;
  m_series->seekRev(this, m_blkOffset, offset);
  return true;
}

template <typename Decoder>
inline bool Reader_<Decoder>::findFwd(const ZuFixed &value)
{
  using namespace ReaderState;

  if (m_state != Stopped) return false;
  m_series->findFwd(this, m_blkOffset, value);
  return true;
}

template <typename Decoder>
inline bool Reader_<Decoder>::findRev(const ZuFixed &value)
{
  using namespace ReaderState;

  if (m_state != Stopped) return false;
  m_series->findRev(this, m_blkOffset, value);
  return true;
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
    case Live:
      m_series->delLiveReader(this);
      m_series->addHistReader(this);
    case Reading:
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

  if (ZuUnlikely(!m_target.is<void>())) {
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

  // nextValue() return is ignored - unlike write_notify(), this caller
  // doesn't care whether the reader hit EOS
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
      m_state = Live;
      m_series->delHistReader(this);
      m_series->addLiveReader(this);
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
      if (!nextBlk()) return true;	// hit EOS, went live if not already
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
  using namespace ReaderState;

  m_failed = true;

  if (m_state == Live) {
    m_series->delLiveReader(this);
    m_series->addHistReader(this);
  }

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
inline bool Writer<Decoder>::write(Value value)
{
  ZeAssert(m_series, (), closedWriter(), return false);

  return m_series->write(this, value);
}

template <typename Decoder>
inline void Writer<Decoder>::stop()
{
  if (m_series) {
    m_series->stop(this);
    m_series = nullptr;
    m_encoder = {};
  }
}

} // namespace Zdf

#endif /* ZdfSeries_HH */
