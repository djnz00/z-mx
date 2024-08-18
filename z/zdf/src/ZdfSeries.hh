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

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfBlk.hh>

namespace Zdf {

template <typename> class Series;

template <typename> struct ValueMap_;
template <> struct ValueMap_<int64_t> { using T = ZuFixed; };
template <> struct ValueMap_<double> { using T = double; };
template <typename U> using ValueMap = typename ValueMap_<U>::T;

// the choice of decoder determines the value type (fixed or floating point)
template <typename Decoder_>
class Reader_ : public ZmObject {
public:
  using Decoder = Decoder_;
  using Series = Zdf::Series<Decoder>;
  using Value = ValueMap<typename Decoder::Value>;
  using Fn = ZmFn<bool(Value)>;

private:
friend Series;

  Reader_() = delete;
  Reader_(const Reader_ &) = delete;
  Reader_ &operator =(const Reader_ &) = delete;
  Reader_(Reader_ &&) = delete;
  Reader_ &operator =(Reader_ &&) = delete;

  Reader_(const Series *series, BlkOffset blkOffset, Blk *blk, Offset offset) :
    m_series{series}, m_blkOffset{blkOffset}, m_blk{blk}, m_offset{offset}
  {
    ZeAssert(m_blk, (), "null blk", return);
  }

  ~Reader_() { close(); }

  void final() { m_fn = {}; }

public:
  Series *series() const { return m_series; }
  Offset offset() const { return m_offset; }
  bool live() const { return m_live; }
  bool failed() const { return m_failed; }

  NDP ndp() const { return m_blk ? m_blk->ndp() : 0; }

  // seek forward to offset
  void seekFwd(uint64_t offset);
  // seek reverse to offset
  void seekRev(uint64_t offset);

  // series must monotonically increase to use find*() (e.g. time series)

  // seek forward to >= value
  void findFwd(ZuFixed value);
  // seek backwards to >= value
  void findRev(ZuFixed value);

  // read values
  void read(Fn);

  // close reader (idempotent)
  void close();

  // purge historical data up to current read position
  void purge();

private:
  BlkOffset blkOffset() const { return m_blkOffset; }
  Blk *blk() const { return m_blk; }

  bool readValue();

  void init(BlkOffset blkOffset, Blk *blk, Offset offset) {
    ZeAssert(blk, (), "null blk", return);

    m_blkOffset = blkOffset;
    m_blk = blk;
    m_offset = offset;
    m_decoder = {};
    m_fn = {};
    m_live = false;
    m_failed = false;
  }

  bool loaded() {
    ZeAssert(m_blk, (), "null m_blk", goto error);

    auto offset = m_blk->offset();

    ZeAssert(m_offset >= offset, (), "m_blk offset mismatch", goto error);

    m_decoder = m_blk->decoder<Decoder>();

    if (m_offset > offset)
      if (!decoder.seek(m_offset - offset))
	return false;

    return true;

  error:
    failed();
    return false;
  }

  bool notify() {
    if (ZuUnlikely(!m_fn)) return false;
    ZeAssert(m_decoder, (), "null decoder", goto failed);
    Decoder::Value value;
    if (!m_decoder.read(value)) return false;
    bool cont;
    if constexpr (ZuIsExact<Value, ZuFixed>{})
      cont = m_fn(ZuFixed{value, ndp()});
    else
      cont = m_fn(value);
    if (!cont) {
      m_fn = {};
      return false;
    }
    return true;
  failed:
    failed();
    return false;
  }

  void live(bool v) { m_live = v; }

  void failed() {
    m_blkOffset = 0;
    m_blk = nullptr;
    m_offset = 0;
    m_decoder = {};
    m_fn = {};
    m_live = false;
    m_failed = true;
  }

private:
  const Series		*m_series = nullptr;
  BlkOffset		m_blkOffset = 0;
  Blk			*m_blk = nullptr;
  Offset		m_offset = 0;
  Decoder		m_decoder;
  Fn			m_fn;
  bool			m_live = false;
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

friend DB;
friend DataFrame;
friend Reader;
friend Writer;

using ID = uint32_t;

private:
  Series(DB *db, bool fixed, ZmRef<ZdbAnyObject> dbObject) :
    m_db{db}, m_dbObject{ZuMv(dbObject)}, m_fixed{fixed} { dbObject->pin(); }
  ~Series() { dbObject->unpin(); }

public:
  DB *db() const { return m_db; }
  ZdbAnyObject *dbObject() const { return m_dbSeries; }
  ZdbObject<DB::SeriesFixed> *dbSeriesFixed() const {
    ZmAssert(m_fixed);
    return m_dbSeries.ptr<ZdbObject<DB::SeriesFixed>>();
  }
  ZdbObject<DB::SeriesFloat> *dbSeriesFloat() const {
    ZmAssert(!m_fixed);
    return m_dbSeries.ptr<ZdbObject<DB::SeriesFloat>>();
  }
  unsigned shard() const { return m_dbSeries->shard(); }
  DataFrame *df() const { return m_df; }
  ID id() const { return data().id; }
  bool opened() const { return m_opened; }
  bool fixed() const { return m_fixed; }

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
  // openFixed() - open fixed point series
  void openFixed(OpenSeriesFn fn) {
    m_index.head(dbSeriesFixed()->data().blkOffset);
    m_db->blkHdrFixedTbl()->selectRows<0>(
      ZuFwdTuple(data().id), IndexBlkSize(), ZuLambda{[
	this_ = ZmMkRef(this), fn = ZuMv(fn), rowRcvd = false
      ](auto &&self, auto result, unsigned) mutable {
	if (this_->m_opened) return; // index already filled
	using Row = ZuFieldTuple<DB::BlkHdrFixed>;
	if (result.template is<Row>()) { // fill index
	  if (!this_->open_loadBlkHdr(result.template p<Row>())) goto opened;
	  rowRcvd = true;
	} else { // complete
	  if (!rowRcvd) goto opened;
	  rowRcvd = false;
	  this_->m_db->blkHdrFixedTbl()->nextRows<0>(
	    ZuFwdTuple(data().id, this_->m_lastBlkOffset), false,
	    IndexBlkSize(), ZuMv(self));
	}
	return;
      opened:
	this_->m_opened = true;
	fn(ZuMv(this_));
      }});
  }
  // openFloat() - open floating point series
  void openFloat(OpenSeriesFn fn) {
    m_index.head(dbSeriesFloat()->data().blkOffset);
    m_db->blkHdrFloatTbl()->selectRows<0>(
      ZuFwdTuple(data().id), IndexBlkSize(), ZuLambda{[
	this_ = ZmMkRef(this), fn = ZuMv(fn), rowRcvd = false
      ](auto &&self, auto result, unsigned) mutable {
	if (this_->m_opened) return; // index already filled
	using Row = ZuFieldTuple<DB::BlkHdrFloat>;
	if (result.template is<Row>()) { // fill index
	  if (!this_->open_addBlkHdr(result.template p<Row>())) goto opened;
	  rowRcvd = true;
	} else { // complete
	  if (!rowRcvd) goto opened;
	  rowRcvd = false;
	  this_->m_db->blkHdrFloatTbl()->nextRows<0>(
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
private:
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
  void write_firstFixed(ZuFixed value) {
    auto dbSeries = dbSeriesFixed();
    dbSeries->data().first = value;
    m_db->seriesFixedTbl()->update<>(dbSeries, // fire-and-forget
      [](ZdbObject<DB::Series> *dbSeries) {
	if (dbSeries) dbSeries->commit();
      });
  }
  void write_firstFloat(double value) {
    auto dbSeries = dbSeriesFloat();
    dbSeries->data().first = value;
    m_db->seriesFloatTbl()->update<>(dbSeries, // fire-and-forget
      [](ZdbObject<DB::Series> *dbSeries) {
	if (dbSeries) dbSeries->commit();
      });
  }
  // main write function
  bool write_(Writer *writer, ZuFixed value) { // fixed-point version
    if (ZuUnlikely(!m_lastBlkOffset && !m_lastBlk->offset()))
      write_firstFixed(value);
    auto &encoder = writer->encoder();
    if (value.ndp == m_ndp && encoder.write(value.mantissa)) return true;
    encoder.finish();
    m_lastBlk->sync(encoder, m_ndp, encoder.last());
    ZmAssert(blkData->pinned());
    m_db->saveBlk(this, m_lastBlk,
      [](ZdbObject<DB::BlkData> *blkData) { blkData->unpin(); });
    pushBlk();
    encoder = m_lastBlk->encoder<Encoder>();
    m_ndp = value.ndp;
    return encoder.write(value.mantissa);
  }
  bool write_(Writer *writer, double value) { // floating-point version
    if (ZuUnlikely(!m_lastBlkOffset && !m_lastBlk->offset()))
      write_firstFloat(value);
    auto &encoder = writer->encoder();
    if (encoder.write(value)) return true;
    encoder.finish();
    m_lastBlk->sync(encoder, 0, encoder.last());
    ZmAssert(m_lastBlk->blkData->pinned());
    m_db->saveBlk(this, m_lastBlk,
      [](ZdbObject<DB::BlkData> *blkData) { blkData->unpin(); });
    pushBlk();
    encoder = m_lastBlk->encoder<Encoder>();
    return encoder.write(value);
  }
  // notify live readers
  template <typename Value>
  void write_notify() {
    auto i = m_liveReaders.iterator();
    while (auto reader = i.iterate())
      if (!reader->notify()) {
	reader->live(false);
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
      indexBlk = ZuMv(indexBlk), blk, l = ZuMv(l)
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

  ZmRef<Reader> seek(uint64_t offset) const {
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
  void seekFwd(Reader_ *reader, uint64_t offset) const {
    BlkOffset blkOffset = reader->blkOffset();
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
  void seekFwd(Reader_ *reader, uint64_t offset) const {
    BlkOffset blkOffset = reader->blkOffset();
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

  // FIXME - mimic seek above
  // begin reading from value
  void find(ZuFixed value, ZmFn<void(ZmRef<Reader>)> fn) const {
    try {
      // FIXME
      fn(new Reader{/* FIXME */});
      // FIXME
      return find_<Decoder>(buf,
	  ZuInterSearch(
	    &m_blks[0], m_blks.length(),
	    findFn(value)),
	  value);
    } catch (InternalError) {
      fn(nullptr);
    }
  }

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

private:
  // number of blocks in index
  unsigned blkCount() const { return (m_lastBlkOffset + 1) - m_index.head(); }

  // first blkOffset (will be non-zero following a purge())
  uint64_t head() const { return m_index.head(); }

  void read(Reader *reader) {
    loadBlkData(reader->blkOffset(), [
      this, reader = ZmMkRef(reader), fn = ZuMv(fn)
    ](Blk *blk) mutable {
      ZeAssert(blk, (), "null blk", goto error);
      ZeAssert(blk == reader->blk(), (), "blk mismatch", goto error);

      Offset offset_ = blk->offset();
      Offset offset = reader->offset();

      if (reader->loaded()) {
	m_histReaders.push(reader);
	readHist(reader);
	return;
      }

      if (reader->failed()) {
	// FIXME - need someway to inform app about failure, the
	// callback just provides a value
	return;
      }

      nextBlk(reader); // FIXME - warn - shouldn't really happen at first read
    });
  }

  void nextBlk(Reader *reader) {
    BlkOffset blkOffset = reader->blkOffset();

    if (blkOffset == m_lastBlkOffset) {
      m_histReaders.delNode(reader);
      reader->live(true);
      m_liveReaders.push(reader);
    }

    ++blkOffset;
    Blk *blk = getBlk(blkOffset);
    reader->init(blkOffset, blk, blk->offset());

    db()->run(shard(), [reader = ZmMkRef(this)]() mutable {
      reader->series()->read(reader);
    });
  }

  void readHist(Reader *reader) {
    if (reader->notify()) {
  again:
      db()->run(shard(), [reader = ZmMkRef(reader)]() mutable {
	reader->series()->readHist(reader);
      });
      return;
    }

    if (reader->failed()) {
      // FIXME
      return;
    }

    nextBlk(reader);
  }

private:
  template <typename L>
  void seek_(BlkOffset blkOffset, uint64_t offset, L l) const {
  }
  // FIXME - find_() - mimic seek_() above
  template <typename Decoder>
  void find_(unsigned search, ZuFixed value, ZmFn<void(Decoder)> fn) const {
    loadBlkData(ZuSearchPos(search), [value, fn = ZuMv(fn)](Blk *blk) {
      if (ZuUnlikely(!blk)) goto null;
      auto reader = blk->decoder<Decoder>();
      bool found = reader.search([
	mantissa = value.adjust(blk()->ndp())
      ](int64_t skip, unsigned count) -> unsigned {
	return skip < mantissa ? count : 0;
      });
      if (!found) goto null;
      fn(ZuMv(reader));
      return;
    null:
      fn(Decoder{});
    });
  }

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
  // find function used in interpolation search
  auto fixedFindFn(uint64_t blkOffset, ZuFixed value) const {
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

  Decoder find(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  blkCount(),
	  findFn<Decoder>(value)),
	value);
  }
  Decoder findFwd(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[buf->blkOffset], m_blks.length() - buf->blkOffset,
	  (m_lastBlkOffset + 1) - blkOffset
	  findFn<Decoder>(blkOffset, value)),
	value);
  }
  Decoder findRev(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], buf->blkOffset + 1,
	  findFn<Decoder>(value)),
	value);
  }

  Decoder nextDecoder(ZmRef<Buf> &buf) const {
    BlkOffset blkOffset = buf->blkOffset + 1;
    if (blkOffset >= m_blks.length()) goto null;
    if (!(buf = loadBuf(blkOffset))) goto null;
    return buf->reader<Decoder>();
  null:
    buf = nullptr;
    return Decoder{};
  }

  void close(Reader *reader) {
    // FIXME - unwatch, manage lists etc.
    if (m_live)
      m_liveReaders.delNode(reader);
    else
      m_histReaders.delNode(reader);
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

  bool loadHdr(unsigned i, Hdr &hdr) const {
    return m_store->loadHdr(m_seriesID, i, hdr);
  }
  ZmRef<Buf> load(unsigned i) const {
    ZmRef<Buf> buf = new Buf{m_store, m_seriesID, i};
    if (m_store->load(m_seriesID, i, buf->data()))
      return buf;
    return nullptr;
  }
  void save(ZmRef<Buf> buf) const {
    return m_store->save(ZuMv(buf));
  }

private:
  DB			*m_db = nullptr;
  ZmRef<ZdbAnyObject>	m_dbSeries;
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

inline constexpr const char *closedReader() {
  return "attempt to use closed Reader";
}
inline constexpr const char *closedWriter() {
  return "attempt to use closed Writer";
}

template <typename Decoder>
inline void Reader_<Decoder>::seekFwd(uint64_t offset)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->seekFwd(this, offset);
}

template <typename Decoder>
inline void Reader_<Decoder>::seekRev(uint64_t offset)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->seekRev(this, offset);
}

template <typename Decoder>
inline void Reader_<Decoder>::findFwd(const ZuFixed &value)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->findFwd(this, value);
}

template <typename Decoder>
inline void Reader_<Decoder>::findRev(const ZuFixed &value)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->findRev(this, value);
}

template <typename Decoder>
inline void Reader_<Decoder>::read(ReadFn fn)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_fn = ZuMv(fn);
  m_series->read(this);
}

template <typename Decoder>
template <
  typename _ = typename Decoder::Value,
  decltype(ZuExact<int64_t, _>(), int()) = 0>
inline bool Reader_<Decoder>::notify()
{
  ZeAssert(m_series, (), closedReader(), return false);
  if (ZuUnlikely(!m_fn)) return false;
  int64_t mantissa;
  if (ZuLikely(m_decoder.read(mantissa))) {
    m_fn(ZuFixed{mantissa, m_ndp});
    return true;
  }
  return false;
}

template <typename Decoder>
template <
  typename _ = typename Decoder::Value,
  decltype(ZuExact<double, _>(), int()) = 0>
inline bool Reader_<Decoder>::notify()
{
  ZeAssert(m_series, (), closedReader(), return false);
  if (ZuUnlikely(!m_fn)) return false;
  double value;
  if (ZuLikely(m_decoder.read(value))) {
    m_fn(value);
    return true;
  }
  return false;
}

template <typename Decoder>
inline void Reader_<Decoder>::close()
{
  if (m_series) {
    m_series->close(this);
    m_series = nullptr;
  }
}

template <typename Decoder>
inline void Reader_<Decoder>::purge()
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->purge(this);
}

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
