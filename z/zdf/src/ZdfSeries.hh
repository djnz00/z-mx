//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series
// - chunked into blocks
// - compressed (see ZdfCompress)
// - indexable (if monotonically increasing, e.g. time series)
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

class Series;

template <typename Decoder_>
class Reader_ : public ZmObject {
public:
  using Decoder = Decoder_;
  using Series = Zdf::Series<Decoder>;

private:
friend Series;

  Reader_() = delete;
  Reader_(const Reader_ &) = delete;
  Reader_ &operator =(const Reader_ &) = delete;
  Reader_(Reader_ &&) = delete;
  Reader_ &operator =(Reader_ &&) = delete;

  Reader_(
    const Series *s, BlkOffset blkOffset, Blk *blk,
    Decoder decoder, bool live) :
    m_series{s}, m_blkOffset{blkOffset}, m_blk{ZuMv(blk)},
    m_decoder{ZuMv(decoder)}, m_ndp{blk->ndp()}, m_live{live} { }

  void init(BlkOffset blkOffset, Blk *blk, Decoder d, bool live) {
    m_blkOffset = blkOffset;
    m_blk = blk;
    m_decoder = ZuMv(d);
    m_ndp = blk->ndp();
    m_live = live;
  }

  void null() {
    m_blkOffset = 0;
    m_blk = nullptr;
    m_decoder = {};
    m_ndp = 0;
    m_live = false;
  }

public:
  bool operator !() const { return !m_blk; }
  ZuOpBool

  // seek forward to offset
  void seekFwd(uint64_t offset);
  // seek reverse to offset
  void seekRev(uint64_t offset);

  // series must monotonically increase to use find*() (e.g. time series)

  // seek forward to >= value
  void findFwd(const ZuFixed &value);
  // seek backwards to >= value
  void findRev(const ZuFixed &value);

  // read values
  void read(ZmFn<bool(const ZuFixed &)> fn);	// fixed point
  void read(ZmFn<bool(double)> fn);		// floating point

  // close reader (idempotent)
  void close();

  // purge historical data up to current read position
  void purge();

  // return value offset of current read position
  uint64_t offset() const {
    if (ZuUnlikely(!*this)) return 0;
    return m_blk()->offset() + m_decoder.count();
  }

private:
  const Series		*m_series = nullptr;
  BlkOffset		m_blkOffset = 0;
  Blk			*m_blk = nullptr;
  unsigned		m_ndp = 0;
  Decoder		m_decoder;
  bool			m_live = false;
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

friend Series;

  Writer_(Series *series, Encoder encoder) :
    m_series{series}, m_encoder{ZuMv(encoder)} { }
  ~Writer_() { close(); }

  // append value to series
  bool write(const ZuFixed &);	// fixed point
  bool write(double);		// floating point

  // sync block header (notifies any waiting readers)
  void sync();

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
  Series(DB *db, ZmRef<ZdbAnyObject> dbObject) :
    m_db{db}, m_dbObject{ZuMv(dbObject)} { dbObject->pin(); }
  ~Series() { dbObject->unpin(); }

public:
  DB *db() const { return m_db; }
  ZdbAnyObject *dbObject() const { return m_dbObject; }
  unsigned shard() const { return m_dbObject->shard(); }
  DataFrame *df() const { return m_df; }
  ID id() const { return data().id; }

private:
  void init(DataFrame *df) {
    m_df = df;
  }
  void final() {
    unpin();
    m_index.final();
  }

  // open series and fill index
  void open(OpenSeriesFn fn) {
    m_index.head(data().blkOffset);
    m_db->blkHdrTbl()->selectRows<0>(
      ZuFwdTuple(data().id), IndexBlkSize(), ZuLambda{[
	this_ = ZmMkRef(this), fn = ZuMv(fn), rowRcvd = false
      ](auto &&self, auto result, unsigned) mutable {
	if (this_->m_opened) return; // index already filled
	using Row = ZuFieldTuple<DB::BlkHdr>;
	if (result.template is<Row>()) { // fill index
	  this_->m_lastBlkOffset = result.template p<Row>().template p<1>();
	  this_->m_lastBlk = loadBlkHdr(result.template p<Row>());
	  if (!this_->m_lastBlk) {
	    this_->m_opened = true;
	    fn(ZuMv(this_));
	  }
	  rowRcvd = true;
	} else { // complete
	  if (!rowRcvd) {
	    this_->m_opened = true;
	    fn(ZuMv(this_));
	  } else {
	    rowRcvd = false;
	    this_->m_db->blkHdrTbl()->nextRows<0>(
	      ZuFwdTuple(data().id, this_->m_lastBlkOffset), false,
	      IndexBlkSize(), ZuMv(self));
	  }
	}
      }});
  }

  void final() { m_index.reset(0); }

  // there are 7 possible Writer states when write() is called:
  // - 1] new writer, new series
  // - 2] new writer, series with full last block
  // - 3] new writer, series with partial last block, different NDP
  // - 4] new writer, series with partial last block, same NDP
  // - 5] existing writer, series with full last block
  // - 6] existing writer, series with partial last block, different NDP
  // - 7] existing writer, series with partial last block, same NDP
  // begin writing (appending) to series

  void write(ZmFn<void(ZmRef<Writer>)> fn) {
    if (m_writer) return nullptr;
    // if the series is empty, append a new index block
    if (!m_lastBlk) { // new series
      IndexBlk *indexBlk = new IndexBlk{0};
      m_index.add(indexBlk);
      m_lastBlk = &indexBlk->data[0];
      m_lastBlkOffset = 0;
      m_lastBlk->blkData = new BlkData{this};
      write3(ZuMv(fn));
    } else if (m_lastBlk->count()) {
      if (!m_lastBlk->blkData) {
	ZmRef<IndexBlk> indexBlk = m_index.find(m_lastBlkOffset);
	db()->loadBlk(this, m_lastBlkOffset, [
	  this, fn = ZuMv(fn), indexBlk = ZuMv(indexBlk),
	](ZmRef<BlkData> blkData) mutable {
	  blkData->pin();
	  m_lastBlk->blkData = ZuMv(blkData);
	  write2(ZuMv(fn));
	});
      }
    } else if (m_lastBlk->blkData) {
      if (m_lastBlk->blkData->state() == Committed)
	m_lastBlk->blkData->pin();
      write3(ZuMv(fn));
    } else {
      m_lastBlk->blkData = new BlkData{this};
      write3(ZuMv(fn));
    }
  }
private:
  void write2(ZmFn<void(ZmRef<Writer>)> fn) {
    if (!m_lastBlk->count()) {
      write3(ZuMv(fn));
      return;
    }
    if (m_lastBlk->space() < 8) {
      appendBlock();
      write3(ZuMv(fn));
      return;
    }
    const auto &buf = m_lastBlk->blkData->buf;
    Decoder decoder{buf.data(), buf.data() + buf.length()};
    while (decoder.skip()); // skip to end
    ZmRef<Writer> writer =
      new Writer{this, Encoder{decoder, buf.data() + BlkSize}};
    fn(ZuMv(writer));
  }
  void write3(ZmFn<void(ZmRef<Writer>)> fn) {
    ZmRef<Writer> writer = new Writer{this, m_lastBlk->encoder<Encoder>()};
    fn(ZuMv(writer));
  }

  void appendBlock() {
    auto offset = m_lastBlk->offset() + m_lastBlk->count();
    ++m_lastBlkOffset;
    ZmRef<IndexBlk> indexBlk = m_index.find(m_lastBlkOffset);
    if (!indexBlk)
      m_index.add(indexBlk = new IndexBlk{m_lastBlkOffset & ~IndexBlkMask()});
    m_lastBlk = &indexBlk->data[m_lastBlkOffset - indexBlk->offset];
    m_lastBlk->offset(offset);
    m_lastBlk->blkData = new BlkData{this};
    m_lastBlk->blkData->pin();
  }

  void write(Writer *writer, const ZuFixed &value) {
    // FIXME - if this is the first value, update the db series (F&F)
    auto &encoder = writer->encoder();
    if (value.ndp != m_ndp || !encoder.write(value.mantissa)) {
      m_lastBlk->sync(encoder, m_ndp, encoder.last());
      ZmAssert(blkData->pinned());
      m_db->saveBlk(this, m_lastBlk,
	[](ZdbObject<DB::BlkData> *blkData) { blkData->unpin(); });
      appendBlock();
      encoder = m_lastBlk->encoder<Encoder>();
      m_ndp = value.ndp;
      if (ZuUnlikely(!encoder.write(value.mantissa))) return false;
    }
  }

  void write(Writer *writer, double value) {
  }

  // FIXME - find needs to support double

  // begin reading from offset
  ZmRef<Reader> seek(uint64_t offset = 0) const {
    // FIXME - new Reader
    // add reader to list
    // return handle, which is ZmRef<Reader>
    // lists own the readers (maintain a ref to them)
    //
    // app must explicitly call detach() to finish reading, and then dtor
    // does nothing - failing to close will leak resources - note that
    // detach() must be called on-thread, precluding use of RAII dtor
    // - readers in detached state cannot perform any reading/seeking/etc.
    //   (they are zombies)
    fn(new Reader{/* FIXME */});
  }

  // begin reading from value
  ZmRef<Reader> find(const ZuFixed &value) const {
    fn(new Reader{/* FIXME */});
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], m_blks.length(),
	  findFn(value)),
	value);
  }

  // value count (length of series in #values)
  uint64_t count() const {
    if (ZuUnlikely(!blkCount())) return 0;
    auto blk = lastBlk();
    if (ZuUnlikely(!blk)) return 0;
    return blk->offset() + blk->count();
  }

  // length in bytes (compressed)
  // - intentionally inaccurate and mildly overestimated
  // - will not return under the actual value
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

  bool loadBlkHdr(const ZuFieldTuple<DB::BlkHdr> &row) {
    auto blk = m_index.set(row.p<1>());
    if (!blk) return false;
    blk->init(row.p<2>(), row.p<4>(), row.p<5>(), row.p<3>());
    return true;
  }

// Series has two reader lists - one live (watching at EOS), one historical
//
// add to historical initially
// remove uses live status to know which one it's in
// watch/unwatch ensure m_live and list membership are kept in sync

// a reader that is not actively reading is historical
// a live reader that ceases reading goes historical
// a reader that is active reading is historical if prior to EOS
// an active reader that hits EOS goes live

// FIX: Writer.open() loads initial block if needed, can be async
// - from then on, write() is always sync, since it writes in memory
//   to ZdbObject<>, and completes with an insert or update depending
//   on object state, which can be fire-and-forget (next block will
//   again be a new blank block)
// - Reader.open() similarly can async load blkData, but read()
//   needs find to complete on a new block. which is async

  // FIXME from here

  bool write(Writer *writer, const ZuFixed &value) {
    // NOTES
    // - insert/update the block when advancing to the next one,
    //   OR when closing the writer (fire+forget)
    // - extend initially with blank block (this is done by blk->encoder())
    // - encoder is reinitialized with each new block
    // - if starting mid-way through an existing block
    //   - 1] load it using loadBlkData()
    //   - 2] read it to end using temporary decoder
    //   - 3] construct encoder from decoder
    // - if starting a new block, just initialize a new encoder for the block
    // - when writing a value, iterate over any watchers and notify them
    auto blkOffset = m_index.tail();

    template <typename Encoder>
    Encoder encoder() {
      // FIXME - may need to loadBlkData
      // FIXME - continuation construct Encoder from Decoder to
      // write a reset to the block
    }
    // FIXME
    // use blk->encoder() etc. to initialize encoder
    // use db()->saveBlk() to save block to disk

  template <typename Encoder>
  ZuTuple<BlkOffset, Blk *, Encoder> encoder() {
    Blk *blk = set(blkOffset);
    return ZuFwdTuple(blkOffset, blk, blk->encoder<Encoder>());
  }
  template <typename Encoder>
  ZuTuple<BlkOffset, Blk *, Encoder>
  nextEncoder(BlkOffset blkOffset, Blk *blk) {
    blkOffset = blkOffset + 1;
    offset = blk->offset() + blk->count();
    // FIXME from here
    buf = new Buf{m_store, m_seriesID, blkOffset};
    new (Blk::new_<ZmRef<Buf>>(m_blks.push())) ZmRef<Buf>{buf};
    new (buf->hdr()) Hdr{offset, 0};
    m_store->push(buf);
    {
      blkOffset = buf->blkOffset;
      const auto *hdr = buf->hdr();
      offset = hdr->offset() + hdr->count();
    }
    return ZuFwdTuple(blk, blk->encoder<Encoder>());
  }

    bool eob;
    if (ZuUnlikely(!m_blk)) { // new writer
      auto blkEncoder = m_series->template encoder<Encoder>();
      m_blkOffset = ZuMv(blkEncoder).p<BlkOffset>();
      m_blk = ZuMv(blkEncoder).p<Blk *>();
      m_encoder = ZuMv(blkEncoder).p<Encoder>();
      if (m_blk->count()) {
	loadBlkData(m_blk->blkOffset(), 
      }
      if (ZuUnlikely(!m_blk)) return false;
      m_blk->blkData->pin();
      m_ndp = value.ndp();
      eob = false;
    } else {
      eob = value.ndp() != m_ndp;
    }
    if (eob || !m_encoder.write(value.mantissa)) {
      sync();
      save(); // FIXME - causes insert or update of blkData to back-end DB
      m_encoder = m_series->template nextEncoder<Encoder>(m_buf);
      if (ZuUnlikely(!m_buf)) return false;
      m_buf->pin();
      m_ndp = value.ndp();
      if (ZuUnlikely(!m_encoder.write(value.mantissa))) return false;
    }
    return true;
  }

  // stop writing
  void close(Writer *writer) {
    // FIXME
  }

  void read(...) {
// FIXME
    if (ZuUnlikely(!*this)) return false;
    ZuFixedVal mantissa;
    if (ZuUnlikely(!m_decoder.read(mantissa))) {
      m_series->nextBlk(this);
      if (ZuUnlikely(!*this)) return false;
      if (ZuUnlikely(!m_decoder.read(mantissa)))
	return false;
    }
    value = {mantissa, m_ndp};
    return true;
  }

  // get Blk from index
  const Blk *get(BlkOffset blkOffset) const {
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk) return nullptr;
    return &indexBlk->blks[blkOffset - indexBlk->offset];
  }

  // set Blk in index
  Blk *set(BlkOffset blkOffset) {
    if (ZuUnlikely(blkOffset < m_index.head())) return nullptr;
    ZmRef<IndexBlk> indexBlk = m_index.find(blkOffset);
    if (!indexBlk)
      m_index.add(indexBlk = new IndexBlk{blkOffset & ~IndexBlkMask()});
    return &indexBlk->data[blkOffset - indexBlk->offset];
  }

  // purge index up to but not including blkOffset
  void purge(BlkOffset blkOffset) {
    blkOffset &= ~IndexBlkMask();
    if (blkOffset < m_index.head()) return; // prevent inadvertent reset
    if (blkOffset >= m_lastBlkOffset) return; // prevent lastBlk removal
    m_index.head(blkOffset);
  }

  // get last (most recent) block in index
  Blk *lastBlk() const { return m_lastBlk; }

private:
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

  template <typename Decoder>
  void seek_(unsigned search, uint64_t offset, ZmFn<void(Decoder)> fn) const {
    loadBlkData(ZuSearchPos(search), [fn = ZuMv(fn)](Blk *blk) {
      if (ZuUnlikely(!blk)) goto null;
      auto reader = blk->decoder<Decoder>();
      uint64_t offset_ = blk()->offset();
      if (offset_ >= offset) return reader;
      if (!reader.seek(offset - offset_)) goto null;
      fn(ZuMv(reader));
      return;
    null:
      fn(Decoder{});
    });
  }
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
  auto seekFn(uint64_t offset) const {
    return [this, offset](uint64_t i) -> int {
      auto blk = get(i + m_index.head());
      ZeAssert(blk, (), "out of bounds", return 1);
      auto offset_ = blk->offset();
      if (offset < offset_) return -int(offset_ - offset);
      offset_ += blk->count();
      if (offset >= offset_) return int(offset - offset_) + 1;
      return 0;
    };
  }
  auto findFn(ZuFixed value) const {
    return [this, value](uint64_t i) -> int {
      // get last value from preceding blk
      ZuFixed value_;
      if (!i)
	value_ = first; // series first
      else {
	auto blk = get((i - 1) + m_index.head());
	ZeAssert(blk, (), "out of bounds", return INT_MAX);
	value_ = ZuFixed{blk->last(), blk->ndp()};
      }
      value_.mantissa = value_.adjust(value.ndp);
      if (value.mantissa < value_.mantissa) {
	int64_t delta = value_.mantissa - value.mantissa;
	if (ZuUnlikely(delta >= int64_t(INT_MAX)))
	  return INT_MIN;
	return -int(delta);
      }
      auto blk = get(i + m_index.head());
      ZeAssert(blk, (), "out of bounds", return INT_MAX);
      value_ = ZuFixed{blk->last(), blk->ndp()};
      value_.mantissa = value_.adjust(value.ndp());
      if (value.mantissa > value_.mantissa) {
	int64_t delta = value.mantissa - value_.mantissa;
	if (ZuUnlikely(delta >= int64_t(INT_MAX)))
	  return INT_MAX;
	return int(delta);
      }
      return 0;
    };
  }

  Decoder seek(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(m_index.blkCount(), seekFn(offset)),
	offset);
  }
  // FIXME from here
  Decoder seekFwd(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[buf->blkOffset], m_blks.length() - buf->blkOffset,
	  seekFn(offset)),
	offset);
  }
  Decoder seekRev(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(&m_blks[0], buf->blkOffset + 1, seekFn(offset)),
	offset);
  }

  Decoder find(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], m_blks.length(),
	  findFn<Decoder>(value)),
	value);
  }
  Decoder findFwd(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[buf->blkOffset], m_blks.length() - buf->blkOffset,
	  findFn<Decoder>(value)),
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

  // sync block header with data
  void sync(Writer *writer) {
    if (ZuLikely(writer->m_blk))
      writer->m_blk->sync(m_encoder, m_ndp, m_encoder.last());
  }
    
  // save block header and data to DB
  void save(Writer *writer) {
    if (ZuLikely(writer->m_blk))
      save(writer->m_blk);
  }

  void close(Reader *reader) {
    // FIXME - unwatch, manage lists etc.
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
  ZmRef<ZdbAnyObject>	m_dbObject;
  DataFrame		*m_df = nullptr;
  Index			m_index;
  Blk			*m_lastBlk = nullptr;
  uint64_t		m_lastBlkOffset = 0;
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

inline constexpr const char *closedReader() {
  return "attempt to use closed Reader";
}
inline constexpr const char *closedWriter() {
  return "attempt to use closed Writer";
}

template <typename Decoder>
inline void Reader<Decoder>::seekFwd(uint64_t offset)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->seekFwd(this, offset);
}

template <typename Decoder>
inline void Reader<Decoder>::seekRev(uint64_t offset)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->seekRev(this, offset);
}

template <typename Decoder>
inline void Reader<Decoder>::findFwd(const ZuFixed &value)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->findFwd(this, value);
}

template <typename Decoder>
inline void Reader<Decoder>::findRev(const ZuFixed &value)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->findRev(this, value);
}

template <typename Decoder>
inline void Reader<Decoder>::read(ZmFn<bool(const ZuFixed &)> fn)
{
  ZeAssert(m_series, (), closedReader(), return);
  m_series->read(this, ZuMv(fn));
}

template <typename Decoder>
inline void Reader<Decoder>::close()
{
  if (m_series) {
    m_series->close(this);
    m_series = nullptr;
  }
}

template <typename Decoder>
inline bool Writer<Decoder>::write(const ZuFixed &value)
{
  ZeAssert(m_series, (), closedWriter(), return false);
  return m_series->write(this, value);
}

template <typename Decoder>
inline bool Writer<Decoder>::write(double value)
{
  ZeAssert(m_series, (), closedWriter(), return false);
  return m_series->write(this, value);
}

template <typename Decoder>
inline void Writer<Decoder>::sync()
{
  ZeAssert(m_series, (), closedWriter(), return);
  return m_series->sync(this, value);
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
