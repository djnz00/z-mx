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

template <typename Series, typename Decoder_>
class Reader {
public:
  using Decoder = Decoder_;

  Reader() { }
  Reader(const Reader &r) :
      m_series(r.m_series), m_buf(r.m_buf), m_ndp(r.m_ndp),
      m_decoder(r.m_decoder) { }
  Reader &operator =(const Reader &r) {
    if (ZuLikely(this != &r)) {
      this->~Reader(); // nop
      new (this) Reader{r};
    }
    return *this;
  }
  Reader(Reader &&r) :
      m_series(r.m_series), m_buf(ZuMv(r.m_buf)), m_ndp(r.m_ndp),
      m_decoder(ZuMv(r.m_decoder)) { }
  Reader &operator =(Reader &&r) {
    if (ZuLikely(this != &r)) {
      this->~Reader(); // nop
      new (this) Reader{ZuMv(r)};
    }
    return *this;
  }
  ~Reader() { }

private:
  Reader(const Series *s, ZmRef<Buf> buf, Decoder r) :
      m_series(s), m_buf(ZuMv(buf)), m_decoder(ZuMv(r)) {
    if (ZuUnlikely(!*this)) return;
    m_ndp = m_buf->hdr()->ndp();
  }

public:
  // start reading at offset
  static Reader seek(const Series *s, uint64_t offset = 0) {
    ZmRef<Buf> buf;
    auto decoder = s->template seek<Decoder>(buf, offset);
    return Reader{s, ZuMv(buf), ZuMv(decoder)};
  }
  // seek forward to offset
  void seekFwd(uint64_t offset) {
    if (ZuUnlikely(!*this)) return;
    m_decoder =
      m_series->template seekFwd<Decoder>(m_buf, offset);
    m_ndp = m_buf->hdr()->ndp();
  }
  // seek reverse to offset
  void seekRev(uint64_t offset) {
    if (ZuUnlikely(!*this)) return;
    m_decoder =
      m_series->template seekFwd<Decoder>(m_buf, offset);
    m_ndp = m_buf->hdr()->ndp();
  }

  // series must monotonically increase to use find*() (e.g. time series)

  // start reading from >= value
  static Reader find(const Series *s, const ZuFixed &value) {
    ZmRef<Buf> buf;
    auto decoder = s->template find<Decoder>(buf, value);
    return Reader{s, ZuMv(buf), ZuMv(decoder)};
  }
  // seek forward to >= value
  void findFwd(const ZuFixed &value) {
    if (ZuUnlikely(!*this)) return;
    m_decoder =
      m_series->template findFwd<Decoder>(m_buf, value);
    m_ndp = m_buf->hdr()->ndp();
  }
  // seek backwards to >= value
  void findRev(const ZuFixed &value) {
    if (ZuUnlikely(!*this)) return;
    m_decoder =
      m_series->template findRev<Decoder>(m_buf, value);
    m_ndp = m_buf->hdr()->ndp();
  }

  // read single value
  bool read(ZuFixed &value) {
    if (ZuUnlikely(!*this)) return false;
    ZuFixedVal mantissa;
    if (ZuUnlikely(!m_decoder.read(mantissa))) {
      m_decoder = m_series->template nextDecoder<Decoder>(m_buf);
      if (ZuUnlikely(!m_decoder || !m_decoder.read(mantissa)))
	return false;
      m_ndp = m_buf->hdr()->ndp();
    }
    value = {mantissa, m_ndp};
    return true;
  }

  void purge() {
    if (ZuUnlikely(!*this)) return;
    const_cast<Series *>(m_series)->purge_(m_buf->blkOffset);
  }

  uint64_t offset() const {
    if (ZuUnlikely(!*this)) return 0;
    return m_buf->hdr()->offset() + m_decoder.count();
  }

  bool operator !() const { return !m_decoder; }
  ZuOpBool

private:
  const Series	*m_series = nullptr;
  ZmRef<Buf>	m_buf;
  unsigned	m_ndp = 0;
  Decoder	m_decoder;
};

template <typename Series, typename Encoder_>
class Writer {
  Writer(const Writer &) = delete;
  Writer &operator =(const Writer &) = delete;

public:
  using Encoder = Encoder_;

  Writer(Series *s) : m_series(s) { }

  Writer() { }
  Writer(Writer &&w) :
      m_series(w.m_series),
      m_buf(ZuMv(w.m_buf)),
      m_ndp(w.m_ndp),
      m_encoder(ZuMv(w.m_encoder)) {
    w.m_series = nullptr;
    w.m_buf = nullptr;
    // w.m_ndp = 0;
  }
  Writer &operator =(Writer &&w) {
    if (ZuLikely(this != &w)) {
      this->~Writer(); // nop
      new (this) Writer{ZuMv(w)};
    }
    return *this;
  }
  ~Writer() {
    sync();
    save();
  }

  void sync() {
    if (ZuLikely(m_buf)) m_buf->sync(m_encoder, m_ndp, m_encoder.last());
  }

  void save() {
    if (ZuLikely(m_buf)) m_series->save(m_buf);
  }

  bool write(const ZuFixed &value) {
    bool eob;
    if (ZuUnlikely(!m_buf)) {
      m_encoder = m_series->template encoder<Encoder>(m_buf);
      if (ZuUnlikely(!m_buf)) return false;
      m_buf->pin();
      m_ndp = value.ndp();
      eob = false;
    } else {
      eob = value.ndp() != m_ndp;
    }
    if (eob || !m_encoder.write(value.mantissa())) {
      sync();
      save();
      m_encoder = m_series->template nextEncoder<Encoder>(m_buf);
      if (ZuUnlikely(!m_buf)) return false;
      m_buf->pin();
      m_ndp = value.ndp();
      if (ZuUnlikely(!m_encoder.write(value.mantissa()))) return false;
    }
    return true;
  }

private:
  Series	*m_series = nullptr;
  ZmRef<Buf>	m_buf;
  unsigned	m_ndp = 0;
  Encoder	m_encoder;
};

// each IndexBlk contains Blk[512]
inline constexpr const unsigned IndexBlkShift() { return 9; }
inline constexpr const unsigned IndexBlkSize() { return 1<<IndexBlkShift(); }
inline constexpr const unsigned IndexBlkMask() {
  return ~((~0U)<<IndexBlkShift());
}

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
using IndexQueue =
  ZmPQueue<IndexBlk_,
    ZmPQueueFn<IndexBlk_Fn,
      ZmPQueueNode<IndexBlk_,
	ZmPQueueStats<false,
	  ZmPQueueOverlap<false,
	    ZmPQueueBits<3,
	      ZmPQueueLevels<3,
		ZmPQueueHeapID<IndexBlk_HeapID>>>>>>>>;
using IndexBlk = IndexQueue::Node;

class Index {
public:
  void init(unsigned blkOffset) { m_queue.head(blkOffset); }
  void final() { m_queue.reset(0); }

  uint64_t head() const { return m_queue.head(); }

  const Blk *get(unsigned blkOffset) const {
    ZuRef<IndexBlk> indexBlk = m_queue.find(blkOffset);
    if (!indexBlk) return nullptr;
    return &indexBlk->blks[blkOffset - indexBlk->offset];
  }

  Blk *set(unsigned blkOffset) {
    if (ZuUnlikely(blkOffset < m_queue.head())) return nullptr;
    ZuRef<IndexBlk> indexBlk = m_queue.find(blkOffset);
    if (!indexBlk)
      m_queue.add(indexBlk = new IndexBlk{blkOffset & ~IndexBlkMask()});
    return &indexBlk->data[blkOffset - indexBlk->offset];
  }

  void purge(unsigned blkOffset) {
    blkOffset &= ~IndexBlkMask();
    if (blkOffset < m_queue.head()) return; // prevent inadvertent reset
    m_queue.head(blkOffset);
  }

private:
  IndexQueue	m_queue;
};

class Series : public ZdbObject<DB::Series> {
template <typename, typename> friend class Reader;
template <typename, typename> friend class Writer;

using ID = uint32_t;

private:
  Series(DB *db, unsigned shard) : m_db{db}, m_shard{shard} { pin(); }
  ~Series() { }

public:
  DB *db() const { return m_db; }
  unsigned shard() const { return m_shard; }
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

private:
  bool loadBlk(const ZuFieldTuple<DB::BlkHdr> &row) {
    auto blkHdr = m_index.set(row.p<1>());
    if (!blkHdr) return false;
    blkHdr->init(row.p<2>(), row.p<4>(), row.p<5>(), row.p<3>());
    return true;
  }

  // open series and fill index
  void open(OpenSeriesFn fn) {
    m_index.init(data().blkOffset);
    m_db->m_blkHdrTbl->selectRows<0>(
      ZuFwdTuple(data().id), IndexBlkSize(), ZuLambda{[
	this, fn = ZuMv(fn)
      ](auto &&self, auto result, unsigned) mutable {
	if (m_opened) return; // index filled, callback already called - ignore
	using Row = ZuFieldTuple<DB::BlkHdr>;
	if (result.template is<Row>()) { // fill index
	  m_openBlkOffset = result.template p<Row>().template p<1>();
	  if (!loadBlk(result.template p<Row>())) {
	    m_opened = true;
	    fn(this);
	  }
	} else { // complete
	  if (!m_openBlkOffset) {
	    m_opened = true;
	    fn(this);
	  } else {
	    auto openBlkOffset = m_openBlkOffset;
	    m_openBlkOffset = 0;
	    nextRows<0>(
	      ZuFwdTuple(data().id, openBlkOffset), false,
	      IndexBlkSize(), ZuMv(self));
	  }
	}
      }});
  }

  // number of blocks
  unsigned blkCount() const { return m_index.tail() - m_index.head(); }

  BlkHdr *lastBlk() const {
    auto blk = m_index.get(m_index.tail() - 1);
    ZeAssert(blk,
      (id = data().id, head = m_index.head(), tail = m_index.tail()),
      "id=" << id << " head=" << head << " tail=" << tail, return nullptr);
    return blk;
  }

  // value count (length of series in #values)
  uint64_t count() const {
    if (ZuUnlikely(m_index.tail() == m_index.head())) return 0;
    auto blk = lastBlk();
    ZeAssert(blk,
      (id = data().id, head = m_index.head(), tail = m_index.tail()),
      "id=" << id << " head=" << head << " tail=" << tail, return 0);
    return blk->offset() + hdr->count();
  }

  // length in bytes (compressed)
  uint64_t length() const {
    unsigned n = blkCount();
    if (ZuUnlikely(!n)) return 0;
    auto blk = lastBlk();
    ZeAssert(blk,
      (id = data().id, head = m_index.head(), tail = m_index.tail()),
      "id=" << id << " head=" << head << " tail=" << tail, return 0);
    ZeAssert(blk->blkData,
      (id = data().id, head = m_index.head(), tail = m_index.tail()),
      "id=" << id << " head=" << head << " tail=" << tail, return n * BlkSize);
    return (n - 1) * BlkSize + blk->blkData->data().buf.length();
  }

  template <typename Decoder>
  auto seek(uint64_t offset = 0) const {
    return Reader<Series, Decoder>::seek(this, offset);
  }
  template <typename Decoder>
  auto find(const ZuFixed &value) const {
    return Reader<Series, Decoder>::find(this, value);
  }

  template <typename Encoder>
  auto writer() { return Writer<Series, Encoder>{this}; }

private:
  void loadBlk(unsigned blkOffset, ZmFn<void(BlkHdr *)> fn) const {
    auto hdr = m_index.get(blkOffset);
    if (ZuUnlikely(!hdr)) return nullptr;
    if (hdr->blkData) { fn(hdr); return; }
    db()->loadBlk(id(), blkOffset, [
      fn = ZuMv(fn)
    ](ZmRef<BlkData> blkData) mutable {
      hdr->blkData = ZuMv(blkData);
      fn(hdr);
    });
  }

  void unloadBlk(unsigned blkOffset) {
    auto hdr = m_index.get(blkOffset);
    if (ZuUnlikely(!hdr)) return nullptr;
    hdr->blkData = nullptr;
  }

  template <typename Decoder>
  void seek_(unsigned search, uint64_t offset, ZmFn<void(Decoder)> fn) const {
    loadBlk(ZuSearchPos(search), [fn = ZuMv(fn)](Blk *blk) {
      if (ZuUnlikely(!blk)) goto null;
      auto reader = blk->reader<Decoder>();
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
    loadBlk(ZuSearchPos(search), [value, fn = ZuMv(fn)](Blk *blk) {
      if (ZuUnlikely(!blk)) goto null;
      auto reader = blk->reader<Decoder>();
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

  // FIXME - ZuInterSearch -> ZuSearch.hh
  // ZuSearch / ZuInterSearch -> generic array with [] operator
  // Series Blk Index -> add array operator (uses get/set)

  auto seekFn(uint64_t offset) const {
    return [offset](const Blk *blk) -> int {
      auto hdrOffset = blk->offset();
      if (offset < hdrOffset) return -int(hdrOffset - offset);
      hdrOffset += blk->count();
      if (offset >= hdrOffset) return int(offset - hdrOffset) + 1;
      return 0;
    };
  }
  template <typename Decoder>
  auto findFn(const ZuFixed &value) const {
    return [this, value](const Blk &blk) -> int {
      unsigned blkOffset = &const_cast<Blk &>(blk) - &m_blks[0];
      auto buf = loadBuf(blkOffset); // FIXME - first can be the previous buffer's last, no need to load the buffer here; meanwhile the series first can be in the series
      if (!buf) return -1;
      auto reader = buf->template reader<Decoder>();
      auto hdr = buf->hdr();
      ZuFixed value_{int64_t(0), hdr->ndp()};
      ZuFixedVal mantissa;
      if (!reader.read(mantissa)) return -1;
      mantissa = value_.adjust(value.ndp());
      if (value.mantissa() < mantissa) {
	int64_t delta = mantissa - value.mantissa();
	if (ZuUnlikely(delta >= int64_t(INT_MAX)))
	  return INT_MIN;
	return -int(delta);
      }
      value_.mantissa(hdr->last);
      mantissa = value_.adjust(value.ndp());
      if (value.mantissa() > mantissa) {
	int64_t delta = value.mantissa() - mantissa;
	if (ZuUnlikely(delta >= int64_t(INT_MAX)))
	  return INT_MAX;
	return int(delta);
      }
      return 0;
    };
  }

  template <typename Decoder>
  Decoder seek(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(&m_blks[0], m_blks.length(), seekFn(offset)),
	offset);
  }
  template <typename Decoder>
  Decoder seekFwd(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[buf->blkOffset], m_blks.length() - buf->blkOffset,
	  seekFn(offset)),
	offset);
  }
  template <typename Decoder>
  Decoder seekRev(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(&m_blks[0], buf->blkOffset + 1, seekFn(offset)),
	offset);
  }

  template <typename Decoder>
  Decoder find(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], m_blks.length(),
	  findFn<Decoder>(value)),
	value);
  }
  template <typename Decoder>
  Decoder findFwd(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[buf->blkOffset], m_blks.length() - buf->blkOffset,
	  findFn<Decoder>(value)),
	value);
  }
  template <typename Decoder>
  Decoder findRev(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], buf->blkOffset + 1,
	  findFn<Decoder>(value)),
	value);
  }

  template <typename Decoder>
  Decoder nextDecoder(ZmRef<Buf> &buf) const {
    unsigned blkOffset = buf->blkOffset + 1;
    if (blkOffset >= m_blks.length()) goto null;
    if (!(buf = loadBuf(blkOffset))) goto null;
    return buf->reader<Decoder>();
  null:
    buf = nullptr;
    return Decoder{};
  }

  template <typename Encoder>
  Encoder encoder(ZmRef<Buf> &buf) {
    return nextEncoder<Encoder>(buf);
  }
  template <typename Encoder>
  Encoder nextEncoder(ZmRef<Buf> &buf) {
    unsigned blkOffset;
    uint64_t offset;
    if (ZuLikely(buf)) {
      blkOffset = buf->blkOffset + 1;
      const auto *hdr = buf->hdr();
      offset = hdr->offset() + hdr->count();
    } else {
      blkOffset = 0;
      offset = 0;
    }
    m_store->shift(); // might call unloadBuf()
    buf = new Buf{m_store, m_seriesID, blkOffset};
    new (Blk::new_<ZmRef<Buf>>(m_blks.push())) ZmRef<Buf>{buf};
    new (buf->hdr()) Hdr{offset, 0};
    m_store->push(buf);
    {
      blkOffset = buf->blkOffset;
      const auto *hdr = buf->hdr();
      offset = hdr->offset() + hdr->count();
    }
    return buf->writer<Encoder>();
  }

  void purge_(unsigned blkOffset) {
    m_index->purge(blkOffset);
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
  DB		*m_db = nullptr;
  unsigned	m_shard = 0;
  DataFrame	*m_df = nullptr;
  Index		m_index;
  uint64_t	m_openBlkOffset = 0;
  bool		m_opened = false;
};

inline void Blk::evict()
{
  ZdbAnyObject::evict();
  m_series->evicted(this);
}

} // namespace Zdf

#endif /* ZdfSeries_HH */
