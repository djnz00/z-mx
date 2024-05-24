//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series
// * chunked into blocks
// * compressed (see ZdfCompress)
// * indexable (if monotonically increasing, e.g. time series)
// * support archiving of old data with purge()
// * in-memory or file-backed (see ZdfMem / ZdfFile)

#ifndef ZdfSeries_HH
#define ZdfSeries_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZuUnion.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/ZuSort.hh>

#include <zlib/ZmRef.hh>

#include <zlib/ZtArray.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfBuf.hh>
#include <zlib/ZdfStore.hh>

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
    const_cast<Series *>(m_series)->purge_(m_buf->blkIndex);
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

class Series {
template <typename, typename> friend class Reader;
template <typename, typename> friend class Writer;

public:
  Series() = default;
  ~Series() { final(); }

  void init(Store *store) {
    m_store = store;
    m_seriesID = store->alloc(
	BufUnloadFn{this, [](Series *this_, BufLRUNode *node) {
	  this_->unloadBuf(node);
	}});
  }
  void final() {
    if (m_store) m_store->free(m_seriesID);
    m_blks.null();
  }

  Store *store() const { return m_store; }
  unsigned seriesID() const { return m_seriesID; }

protected:
  void open_(unsigned blkOffset, OpenFn openFn) {
    m_blkOffset = blkOffset;
    Hdr hdr;
    for (unsigned i = 0; loadHdr(i + blkOffset, hdr); i++)
      new (Blk::new_<Hdr>(m_blks.push())) Hdr{hdr};
    openFn(OpenResult{});
  }

public:
  void open(ZuString parent, ZuString name, OpenFn openFn) {
    m_store->open(m_seriesID, parent, name,
	[this, openFn = ZuMv(openFn)](Store_::OpenResult result) {
	  if (result.is<Store_::OpenData>()) {
	    open_(result.p<Store_::OpenData>().blkOffset, ZuMv(openFn));
	  } else if (result.is<Event>()) {
	    openFn(OpenResult{ZuMv(result).p<Event>()});
	  }
	});
  }
  void close(CloseFn closeFn) {
    // assumes CloseFn is same type as Store_::CloseFn
    m_store->close(m_seriesID, ZuMv(closeFn));
  }

  // number of blocks
  unsigned blkCount() const { return m_blks.length(); }

  // value count (length of series in #values)
  uint64_t count() const {
    unsigned n = m_blks.length();
    if (ZuUnlikely(!n)) return 0;
    auto hdr = this->hdr(m_blks[n - 1]);
    return hdr->offset() + hdr->count();
  }

  // length in bytes (compressed)
  uint64_t length() const {
    unsigned n = m_blks.length();
    if (ZuUnlikely(!n)) return 0;
    auto hdr = this->hdr(m_blks[n - 1]);
    return (n - 1) * BufSize + hdr->length();
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
  using Blk = ZuUnion<Hdr, ZmRef<Buf>>;

  static const Hdr *hdr_(const ZuNull &) { return nullptr; } // never called
  static const Hdr *hdr_(const Hdr &hdr) { return &hdr; }
  static const Hdr *hdr_(const ZmRef<Buf> &buf) { return buf->hdr(); }
  static const Hdr *hdr(const Blk &blk) {
    return blk.cdispatch([](auto, auto &&v) { return hdr_(v); });
  }

  Buf *loadBuf(unsigned blkIndex) const {
    auto &blk = m_blks[blkIndex];
    ZmRef<Buf> buf;
    Buf *buf_;
    if (ZuLikely(blk.is<ZmRef<Buf>>())) {
      buf_ = blk.p<ZmRef<Buf>>().ptr();
      m_store->use(buf_);
    } else {
      if (ZuUnlikely(!blk.is<Hdr>())) return nullptr;
      m_store->shift(); // might call unloadBuf()
      buf = load(blkIndex + m_blkOffset);
      if (!buf) return nullptr;
      buf_ = buf.ptr();
      const_cast<Blk &>(blk).p<ZmRef<Buf>>(ZuMv(buf));
      m_store->push(buf_);
    }
    return buf_;
  }

  void unloadBuf(BufLRUNode *node) {
    auto &lruBlk = m_blks[node->blkIndex];
    if (ZuLikely(lruBlk.is<ZmRef<Buf>>())) {
      Hdr hdr = *(lruBlk.p<ZmRef<Buf>>()->hdr());
      lruBlk.p<Hdr>(hdr);
    }
  }

  template <typename Decoder>
  Decoder seek_(
      ZmRef<Buf> &buf, unsigned search, uint64_t offset) const {
    unsigned blkIndex = ZuSearchPos(search);
    if (blkIndex >= m_blks.length()) goto null;
    if (!(buf = loadBuf(blkIndex))) goto null;
    {
      auto reader = buf->reader<Decoder>();
      auto offset_ = buf->hdr()->offset();
      if (offset_ >= offset) return reader;
      if (!reader.seek(offset - offset_)) goto null;
      return reader;
    }
  null:
    buf = nullptr;
    return Decoder{};
  }
  template <typename Decoder>
  Decoder find_(
      ZmRef<Buf> &buf, unsigned search, const ZuFixed &value_) const {
    unsigned blkIndex = ZuSearchPos(search);
    if (blkIndex >= m_blks.length()) goto null;
    if (!(buf = loadBuf(blkIndex))) goto null;
    {
      auto reader = buf->reader<Decoder>();
      bool found = reader.search(
	  [mantissa = value_.adjust(buf->hdr()->ndp())](
	    int64_t skip, unsigned count) -> unsigned {
	      return skip < mantissa ? count : 0;
	    });
      if (!found) goto null;
      return reader;
    }
  null:
    buf = nullptr;
    return Decoder{};
  }

  auto seekFn(uint64_t offset) const {
    return [offset](const Blk &blk) -> int {
      auto hdr = Series::hdr(blk);
      auto hdrOffset = hdr->offset();
      if (offset < hdrOffset) return -static_cast<int>(hdrOffset - offset);
      hdrOffset += hdr->count();
      if (offset >= hdrOffset) return static_cast<int>(offset - hdrOffset) + 1;
      return 0;
    };
  }
  template <typename Decoder>
  auto findFn(const ZuFixed &value) const {
    return [this, value](const Blk &blk) -> int {
      unsigned blkIndex = &const_cast<Blk &>(blk) - &m_blks[0];
      auto buf = loadBuf(blkIndex);
      if (!buf) return -1;
      auto reader = buf->template reader<Decoder>();
      auto hdr = buf->hdr();
      ZuFixed value_{static_cast<int64_t>(0), hdr->ndp()};
      ZuFixedVal mantissa;
      if (!reader.read(mantissa)) return -1;
      mantissa = value_.adjust(value.ndp());
      if (value.mantissa() < mantissa) {
	int64_t delta = mantissa - value.mantissa();
	if (ZuUnlikely(delta >= static_cast<int64_t>(INT_MAX)))
	  return INT_MIN;
	return -static_cast<int>(delta);
      }
      value_.mantissa(hdr->last);
      mantissa = value_.adjust(value.ndp());
      if (value.mantissa() > mantissa) {
	int64_t delta = value.mantissa() - mantissa;
	if (ZuUnlikely(delta >= static_cast<int64_t>(INT_MAX)))
	  return INT_MAX;
	return static_cast<int>(delta);
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
	  &m_blks[buf->blkIndex], m_blks.length() - buf->blkIndex,
	  seekFn(offset)),
	offset);
  }
  template <typename Decoder>
  Decoder seekRev(ZmRef<Buf> &buf, uint64_t offset) const {
    return seek_<Decoder>(buf,
	ZuInterSearch(&m_blks[0], buf->blkIndex + 1, seekFn(offset)),
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
	  &m_blks[buf->blkIndex], m_blks.length() - buf->blkIndex,
	  findFn<Decoder>(value)),
	value);
  }
  template <typename Decoder>
  Decoder findRev(ZmRef<Buf> &buf, const ZuFixed &value) const {
    return find_<Decoder>(buf,
	ZuInterSearch(
	  &m_blks[0], buf->blkIndex + 1,
	  findFn<Decoder>(value)),
	value);
  }

  template <typename Decoder>
  Decoder nextDecoder(ZmRef<Buf> &buf) const {
    unsigned blkIndex = buf->blkIndex + 1;
    if (blkIndex >= m_blks.length()) goto null;
    if (!(buf = loadBuf(blkIndex))) goto null;
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
    unsigned blkIndex;
    uint64_t offset;
    if (ZuLikely(buf)) {
      blkIndex = buf->blkIndex + 1;
      const auto *hdr = buf->hdr();
      offset = hdr->offset() + hdr->count();
    } else {
      blkIndex = 0;
      offset = 0;
    }
    m_store->shift(); // might call unloadBuf()
    buf = new Buf{m_store, m_seriesID, blkIndex};
    new (Blk::new_<ZmRef<Buf>>(m_blks.push())) ZmRef<Buf>{buf};
    new (buf->hdr()) Hdr{offset, 0};
    m_store->push(buf);
    {
      blkIndex = buf->blkIndex;
      const auto *hdr = buf->hdr();
      offset = hdr->offset() + hdr->count();
    }
    return buf->writer<Encoder>();
  }

  void purge_(unsigned blkIndex) {
    m_store->purge(m_seriesID, m_blkOffset += blkIndex);
    {
      unsigned n = m_blks.length();
      if (n > blkIndex) n = blkIndex;
      for (unsigned i = 0; i < n; i++) {
	auto &blk = m_blks[i];
	if (blk.is<ZmRef<Buf>>())
	  m_store->del(blk.p<ZmRef<Buf>>().ptr());
      }
    }
    m_blks.splice(0, blkIndex);
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
  Store		*m_store = nullptr;
  ZtArray<Blk>	m_blks;
  unsigned	m_seriesID = 0;
  unsigned	m_blkOffset = 0;
};

} // namespace Zdf

#endif /* ZdfSeries_HH */
