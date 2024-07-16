//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z database

// Zdb is a clustered/replicated in-process/in-memory DB that includes
// RAFT-like leader election and failover. Zdb dynamically organizes
// cluster hosts into a replication chain from the leader to the
// lowest-priority follower. Replication is async. ZmEngine is used for
// start/stop state management. Zdb applications are stateful back-end
// services that defer to Zdb for activation/deactivation.
// Restart/recovery is from backing data store, then from the cluster
// leader (if the local host itself is not elected leader).

// Principal features:
// * Plug-in backing data store (mocked for unit-testing)
//   - Currently Postgres
// * In-memory write-through object cache
//   - Deferred async writes
//   - In-memory write buffer queue
// * Async replication independent of backing store
//   (can be disabled for replicated backing stores)
// * Primary and multiple secondary unique in-memory and on-disk indices
// * Find, insert, update, delete operations
// * Batched select queries (index-based, optionally grouped)

// select() returns 0..N immutable ZuTuples for read-only purposes
// find() returns 0..1 mutable ZdbObjects for read-modify-write purposes

//  host state		engine state
//  ==========		============
//  Instantiated	Stopped
//  Initialized		Stopped
//  Electing		!Stopped
//  Active		!Stopped
//  Inactive		!Stopped
//  Stopping		Stopping | StartPending

#ifndef Zdb_HH
#define Zdb_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuInt.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmSpecific.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmEngine.hh>
#include <zlib/ZmPolyCache.hh>
#include <zlib/ZmPLock.hh>

#include <zlib/ZtString.hh>
#include <zlib/ZtEnum.hh>
#include <zlib/ZtHexDump.hh>

#include <zlib/ZePlatform.hh>
#include <zlib/ZeLog.hh>

#include <zlib/ZiFile.hh>
#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiIOBuf.hh>
#include <zlib/ZiRx.hh>
#include <zlib/ZiTx.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/ZvCf.hh>

#include <zlib/ZdbTypes.hh>
#include <zlib/ZdbBuf.hh>
#include <zlib/ZdbMsg.hh>
#include <zlib/ZdbStore.hh>

#if defined(ZDEBUG) && !defined(Zdb_DEBUG)
#define Zdb_DEBUG 1
#endif

#if Zdb_DEBUG
#define ZdbDEBUG(db, e) \
  do { if ((db)->debug()) ZeLOG(Debug, (e)); } while (0)
#else
#define ZdbDEBUG(db, e) (void())
#endif

#include <zlib/zdb_telemetry_fbs.h>

// type-specific buffer size - specialize to override default
template <typename>
struct ZdbBufSize : public ZuUnsigned<Zdb_::DefltBufSize> { };

namespace Zdb_ {

// --- DLQ block sizes

enum { FindDLQ_BlkSize = 128 };
enum { StoreDLQ_BlkSize = 128 };

// --- pre-declarations

class DB;				// database
class Host;				// cluster host
class AnyTable;				// untyped table
template <typename T> class Table;	// typed table
struct Record_Print;

// --- replication connection

class Cxn_ :
    public ZiConnection,
    public ZiRx<Cxn_, RxBufAlloc>,
    public ZiTx<Cxn_> {
friend DB;
friend Host;
friend AnyTable;

  using Buf = Zdb_::RxBufAlloc; // de-conflict with ZiConnection

  using Rx = ZiRx<Cxn_, Buf>;
  using Tx = ZiTx<Cxn_>;

  using Rx::recv; // de-conflict with ZiConnection
  using Tx::send; // ''

protected:
  Cxn_(DB *db, Host *host, const ZiCxnInfo &ci);

private:
  DB *db() const { return m_db; }
  void host(Host *host) { m_host = host; }
  Host *host() const { return m_host; }

  void connected(ZiIOContext &);
  void disconnected();

  void msgRead(ZiIOContext &);
  int msgRead2(ZmRef<IOBuf>);
  void msgRead3(ZmRef<IOBuf>);

  void hbRcvd(const fbs::Heartbeat *);
  void hbTimeout();
  void hbSend();

  void repRecordRcvd(ZmRef<const IOBuf>);
  void repCommitRcvd(ZmRef<const IOBuf>);

  DB			*m_db;
  Host			*m_host;	// nullptr if not yet associated

  ZmScheduler::Timer	m_hbTimer;
};
inline constexpr const char *CxnHeapID() { return "Zdb.Cxn"; }
using CxnList =
  ZmList<Cxn_,
    ZmListNode<Cxn_,
      ZmListHeapID<CxnHeapID>>>;
using Cxn = CxnList::Node;

// --- DB state - SN and key/value linear hash from table ID -> UN

using DBState_ = ZmLHashKV<ZuID, UN, ZmLHashLocal<>>;
struct DBState : public DBState_ {
  SN		sn = 0;

  DBState() = delete;

  DBState(unsigned size) : DBState_{ZmHashParams{size}} { }

  DBState(const fbs::DBState *dbState) :
      DBState_{ZmHashParams{dbState->tableStates()->size()}},
      sn{Zfb::Load::uint128(dbState->sn())} {
    using namespace Zdb_;
    using namespace Zfb::Load;

    all(dbState->tableStates(),
	[this](unsigned, const fbs::TableState *tableState) {
	  add(id(&(tableState->table())), tableState->un());
	});
  }
  void load(const fbs::DBState *dbState) {
    using namespace Zdb_;
    using namespace Zfb::Load;

    sn = uint128(dbState->sn());
    all(dbState->tableStates(),
	[this](unsigned, const fbs::TableState *tableState) {
	  update(id(&(tableState->table())), tableState->un());
	});
  }
  Zfb::Offset<fbs::DBState> save(Zfb::Builder &fbb) const {
    using namespace Zdb_;
    using namespace Zfb::Save;

    auto sn_ = uint128(sn);
    auto i = readIterator();
    return fbs::CreateDBState(fbb, &sn_, structVecIter<fbs::TableState>(
	fbb, i.count(), [&i](fbs::TableState *ptr, unsigned) {
      if (auto state = i.iterate())
	new (ptr)
	  fbs::TableState{id(state->template p<0>()), state->template p<1>()};
      else
	new (ptr) fbs::TableState{}; // unused
    }));
  }

  void reset() {
    sn = 0;
    clean();
  }

  bool updateSN(SN sn_) {
    if (sn < sn_) {
      sn = sn_;
      return true;
    }
    return false;
  }
  bool update(ZuID id, UN un_) {
    auto state = find(id);
    if (!state) {
      add(id, un_);
      return true;
    }
    auto &un = const_cast<T *>(state)->template p<1>();
    if (un < un_) {
      un = un_;
      return true;
    }
    return false;
  }
  DBState &operator |=(const DBState &r) {
    if (ZuLikely(this != &r)) {
      updateSN(r.sn);
      auto i = r.readIterator();
      while (auto rstate = i.iterate())
	update(rstate->template p<0>(), rstate->template p<1>());
    }
    return *this;
  }
  DBState &operator =(const DBState &r) {
    if (ZuLikely(this != &r)) {
      clean();
      this->operator |=(r);
    }
    return *this;
  }

  int cmp(const DBState &r) const {
    return (sn > r.sn) - (sn < r.sn);
  }

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(DBState *);
};

// --- replication message printing

namespace HostState {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(DBHostState,
    Instantiated,
    Initialized,
    Electing,
    Active,
    Inactive,
    Stopping)
}

// --- generic object

namespace ObjState {
  ZtEnumValues(ObjState, int8_t,
    Undefined = 0,
    Insert,
    Update,
    Committed,
    Delete,
    Deleted);
}

const char *Object_HeapID() { return "Zdb.Object"; }

// possible object state paths:
//
// Undefined > Insert			insert
// Insert > Committed			insert committed
// Insert > Undefined			insert aborted
// Committed > Update > Committed	update committed or aborted
// Committed > Delete > Deleted		delete committed
// Committed > Delete > Committed	delete aborted
//
// path forks:
//
// Insert > (Committed|Undefined)
// Delete > (Deleted|Committed)
//
// possible event sequences:
//
// insert, commit
// insert, abort
// update, commit
// update, abort
// del, commit
// del, abort
//
// events and state transitions:
//
// insert	Undefined > Insert
// commit	Insert > Committed
// abort	Insert > Undefined
// update	Committed > Update
// commit	Update > Committed
// abort	Update > Committed
// del		Committed > Delete
// commit	Delete > Deleted
// abort	Delete > Committed

class ZdbAPI AnyObject : public ZmPolymorph {
  AnyObject() = delete;
  AnyObject(const AnyObject &) = delete;
  AnyObject &operator =(const AnyObject &) = delete;
  AnyObject(AnyObject &&) = delete;
  AnyObject &operator =(AnyObject &&) = delete;

  friend AnyTable;
  template <typename> friend class Table;

public:
  AnyObject(AnyTable *table) : m_table{table} { }

  AnyTable *table() const { return m_table; }
  UN un() const { return m_un; }
  SN sn() const { return m_sn; }
  VN vn() const { return m_vn; }
  int state() const { return m_state; }
  UN origUN() const { return m_origUN; }
  bool evicted() const { return m_evicted; }

  ZmRef<const IOBuf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<AnyObject *>(this)->ptr_(); }

  template <typename S> void print(S &s) const;
  friend ZuPrintFn ZuPrintType(AnyObject *);

private:
  void init(UN un, SN sn, VN vn) {
    m_un = un;
    m_sn = sn;
    m_vn = vn;
    m_state = ObjState::Committed;
  }

  bool insert_(UN un);
  bool update_(UN un);
  bool del_(UN un);
  bool commit_();
  bool abort_();

  void evicted(bool v) { m_evicted = v; }

  AnyTable	*m_table;
  UN		m_un = nullUN();
  SN		m_sn = nullSN();
  VN		m_vn = 0;
  int		m_state = ObjState::Undefined;
  UN		m_origUN = nullUN();
  bool		m_evicted = false;
};

inline UN AnyObject_UNAxor(const ZmRef<AnyObject> &object) {
  return object->un();
}
// temporarily there may be more than one UN referencing a cached object
using CacheUN =
  ZmHashKV<UN, ZmRef<AnyObject>,
    ZmHashLock<ZmPLock,
      ZmHashHeapID<Object_HeapID>>>;

// --- typed object

// Zdf data-frames are comprised of series fields that do not form part of
// a primary or secondary key for the object - Zdb skips Zdf fields
// and does not persist them
template <typename Field>
struct FieldFilter :
    public ZuBool<!ZuTypeIn<ZuFieldProp::Series, typename Field::Props>{}> { };

template <typename T>
using FieldList = ZuTypeGrep<FieldFilter, ZuFieldList<T>>;

template <typename T> class Table;

template <typename T_>
class Object_ : public AnyObject {
  Object_() = delete;
  Object_(const Object_ &) = delete;
  Object_ &operator =(const Object_ &) = delete;
  Object_(Object_ &&) = delete;
  Object_ &operator =(Object_ &&) = delete;

public:
  using T = T_;

  Object_(Table<T> *table_) : AnyObject{table_} { }

  template <typename L>
  Object_(Table<T> *table_, L l) : AnyObject{table_} {
    l(static_cast<void *>(&m_data[0]));
  }

  Table<T> *table() const {
    return static_cast<Table<T> *>(AnyObject::table());
  }

  void *ptr_() { return &m_data[0]; }
  const void *ptr_() const { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~Object_() { ptr()->~T(); }

  const T &data() const & { return *ptr(); }
  T &data() & { return *ptr(); }
  T &&data() && { return ZuMv(*ptr()); }

  void commit();
  bool abort();

  // transform original fields, overriding get/set
  using Fields_ = FieldList<T>;
  template <typename Base>
  struct Adapted : public Base {
    using Orig = Base;
    template <template <typename> typename Override>
    using Adapt = Adapted<Override<Orig>>;
    using O = Object_;
    // using decltype(auto) here creates a circular dependency
    static decltype(Orig::get(ZuDeclVal<const T &>())) get(const O &o) {
      return Orig::get(o.data());
    }
    static decltype(Orig::get(ZuDeclVal<T &>())) get(O &o) {
      return Orig::get(o.data());
    }
    static decltype(Orig::get(ZuDeclVal<T &&>())) get(O &&o) {
      return Orig::get(ZuMv(o).data());
    }
    template <typename U> static void set(O &o, U &&v) {
      return Orig::set(o.data(), ZuFwd<U>(v));
    }
    // remove any Ctor property
  private:
    template <typename>
    struct CtorFilter : public ZuTrue { };
    template <unsigned J>
    struct CtorFilter<ZuFieldProp::Ctor<J>> : public ZuFalse { };
  public:
    using Props = ZuTypeGrep<CtorFilter, typename Orig::Props>;
  };
  template <typename Field>
  using Map = typename Field::template Adapt<Adapted>;
  using Fields = ZuTypeMap<Map, Fields_>;
  // bind Fields
  friend Object_ ZuFielded_(Object_ *);
  friend Fields ZuFieldList_(Object_ *);

private:
  uint8_t	m_data[sizeof(T)];
}
#ifdef __GNUC__
  __attribute__ ((aligned(alignof(T_))))
#endif
;

// typed object cache
template <typename T>
using Cache = ZmPolyCache<Object_<T>, ZmPolyCacheHeapID<Object_HeapID>>;

// typed object
template <typename T>
struct Object : public Cache<T>::Node {
  using Base = Cache<T>::Node;
  using Base::Base;
  using Base::operator =;
  using Object_<T>::data;
};

// --- table configuration

namespace CacheMode {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(DBCacheMode,
    Normal,
    All)
}

struct TableCf {
  ZuID			id;
  ZmThreadName		thread;		// in-memory thread
  mutable unsigned	sid = 0;	// in-memory thread slot ID
  int			cacheMode = CacheMode::Normal;
  bool			warmup = false;	// warm-up caches, backing store

  TableCf() = default;
  TableCf(ZuString id_) : id{id_} { }
  TableCf(ZuString id_, const ZvCf *cf) : id{id_} {
    thread = cf->get("thread");
    cacheMode = cf->getEnum<CacheMode::Map>(
	"cacheMode", CacheMode::Normal);
    warmup = cf->getBool("warmup");
  }

  static ZuID IDAxor(const TableCf &cf) { return cf.id; }
};

// --- table configuration

inline constexpr const char *TableCfs_HeapID() { return "Zdb.TableCfs"; }
using TableCfs =
  ZmRBTree<TableCf,
    ZmRBTreeKey<TableCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<TableCfs_HeapID>>>>;

// --- generic table

class ZdbAPI AnyTable : public ZmPolymorph {
friend DB;
friend Cxn_;
friend AnyObject;
friend Record_Print;	// uses objPrintFB

  using StoreDLQ = ZmXRing<ZmRef<const IOBuf>>;

protected:
  AnyTable(DB *db, TableCf *cf);

public:
  ~AnyTable();

private:
  template <typename L> void open(L l);		// l(OpenResult)
  bool opened(OpenResult);
  template <typename L> void close(L l);	// l()

public:
  DB *db() const { return m_db; }
  ZiMultiplex *mx() const { return m_mx; }
  const TableCf &config() const { return *m_cf; }

  static ZuID IDAxor(AnyTable *table) { return table->config().id; }

  ZuID id() const { return config().id; }

  // DB thread (may be shared)
  template <typename ...Args>
  void run(Args &&...args) const {
    m_mx->run(m_cf->sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&...args) const {
    m_mx->invoke(m_cf->sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_cf->sid); }

  // record count - SWMR
  uint64_t count() const { return m_count.load_(); }

  // buffer allocator
  virtual ZmRef<IOBuf> allocBuf() = 0;

private:
  IOBuf *findBufUN(UN un) {
    return static_cast<IOBuf *>(m_bufCacheUN->find(un));
  }
protected:
  void cacheBufUN(IOBuf *buf) { m_bufCacheUN->addNode(buf); }
  auto evictBufUN(UN un) { return m_bufCacheUN->del(un); }

public:
  // next UN that will be allocated
  UN nextUN() const { return m_nextUN; }

  // enable/disable writing to cache (temporarily)
  void writeCache(bool enabled) { m_writeCache = enabled; }

  // all transactions begin with a insert(), update() or del(),
  // and complete with object->commit() or object->abort()

protected:
  // -- implemented by Table<T>

  // warmup
  virtual void warmup() = 0;

  // objSave(fbb, ptr) - save object into flatbuffer, return offset
  virtual Zfb::Offset<void> objSave(Zfb::Builder &, const void *) const = 0;
  virtual Zfb::Offset<void> objSaveUpd(Zfb::Builder &, const void *) const = 0;
  virtual Zfb::Offset<void> objSaveDel(Zfb::Builder &, const void *) const = 0;
  // objRecover(record) - process recovered FB record (untrusted source)
  virtual void objRecover(const fbs::Record *) = 0;

  // objFields() - run-time field array
  virtual ZtMFields objFields() const = 0;
  // objKeyFields() - run-time key field arrays
  virtual ZtMKeyFields objKeyFields() const = 0;
  // objSchema() - flatbuffer reflection schema
  virtual const reflection::Schema *objSchema() const = 0;

  // objPrint(stream, ptr) - print object
  virtual void objPrint(ZuMStream &, const void *) const = 0;
  // objPrintFB(stream, data) - print flatbuffer
  virtual void objPrintFB(ZuMStream &, ZuBytes) const = 0;

  // buffer cache
  virtual void cacheBuf_(ZmRef<const IOBuf>) = 0;
  virtual ZmRef<const IOBuf> evictBuf_(IOBuf *) = 0;

  // cache statistics
  virtual void cacheStats(ZmCacheStats &stats) const = 0;

  ZuTuple<Zfb::IOBuilder, Zfb::Offset<void>>
  telemetry(bool update) const;

protected:
  bool writeCache() const { return m_writeCache; }

  auto findUN(UN un) const { return m_cacheUN->findVal(un); }
  void cacheUN(UN un, AnyObject *object) { m_cacheUN->add(un, object); }
  void evictUN(UN un) { m_cacheUN->del(un); }

  StoreTbl *storeTbl() const { return m_storeTbl; }

protected:
  // cache replication buffer
  void cacheBuf(ZmRef<const IOBuf>);
  // evict replication buffer
  void evictBuf(UN un);

  // outbound replication / write to backing data store
  void write(ZmRef<const IOBuf> buf);

  // maintain record count
  void incCount() { ++m_count; }
  void decCount() { --m_count; }

private:
  // low-level write to backing data store - write thread
  void store(ZmRef<const IOBuf> buf);
  void retryStore_();			// retry on timer
  void store_(ZmRef<const IOBuf> buf);

  // outbound recovery / replication
  void recSend(ZmRef<Cxn> cxn, UN un, UN endUN);
  void recSend_(ZmRef<Cxn> cxn, UN un, UN endUN, ZmRef<const IOBuf> buf);
  void recNext(ZmRef<Cxn> cxn, UN un, UN endUN);
  ZmRef<const IOBuf> mkBuf(UN un);
  void commitSend(UN un);

  // inbound replication
  void repRecordRcvd(ZmRef<const IOBuf> buf);
  void repCommitRcvd(UN un);

  // recovery - DB thread
  void recover(const fbs::Record *record);

  // UN
  bool allocUN(UN un) {
    if (ZuUnlikely(un != m_nextUN)) return false;
    ++m_nextUN;
    return true;
  }
  void recoveredUN(UN un) {
    if (ZuUnlikely(un == nullUN())) return;
    if (m_nextUN <= un) m_nextUN = un + 1;
  }

  // immutable
  DB			*m_db;
  const TableCf		*m_cf;
  ZiMultiplex		*m_mx;

  // DB thread exclusive - no need for atomics
  UN			m_nextUN = 0;		// UN allocator

  // open/closed state, record count
  ZmAtomic<unsigned>	m_open = 0;		// DB thread SWMR
  ZmAtomic<uint64_t>	m_count = 0;		// ''

  // backing data store table
  StoreTbl		*m_storeTbl = nullptr;	// DB thread
  StoreDLQ		m_storeDLQ;		// store() dead letter queue

  // object cache indexed by UN
  bool			m_writeCache = true;
  ZmRef<CacheUN>	m_cacheUN;

  // buffer cache indexed by UN
  ZmRef<BufCacheUN>	m_bufCacheUN;
};

// typed I/O buffer base
template <typename T_>
struct Buf_ : public ZmPolymorph {
  ZmRef<const IOBuf>	buf;
  bool			stale = false;

  Buf_(ZmRef<const IOBuf> buf_) : buf{ZuMv(buf_)} { buf->typed = this; }

  using T = T_;
  using FB = ZfbType<T>;
  const FB *fbo() const {
    auto record = record_(msg_(buf->hdr()));
    auto data = Zfb::Load::bytes(record->data());
    return ZfbField::verify<T>(data);
  }
  const FB *fbo_() const {
    auto record = record_(msg_(buf->hdr()));
    auto data = Zfb::Load::bytes(record->data());
    return ZfbField::root<T>(&data[0]);
  }

  // transform original fields, overriding get/set
  using Fields_ = FieldList<FB>;
  template <typename Base>
  struct Adapted : public Base {
    using Orig = Base;
    template <template <typename> typename Override>
    using Adapt = Adapted<Override<Orig>>;
    using O = Buf_;
    enum { ReadOnly = true };
    // using decltype(auto) here creates a circular dependency
    static decltype(Orig::get(ZuDeclVal<const FB &>())) get(const O &o) {
      return Orig::get(*(o.fbo_()));
    }
    template <typename U> static void set(O &, U &&);
  };
  template <typename Field>
  using Map = typename Field::template Adapt<Adapted>;
  using Fields = ZuTypeMap<Map, Fields_>;
  // bind Fields
  friend Buf_ ZuFielded_(Buf_ *);
  friend Fields ZuFieldList_(Buf_ *);

  // override printing
  friend ZtFieldPrint ZuPrintType(Buf_ *);
};

// buffer cache
template <typename T>
using BufCache = ZmPolyHash<Buf_<T>, ZmPolyHashHeapID<Buf_HeapID>>;

// typed buffer
template <typename T>
struct Buf : public BufCache<T>::Node {
  using Base = BufCache<T>::Node;
  using Base::Base;
};

// backing data store count() context
struct Count__ {
  using Result = ZuUnion<void, uint64_t>;

  ZmFn<void(Result)>	fn;
};
inline constexpr const char *Count_HeapID() { return "Zdb.Count"; }
template <typename Heap>
struct Count_ : public Heap, public ZmPolymorph, public Count__ {
  using Base = Count__;
  using Base::Base;
  template <typename ...Args>
  Count_(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};
using Count_Heap = ZmHeap<Count_HeapID, sizeof(Count_<ZuNull>)>;
using Count = Count_<Count_Heap>;

// backing data store select() context
template <typename Tuple> struct Select__ {
  using Result = ZuUnion<void, Tuple>;

  ZmFn<void(Result, unsigned)>	fn;
};
inline constexpr const char *Select_HeapID() { return "Zdb.Select"; }
template <typename Tuple, typename Heap>
struct Select_ : public Heap, public ZmPolymorph, public Select__<Tuple> {
  using Base = Select__<Tuple>;
  using Base::Base;
  template <typename ...Args>
  Select_(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};
template <typename Tuple>
using Select_Heap = ZmHeap<Select_HeapID, sizeof(Select_<Tuple, ZuNull>)>;
template <typename Tuple>
using Select = Select_<Tuple, Select_Heap<Tuple>>;

// backing data store find() context (retried on failure)
template <typename T, typename Key> struct Find__ {
  Table<T>			*table;
  Key				key;
  ZmFn<void(ZmRef<Object<T>>)>	fn;
};
inline constexpr const char *Find_HeapID() { return "Zdb.Find"; }
template <typename T, typename Key, typename Heap>
struct Find_ : public Heap, public ZmPolymorph, public Find__<T, Key> {
  using Base = Find__<T, Key>;
  using Base::Base;
  template <typename ...Args>
  Find_(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};
template <typename T, typename Key>
using Find_Heap = ZmHeap<Find_HeapID, sizeof(Find_<T, Key, ZuNull>)>;
template <typename T, typename Key>
using Find = Find_<T, Key, Find_Heap<T, Key>>;

// split group keys into group part and grouped part
template <typename O, unsigned KeyID>
struct SplitKey {
  using Key = ZuFieldKeyT<O, KeyID>;
  using KeyFields = ZuFieldList<Key>;
  // - filter fields that are part of a group
  template <typename Field>
  using IsGroup = ZuFieldProp::IsGroup<typename Field::Props, KeyID>;
  // - filter fields that are not part of a group
  template <typename Field>
  using NotGroup = ZuBool<!IsGroup<Field>{}>;
  // - extract group fields from fields comprising a key
  using GroupFields = ZuTypeGrep<IsGroup, KeyFields>;
  // - tuple type for group fields
  using GroupKey = ZuFieldTupleT<Key, ZuMkCRef, ZuDecay, GroupFields>;
  // - extract member fields from fields comprising a key
  using MemberFields = ZuTypeGrep<NotGroup, KeyFields>;
  // - tuple type for group fields
  using MemberKey = ZuFieldTupleT<Key, ZuMkCRef, ZuDecay, MemberFields>;
  // - filter keys that have 1 or more group fields
  using IsGroupKey = ZuBool<GroupFields::N>;
};

// --- typed table

template <typename T>
class Table : public AnyTable {
friend DB;
friend Cxn_;
friend Object_<T>;

public:
  enum { BufSize = ZdbBufSize<T>{} };

  using Fields = FieldList<T>;
  using Keys = ZuFieldKeys<T>;
  using KeyIDs = ZuFieldKeyIDs<T>;
  template <int KeyID> using Key = ZuFieldKeyT<T, KeyID>;
  using Tuple = ZuFieldTuple<T>; // same as ZuFieldKeyT<T, ZuFieldKeyID::All>

  ZuAssert(Fields::N < maxFields());
  ZuAssert(KeyIDs::N < maxKeys());

private:
  // need one find DLQ for each Key
  template <typename Key>
  using FindDLQ = ZmXRing<ZmRef<Find<T, Key>>>;
  template <typename Key>
  using FindDLQPtr = ZuPtr<FindDLQ<Key>>;
  using FindDLQs = ZuTypeApply<ZuTuple, ZuTypeMap<FindDLQPtr, ZuFieldKeys<T>>>;

  // - grouping key for a KeyID
  template <unsigned KeyID>
  using GroupKey = typename SplitKey<T, KeyID>::GroupKey;
  // - grouped key for a KeyID
  template <unsigned KeyID>
  using MemberKey = typename SplitKey<T, KeyID>::MemberKey;

public:
  Table(DB *db, TableCf *cf) : AnyTable{db, cf} {
    ZuUnroll::all<KeyIDs>([this](auto KeyID) {
      m_findDLQs.template p<ZuTypeIndex<Key<KeyID>, Keys>{}>(
	new FindDLQ<Key<KeyID>>{
	  ZmXRingParams{}.initial(FindDLQ_BlkSize).increment(FindDLQ_BlkSize)});
    });
  }
  ~Table() = default;

  // buffer allocator
  ZmRef<IOBuf> allocBuf() { return new IOBufAlloc<BufSize>{}; }

private:
  // objLoad(fbo) - construct object from flatbuffer (trusted source)
  ZmRef<Object<T>> objLoad(const IOBuf *buf) {
    using namespace Zfb::Load;
    auto record = record_(msg_(buf->hdr()));
    if (record->vn() < 0) return {}; // deleted
    auto data = bytes(record->data());
    if (ZuUnlikely(!data)) return {}; // should never happen
    auto fbo = ZfbField::root<T>(&data[0]);
    if (ZuUnlikely(!fbo)) return {}; // should never happen
    ZmRef<Object<T>> object = new Object<T>{this, [fbo](void *ptr) {
      ZfbField::ctor<T>(ptr, fbo);
    }};
    object->init(record->un(), uint128(record->sn()), record->vn());
    return object;
  }
  // objSave(fbb, ptr) - save object into flatbuffer, return offset
  Zfb::Offset<void> objSave(Zfb::Builder &fbb, const void *ptr) const {
    return ZfbField::save<T>(fbb, *static_cast<const T *>(ptr)).Union();
  }
  Zfb::Offset<void> objSaveUpd(Zfb::Builder &fbb, const void *ptr) const {
    return ZfbField::saveUpd<T>(fbb, *static_cast<const T *>(ptr)).Union();
  }
  Zfb::Offset<void> objSaveDel(Zfb::Builder &fbb, const void *ptr) const {
    return ZfbField::saveDel<T>(fbb, *static_cast<const T *>(ptr)).Union();
  }
  // objRecover(record) - process recovered record (untrusted source)
  void objRecover(const fbs::Record *record) {
    auto fbo = ZfbField::verify<T>(Zfb::Load::bytes(record->data()));
    if (!fbo) return;
    // mark outdated buffers as stale
    ZuUnroll::all<KeyIDs>([this, fbo](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(*fbo);
      auto i = m_bufCache.template iterator<KeyID>(ZuMv(key));
      while (auto typedBuf = i.iterate()) {
	if (!typedBuf->stale) {
	  typedBuf->stale = true;
	  break;
	}
      }
    });
    // maintain cache consistency
    if (record->vn() > 0) {
      // primary key is immutable
      if constexpr (KeyIDs::N > 1)
	if (ZmRef<Object<T>> object = m_cache.find(ZuFieldKey<0>(*fbo))) {
	  m_cache.template update<ZuTypeTail<1, KeyIDs>>(ZuMv(object),
	    [fbo](const ZmRef<Object<T>> &object) {
	      ZfbField::update(object->data(), fbo);
	    });
	}
    } else {
      m_cache.template del<0>(ZuFieldKey<0>(*fbo));
    }
  }

  // objFields() - run-time field array
  ZtMFields objFields() const { return ZtMFieldList<T>(); }
  // objKeyFields() - run-time key field arrays
  ZtMKeyFields objKeyFields() const { return ZtMKeyFieldList<T>(); }
  // objSchema() - flatbuffer reflection schema
  const reflection::Schema *objSchema() const {
    return reflection::GetSchema(ZfbSchema<T>::data());
  }

  // objPrint(stream, ptr) - print object
  void objPrint(ZuMStream &s, const void *ptr) const {
    ZtFieldPrint::print(s, *static_cast<const T *>(ptr));
  }
  // objPrintFB(stream, data) - print flatbuffer
  void objPrintFB(ZuMStream &s, ZuBytes data) const {
    auto fbo = ZfbField::verify<T>(data);
    if (!fbo) return;
    s << *fbo;
  }

  // find buffer in buffer cache
  template <unsigned KeyID>
  ZuTuple<ZmRef<const IOBuf>, bool> findBuf(const Key<KeyID> &key) const {
    auto i = m_bufCache.template iterator<KeyID>(key);
    bool found = false;
    while (auto typedBuf = i.iterate()) {
      if (!typedBuf->stale) return {typedBuf->buf, true};
      found = true;
    }
    return {ZmRef<const IOBuf>{}, found};
  }

  // find, falling through object cache, buffer cache, backing data store
  template <unsigned KeyID, bool UpdateLRU, bool Evict, typename L>
  void find_(Key<KeyID>, L l);
  // find from backing data store (retried on failure)
  template <unsigned KeyID> void retrieve(Key<KeyID>, ZmFn<void(ZmRef<Object<T>>)>);
  template <unsigned KeyID> void retryRetrieve_();
  template <unsigned KeyID> void retrieve_(ZmRef<Find<T, Key<KeyID>>> context);

  // buffer cache
  void cacheBuf_(ZmRef<const IOBuf> buf) {
    m_bufCache.add(new Buf<T>{ZuMv(buf)});
  }
  ZmRef<const IOBuf> evictBuf_(IOBuf *buf) {
    if (auto typedBuf = m_bufCache.delNode(
	static_cast<Buf<T> *>(static_cast<Buf_<T> *>(buf->typed))))
      return ZuMv(typedBuf->buf);
    return nullptr;
  }

  // cache statistics
  void cacheStats(ZmCacheStats &stats) const { m_cache.stats(stats); }

  // ameliorate cold start
  void warmup() {
    // warmup heaps
    ZmRef<Object<T>> object = placeholder();
    object->init(0, 0, 0);
    // warmup cache
    m_cache.add(object); m_cache.delNode(object);
    // warmup UN cache
    cacheUN(0, object); evictUN(0);
    ZmRef<const IOBuf> buf = object->replicate(int(fbs::Body::Replication));
    // warmup buffer cache
    cacheBuf(buf); evictBuf(0);
    // warmup backing data store
    storeTbl()->warmup();
    // FIXME - if fully cached, perform table scan and load cache
  }

public:
  // create placeholder record
  // - null UN/SN, in-memory, never persisted/replicated
  ZmRef<Object<T>> placeholder() {
    return new Object<T>{this, [](void *ptr) { new (ptr) T{}; }};
  }

private:
  template <
    unsigned KeyID,
    typename SelectKey,
    typename Tuple_,
    bool SelectRow,
    bool SelectNext,
    typename L>
  void select_(SelectKey selectKey, bool inclusive, unsigned limit, L l);

public:
  // table count is implemented by AnyTable
  uint64_t count() const { return AnyTable::count(); }
  // count query lambda - l(ZuUnion<void, uint64_t>)
  template <unsigned KeyID, typename L>	// initial
  void count(GroupKey<KeyID> groupKey, L l);

  // select query lambda - l(ZuUnion<void, ZuTuple<...>>, unsigned count)
  // - count is #results so far, including this one
  template <unsigned KeyID, typename L>	// initial
  void selectKeys(GroupKey<KeyID> groupKey, unsigned limit, L l) {
    select_<KeyID, GroupKey<KeyID>, Key<KeyID>, 0, 0>(
      ZuMv(groupKey), false, limit, ZuMv(l));
  }
  template <unsigned KeyID, typename L>	// continuation from key
  void nextKeys(Key<KeyID> key, bool inclusive, unsigned limit, L l) {
    select_<KeyID, Key<KeyID>, Key<KeyID>, 0, 1>(
      ZuMv(key), inclusive, limit, ZuMv(l));
  }
  template <unsigned KeyID, typename L>	// initial
  void selectRows(GroupKey<KeyID> groupKey, unsigned limit, L l) {
    select_<KeyID, GroupKey<KeyID>, Tuple, 1, 0>(
      ZuMv(groupKey), false, limit, ZuMv(l));
  }
  template <unsigned KeyID, typename L>	// continuation from key
  void nextRows(Key<KeyID> key, bool inclusive, unsigned limit, L l) {
    select_<KeyID, Key<KeyID>, Tuple, 1, 1>(
      ZuMv(key), inclusive, limit, ZuMv(l));
  }

  // find lambda - l(ZmRef<ZdbObject<T>>)
  template <unsigned KeyID, typename L>
  void find(Key<KeyID> key, L l) {
    config().cacheMode == CacheMode::All ?
      find_<KeyID, true, false>(ZuMv(key), ZuMv(l)) :
      find_<KeyID, true, true >(ZuMv(key), ZuMv(l));
  }

private: // RMU version used by findUpd() and findDel()
  template <unsigned KeyID, typename L>
  void findUpd_(Key<KeyID> key, L l) {
    config().cacheMode == CacheMode::All ?
      find_<KeyID, false, false>(ZuMv(key), ZuMv(l)) :
      find_<KeyID, false, true >(ZuMv(key), ZuMv(l));
  }

public:
  // init lambda - l(ZdbObject<T> *)

  // create new object
  template <typename L>
  void insert(L l) {
    ZmAssert(invoked());

    ZmRef<Object<T>> object = insert_(nextUN());
    if (!object) { l(nullptr); return; }
    try {
      l(object);
    } catch (...) { object->abort(); throw; }
    object->abort();
  }
  // create new object (idempotent with UN as key)
  template <typename L>
  void insert(UN un, L l) {
    ZmAssert(invoked());

    if (un != nullUN() && ZuUnlikely(nextUN() > un)) {
      l(static_cast<Object<T> *>(nullptr));
      return;
    }
    insert(ZuMv(l));
  }

  // update lambda - l(ZdbObject<T> *)
 
  // update object
  template <typename KeyIDs_ = ZuSeq<>, typename L>
  void update(ZmRef<Object<T>> object, L l) {
    ZmAssert(invoked());

    if (!update_(object.ptr(), nextUN())) {
      l(nullptr);
      return;
    }
    auto bufs = ZmAlloc(ZmRef<Buf<T>>, KeyIDs::N);	// undo buffer
    auto nBufs = 0;
    auto abort = [&object, &bufs, &nBufs]() {
      if (!object->abort()) return;
      for (unsigned i = 0; i < nBufs; i++) {
	bufs[i]->stale = false;
	bufs[i].~ZmRef<Buf<T>>();
      }
    };
    ZuUnroll::all<KeyIDs>([this, &object, &bufs, &nBufs](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(object->data());
      auto i = m_bufCache.template iterator<KeyID>(ZuMv(key));
      while (auto typedBuf = i.iterate()) {
	if (!typedBuf->stale) {
	  typedBuf->stale = true;
	  new (&bufs[nBufs++]) ZmRef<Buf<T>>{ZuMv(typedBuf)};
	  // at most one buffer per key can be fresh
	  break;
	}
      }
    });
    try {
      m_cache.template update<KeyIDs_>(object, [
	l = ZuMv(l)
      ](typename Cache<T>::Node *node) mutable {
	l(static_cast<Object<T> *>(node));
      });
    } catch (...) { abort(); throw; }
    abort();
  }
  // update object (idempotent) - calls l(null) to skip
  template <typename KeyIDs_ = ZuSeq<>, typename L>
  void update(ZmRef<Object<T>> object, UN un, L l) {
    ZmAssert(invoked());

    if (un != nullUN() && ZuUnlikely(nextUN() > un)) {
      l(nullptr);
      return;
    }
    update<KeyIDs_>(ZuMv(object), ZuMv(l));
  }

  // find and update record (with key, without object)
  template <unsigned KeyID, typename KeyIDs_ = ZuSeq<>, typename L>
  void findUpd(Key<KeyID> key, L l) {
    findUpd_<KeyID>(ZuMv(key),
	[this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	  if (ZuUnlikely(!object)) { l(object); return; }
	  update<KeyIDs_>(ZuMv(object), ZuMv(l));
	});
  }
  // find and update record (idempotent) (with key, without object)
  template <unsigned KeyID, typename KeyIDs_ = ZuSeq<>, typename L>
  void findUpd(Key<KeyID> key, UN un, L l) {
    findUpd_<KeyID>(ZuMv(key),
	[this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	  if (ZuUnlikely(!object)) { l(object); return; }
	  update<KeyIDs_>(ZuMv(object), un, ZuMv(l));
	});
  }

  // delete lambda - l(ZdbObject<T> *)

  // delete record
  template <typename L>
  void del(ZmRef<Object<T>> object, L l) {
    ZmAssert(invoked());

    if (!del_(object.ptr(), nextUN())) {
      l(nullptr);
      return;
    }
    // all object keys are being invalidated, need to:
    // - evict from cache
    // - mark pending buffers indexed by the old keys as stale
    // - revert above actions on abort
    // - note that a new buffer is written by commit(), which
    //   causes a future find() to return null
    bool cached = false;
    auto bufs = ZmAlloc(ZmRef<Buf<T>>, KeyIDs::N);	// "undo" buffer
    auto nBufs = 0;
    auto abort = [this, &object, &cached, &bufs, &nBufs]() {
      if (!object->abort()) return;
      if (cached) { object->evicted(false); m_cache.add(object); }
      for (unsigned i = 0; i < nBufs; i++) {
	bufs[i]->stale = false;
	bufs[i].~ZmRef<Buf<T>>();
      }
    };
    ZuUnroll::all<KeyIDs>([this, &object, &bufs, &nBufs](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(object->data());
      auto i = m_bufCache.template iterator<KeyID>(ZuMv(key));
      while (auto typedBuf = i.iterate()) {
	if (!typedBuf->stale) {
	  typedBuf->stale = true;
	  new (&bufs[nBufs++]) ZmRef<Buf<T>>{ZuMv(typedBuf)};
	  break;
	}
      }
    });
    try {
      if (cached = m_cache.delNode(object))
	object->evicted(true);
      l(object);
    } catch (...) { abort(); throw; }
    abort();
  }
  // delete record (idempotent) - returns true if del can proceed
  template <typename L>
  void del(ZmRef<AnyObject> object, UN un, L l) {
    if (un != nullUN() && ZuUnlikely(nextUN() > un)) {
      l(nullptr);
      return;
    }
    del(ZuMv(object), ZuMv(l));
  }

  // find and delete record (with key, without object)
  template <unsigned KeyID, typename L>
  void findDel(const Key<KeyID> &key, L l) {
    findUpd_<KeyID>(key,
      [this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	del(ZuMv(object), ZuMv(l));
      });
  }

  // find and delete record (idempotent) (with key, without object)
  template <unsigned KeyID, typename L>
  void findDel(const Key<KeyID> &key, UN un, L l) {
    findUpd_<KeyID>(key,
      [this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	del(ZuMv(object), un, ZuMv(l));
      });
  }

private:
  // commit insert/update/delete - causes replication/write
  bool commit(AnyObject *object) {
    ZmAssert(invoked());

    int origState = object->state();
    if (!object->commit_()) return false;
    switch (origState) {
      case ObjState::Insert:
	if (writeCache()) {
	  m_cache.add(object);
	  cacheUN(object->un(), object);
	}
	incCount();
	break;
      case ObjState::Update:
	if (writeCache())
	  cacheUN(object->un(), object);
	break;
      case ObjState::Delete:
	if (m_cache.delNode(static_cast<Object<T> *>(object)))
	  object->evicted(true);
	decCount();
	break;
    }
    write(object->replicate(int(fbs::Body::Replication)));
    return true;
  }

  // abort insert/update/delete
  bool abort(AnyObject *object) {
    ZmAssert(invoked());

    return object->abort_();
  }

  // low-level insert - calls ctor, AnyObject::insert_()
  ZmRef<Object<T>> insert_(UN un) {
    ZmRef<Object<T>> object = new Object<T>{this};
    if (ZuUnlikely(!object)) return nullptr;
    if (ZuUnlikely(!object->insert_(un))) return nullptr;
    return object;
  }

  // low-level update, calls AnyObject::update_()
  bool update_(Object<T> *object, UN un) {
    evictUN(object->un());
    return object->update_(un);
  }

  // low-level delete, calls AnyObject::del_()
  bool del_(Object<T> *object, UN un) {
    evictUN(object->un());
    return object->del_(un);
  }

private:
  // object caches
  Cache<T>			m_cache;

  // pending replications
  BufCache<T>			m_bufCache;

  // find DLQs
  FindDLQs			m_findDLQs;	// find() dead letter queues
};

template <typename T>
inline void Object_<T>::commit() {
  this->table()->commit(static_cast<AnyObject *>(this));
}
template <typename T>
inline bool Object_<T>::abort() {
  return this->table()->abort(static_cast<AnyObject *>(this));
}

// --- table container

inline constexpr const char *Tables_HeapID() { return "Zdb.Tables"; }
using Tables =
  ZmRBTree<ZmRef<AnyTable>,
    ZmRBTreeKey<AnyTable::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<Tables_HeapID>>>>;

// --- DB host configuration

struct HostCf {
  ZuID		id;
  unsigned	priority = 0;
  ZiIP		ip;
  uint16_t	port = 0;
  bool		standalone = false;
  ZtString	up;
  ZtString	down;

  HostCf(const ZtString &key, const ZvCf *cf) {
    id = key;
    if (!cf->getBool("standalone", false)) {
      priority = cf->getInt<true>("priority", 0, 1<<30);
      ip = cf->get<true>("ip");
      port = cf->getInt<true>("port", 1, (1<<16) - 1);
    }
    up = cf->get("up");
    down = cf->get("down");
  }

  static ZuID IDAxor(const HostCf &cfg) { return cfg.id; }
};

inline constexpr const char *HostCfs_HeapID() { return "Zdb.HostCfs"; }
using HostCfs =
  ZmRBTree<HostCf,
    ZmRBTreeKey<HostCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<HostCfs_HeapID>>>>;

// --- DB host

class ZdbAPI Host {
friend Cxn_;
friend DB;

protected:
  Host(DB *db, const HostCf *cf, unsigned dbCount);

public:
  const HostCf &config() const { return *m_cf; }

  ZuID id() const { return m_cf->id; }
  unsigned priority() const { return m_cf->priority; }
  bool standalone() const { return m_cf->standalone; }
  ZiIP ip() const { return m_cf->ip; }
  uint16_t port() const { return m_cf->port; }

  bool voted() const { return m_voted; }
  int state() const { return m_state; }

  bool replicating() const { return m_cxn; }
  static bool replicating(const Host *host) {
    return host ? host->replicating() : false;
  }

  static const char *stateName(int);

  template <typename S> void print(S &s) const {
    s << "{id=" << id() << ", priority=" << priority()
      << ", voted=" << voted() << ", state=" << state()
      << ", dbState=" << dbState() << '}';
  }
  friend ZuPrintFn ZuPrintType(Host *);

  static ZuID IDAxor(const Host &h) { return h.id(); }
  static ZuTuple<unsigned, ZuID> IndexAxor(const Host &h) {
    return ZuFwdTuple(h.priority(), h.id());
  }

  ZuTuple<Zfb::IOBuilder, Zfb::Offset<void>>
  telemetry(bool update) const;

private:
  ZmRef<Cxn> cxn() const { return m_cxn; }

  void state(int s) { m_state = s; }

  const DBState &dbState() const { return m_dbState; }
  DBState &dbState() { return m_dbState; }

  bool active() const { return m_state == HostState::Active; }

  int cmp(const Host *host) const {
    if (ZuUnlikely(host == this)) return 0;
    int i;
    if (i = m_dbState.cmp(host->m_dbState)) return i;
    if (i = ZuCmp<bool>::cmp(active(), host->active())) return i;
    return ZuCmp<int>::cmp(priority(), host->priority());
  }

  void voted(bool v) { m_voted = v; }

  void connect();
  void connectFailed(bool transient);
  void reconnect();
  void reconnect2();
  void cancelConnect();
  ZiConnection *connected(const ZiCxnInfo &ci);
  void associate(Cxn *cxn);
  void disconnected();

  void reactivate();

  DB			*m_db;
  const HostCf		*m_cf;
  ZiMultiplex		*m_mx;

  ZmScheduler::Timer	m_connectTimer;

  // guarded by DB

  ZmRef<Cxn>		m_cxn;
  int			m_state = HostState::Instantiated;
  DBState		m_dbState;
  bool			m_voted = false;
};

// host container
using HostIndex =
  ZmRBTree<Host,
    ZmRBTreeNode<Host,
      ZmRBTreeShadow<true,
	ZmRBTreeKey<Host::IndexAxor,
	  ZmRBTreeUnique<true>>>>>;
inline constexpr const char *Hosts_HeapID() { return "Zdb.Hosts"; }
using Hosts =
  ZmHash<HostIndex::Node,
    ZmHashNode<HostIndex::Node,
      ZmHashKey<Host::IDAxor,
	ZmHashHeapID<Hosts_HeapID>>>>;

// --- DB handler functions

// UpFn() - activate
typedef void (*UpFn)(DB *, Host *); // db, oldMaster
// DownFn() - de-activate
typedef void (*DownFn)(DB *);

struct DBHandler {
  UpFn		upFn = [](DB *, Host *) { };
  DownFn	downFn = [](DB *) { };
};

// --- DB configuration

struct DBCf {
  ZmThreadName		thread;
  mutable unsigned	sid = 0;
  ZmRef<ZvCf>		storeCf;
  TableCfs		tableCfs;
  HostCfs		hostCfs;
  ZuID			hostID;
  unsigned		nAccepts = 0;
  unsigned		heartbeatFreq = 0;
  unsigned		heartbeatTimeout = 0;
  unsigned		reconnectFreq = 0;
  unsigned		electionTimeout = 0;
  unsigned		retryFreq = 0;
  ZmHashParams		cxnHash;
#if Zdb_DEBUG
  bool			debug = 0;
#endif

  DBCf() = default;
  DBCf(const ZvCf *cf) {
    thread = cf->get<true>("thread");
    storeCf = cf->getCf("store");
    cf->getCf<true>("tables")->all([this](ZvCfNode *node) {
      if (auto tableCf = node->getCf())
	tableCfs.addNode(new TableCfs::Node{node->key, ZuMv(tableCf)});
    });
    cf->getCf<true>("hosts")->all([this](ZvCfNode *node) {
      if (auto hostCf = node->getCf())
	hostCfs.addNode(new HostCfs::Node{node->key, ZuMv(hostCf)});
    });
    hostID = cf->get("hostID"); // may be supplied separately
    nAccepts = cf->getInt("nAccepts", 1, 1<<10, 8);
    heartbeatFreq = cf->getInt("heartbeatFreq", 1, 3600, 1);
    heartbeatTimeout = cf->getInt("heartbeatTimeout", 1, 14400, 4);
    reconnectFreq = cf->getInt("reconnectFreq", 1, 3600, 1);
    electionTimeout = cf->getInt("electionTimeout", 1, 3600, 8);
    retryFreq = cf->getInt("retryFreq", 1, 3600, 1);
#if Zdb_DEBUG
    debug = cf->getBool("debug");
#endif
  }
  DBCf(DBCf &&) = default;
  DBCf &operator =(DBCf &&) = default;
};

// --- DB

class ZdbAPI DB : public ZmPolymorph, public ZmEngine<DB> {
  DB(const DB &);
  DB &operator =(const DB &);		// prevent mis-use

public:
  using Engine = ZmEngine<DB>;

  using Engine::start;
  using Engine::stop;

friend Engine;
friend Cxn_;
friend Host;
friend AnyTable;
friend AnyObject;

private:
  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

private:
  static const char *CxnHash_HeapID() { return "Zdb.CxnHash"; }
  using CxnHash =
    ZmHash<ZmRef<Cxn>,
      ZmHashLock<ZmPLock,
	  ZmHashHeapID<CxnHash_HeapID>>>;

public:
#if Zdb_DEBUG
  bool debug() const { return m_cf.debug; }
#endif

  DB() { }
  ~DB() { }

  // init() and final() throw ZtString on error
  void init(
      DBCf config,
      ZiMultiplex *mx,
      DBHandler handler,
      ZmRef<Store> store = {});
  void final();

  template <typename T>
  ZmRef<Table<T>> initTable(ZuID id) {
    return initTable_(id,
      [](DB *db, TableCf *tableCf) mutable {
	return static_cast<AnyTable *>(new Table<T>{db, tableCf});
      });
  }

private:
  ZmRef<AnyTable> initTable_(ZuID, ZmFn<AnyTable *(DB *, TableCf *)> ctorFn);

public:
  template <typename ...Args>
  void run(Args &&...args) const {
    m_mx->run(m_cf.sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&...args) const {
    m_mx->invoke(m_cf.sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_cf.sid); }

  const DBCf &config() const { return m_cf; }
  ZiMultiplex *mx() const { return m_mx; }

  int state() const {
    return ZuLikely(m_self) ? m_self->state() : HostState::Instantiated;
  }
private:
  void state(int n) {
    if (ZuUnlikely(!m_self)) {
      ZeLOG(Fatal, ([n](auto &s) {
	s << "Zdb::state(" << HostState::name(n) <<
	  ") called out of order";
      }));
      return;
    }
    m_self->state(n);
  }
public:
  bool active() const { return state() == HostState::Active; }

  Host *self() const { return m_self; }
  template <typename L> void allHosts(L l) const {
    auto i = m_hosts->readIterator();
    while (auto node = i.iterate()) l(node);
  }

  // backing data store
  Store *store() const { return m_store; }

  // find table
  ZmRef<AnyTable> table(ZuID id) {
    ZmAssert(invoked());

    return m_tables.findVal(id);
  }

  using AllFn = ZmFn<void(AnyTable *, ZmFn<void(bool)>)>;
  using AllDoneFn = ZmFn<void(DB *, bool)>;

  void all(AllFn fn, AllDoneFn doneFn = AllDoneFn{});

  ZuTuple<Zfb::IOBuilder, Zfb::Offset<void>>
  telemetry(bool update) const;

private:
  void allDone(bool ok);

  template <typename L> void all_(L l) const {
    auto i = m_tables.readIterator();
    while (auto table = i.iterateVal()) l(table);
  }

  // debug printing
  template <typename S> void print(S &);
  friend ZuPrintFn ZuPrintType(DB *);

  // ZmEngine implementation
  void start_();
  void stop_();
  template <typename L>
  bool spawn(L l) {
    if (!m_mx || !m_mx->running()) return false;
    m_mx->run(m_cf.sid, ZuMv(l));
    return true;
  }
  void wake();

  void start_1();
  void start_2();
  void stop_1();
  void stop_2();
  void stop_3();

  // leader election and activation/deactivation
  void holdElection();		// elect new leader
  void deactivate();		// become client (following dup leader)
  void reactivate(Host *host);	// re-assert leader

  void up_(Host *oldMaster);	// run up command
  void down_();			// run down command

  // host connection management
  void listen();
  void listening(const ZiListenInfo &);
  void listenFailed(bool transient);
  void stopListening();

  bool disconnectAll();

  ZiConnection *accepted(const ZiCxnInfo &ci);
  void connected(ZmRef<Cxn> cxn);
  void disconnected(ZmRef<Cxn> cxn);
  void associate(Cxn *cxn, ZuID hostID);
  void associate(Cxn *cxn, Host *host);

  // heartbeats and voting
  void hbRcvd(Host *host, const fbs::Heartbeat *hb);
  void vote(Host *host);

  void hbStart();
  void hbSend();		// send heartbeat and reschedule self
  void hbSend_();		// send heartbeat (once, broadcast)
  void hbSend_(Cxn *cxn);	// send heartbeat (once, directed)

  void dbStateRefresh();	// refresh m_self->dbState()

  Host *setMaster();		// returns old leader
  void setNext(Host *host);
  void setNext();

  // outbound replication
  void repStart();
  void repStop();
  void recEnd();

  bool replicate(ZmRef<const IOBuf> buf);

  // inbound replication
  void replicated(Host *host, ZuID dbID, UN un, SN sn);

  bool isStandalone() const { return m_standalone; }

  // SN
  SN allocSN() { return m_nextSN++; }
  void recoveredSN(SN sn) {
    if (ZuUnlikely(sn == nullSN())) return;
    m_nextSN.maximum(sn + 1);
  }

  // data store
  bool repStore() const { return m_repStore; }

  DBCf			m_cf;
  ZiMultiplex		*m_mx = nullptr;
  ZmRef<Store>		m_store;
  bool			m_repStore = false;	// replicated data store

  // mutable while stopped
  DBHandler		m_handler;
  ZmRef<Hosts>		m_hosts;
  HostIndex		m_hostIndex;

  // SN allocator - atomic
  ZmAtomic<SN>		m_nextSN = 0;

  // DB thread
  Tables		m_tables;
  CxnList		m_cxns;
  AllFn			m_allFn;		// all() iteration context
  AllDoneFn		m_allDoneFn;		// ''
  unsigned		m_allCount = 0;		// remaining count
  unsigned		m_allNotOK = 0;		// remaining not OK

  bool			m_appActive =false;
  Host			*m_self = nullptr;
  Host			*m_leader = nullptr;	// == m_self if Active
  Host			*m_prev = nullptr;	// previous-ranked host
  Host			*m_next = nullptr;	// next-ranked host
  unsigned		m_recovering = 0;	// recovering next-ranked host
  DBState		m_recover{4};		// recovery state
  DBState		m_recoverEnd{4};	// recovery end
  int			m_nPeers = 0;	// # up to date peers
					// # votes received (Electing)
					// # pending disconnects (Stopping)
  ZuTime		m_hbSendTime;

  bool			m_standalone = false;

  ZmScheduler::Timer	m_hbSendTimer;
  ZmScheduler::Timer	m_electTimer;

  // telemetry
  ZuID			m_selfID, m_leaderID, m_prevID, m_nextID;
};

template <typename S>
inline void DB::print(S &s)
{
  s <<
    "self=" << ZuPrintPtr{m_self} << '\n' <<
    " prev=" << ZuPrintPtr{m_prev} << '\n' <<
    " next=" << ZuPrintPtr{m_next} << '\n' <<
    " recovering=" << m_recovering <<
    " replicating=" << Host::replicating(m_next);

  auto i = m_hostIndex.readIterator();

  while (Host *host = i.iterate()) {
    ZdbDEBUG(this, ZtString{} <<
	" host=" << ZuPrintPtr{host} << '\n' <<
	" leader=" << ZuPrintPtr{m_leader});

    if (host->voted()) {
      if (host != m_self) ++m_nPeers;
      if (!m_leader) { m_leader = host; continue; }
      if (host->cmp(m_leader) > 0) m_leader = host;
    }
  }
}

template <typename T>
template <unsigned KeyID, typename L>
inline void Table<T>::count(GroupKey<KeyID> key, L l)
{
  using Context = Count;

  ZmAssert(invoked());

  auto context = ZmMkRef(new Context{ZuMv(l)});

  using Key = GroupKey<KeyID>;

  Zfb::IOBuilder fbb{allocBuf()};
  fbb.Finish(
    ZfbField::SaveFieldsFn<Key, ZuFieldList<Key>>::save(fbb, key).Union());
  auto keyBuf = fbb.buf();

  auto countFn = CountFn::mvFn(ZuMv(context),
    [](ZmRef<Context> context, CountResult result) {
      if (ZuUnlikely(result.is<Event>())) { // error
	ZeLogEvent(ZuMv(result).p<Event>());
	context->fn(typename Context::Result{});
	return;
      }
      context->fn(typename Context::Result{result.p<CountData>().count});
    });

  storeTbl()->count(KeyID, ZuMv(keyBuf).constRef(), ZuMv(countFn));
}

template <typename T>
template <
  unsigned KeyID,
  typename SelectKey,
  typename Tuple_,
  bool SelectRow,
  bool SelectNext,
  typename L>
inline void Table<T>::select_(
  SelectKey selectKey, bool inclusive, unsigned limit, L l)
{
  using Context = Select<Tuple_>;

  ZmAssert(invoked());

  auto context = ZmMkRef(new Context{ZuMv(l)});

  Zfb::IOBuilder fbb{allocBuf()};
  fbb.Finish(
    ZfbField::SaveFieldsFn<SelectKey, ZuFieldList<SelectKey>>::save(
      fbb, selectKey).Union());
  auto keyBuf = fbb.buf();

  auto tupleFn = TupleFn::mvFn(ZuMv(context),
    [](ZmRef<Context> context, TupleResult result) {
      if (ZuUnlikely(result.is<Event>())) { // error
	ZeLogEvent(ZuMv(result).p<Event>());
	context->fn(typename Context::Result{}, 0);
	return;
      }
      if (ZuUnlikely(!result.is<TupleData>())) { // end of results
	context->fn(typename Context::Result{}, 0);
	return;
      }
      auto tupleData = result.p<TupleData>();
      auto fbo = ZfbField::root<T>(tupleData.buf->data());
      auto tuple = ZfbField::ctor<Tuple_>(fbo);
      context->fn(typename Context::Result{ZuMv(tuple)}, tupleData.count);
    });

  storeTbl()->select(
    SelectRow, SelectNext, inclusive,
    KeyID, ZuMv(keyBuf).constRef(), limit, ZuMv(tupleFn));
}

template <typename T>
template <unsigned KeyID, bool UpdateLRU, bool Evict, typename L>
inline void Table<T>::find_(Key<KeyID> key, L l) {
  ZmAssert(invoked());

  auto load = [this]<typename L_>(const Key<KeyID> &key, L_ l) mutable {
    auto [buf, found] = findBuf<KeyID>(key);
    if (buf) {
      l(objLoad(buf));
      return;
    }
    if (found) {
      l(nullptr);
      return;
    }
    retrieve<KeyID>(key, ZuMv(l));
  };
  if constexpr (Evict) {
    auto evict = [this](ZmRef<AnyObject> object) {
      evictUN(object->un());
      object->evicted(true);
    };
    m_cache.template find<KeyID, UpdateLRU>(
	ZuMv(key), ZuMv(l), ZuMv(load), ZuMv(evict));
  } else
    m_cache.template find<KeyID, UpdateLRU, false>(
	ZuMv(key), ZuMv(l), ZuMv(load));
}

template <typename T>
template <unsigned KeyID>
inline void Table<T>::retrieve(Key<KeyID> key, ZmFn<void(ZmRef<Object<T>>)> fn)
{
  using Key_ = Key<KeyID>;
  using Context = Find<T, Key_>;

  ZmAssert(invoked());

  auto context = ZmMkRef(new Context{this, ZuMv(key), ZuMv(fn)});

  // DLQ draining in progress - just push onto the queue
  auto &dlq = *(m_findDLQs.template p<KeyID>());
  if (dlq.count_()) {
    dlq.push(ZuMv(context));
    return;
  }

  retrieve_<KeyID>(ZuMv(context));
}
template <typename T>
template <unsigned KeyID>
void Table<T>::retryRetrieve_()
{
  auto &dlq = *(m_findDLQs.template p<KeyID>());

  if (!dlq.count_()) return;
  retrieve_<KeyID>(dlq.shift());
}
template <typename T>
template <unsigned KeyID>
inline void Table<T>::retrieve_(ZmRef<Find<T, ZuFieldKeyT<T, KeyID>>> context)
{
  using Key_ = Key<KeyID>;
  using Context = Find<T, Key_>;

  Zfb::IOBuilder fbb{allocBuf()};
  fbb.Finish(ZfbField::save(fbb, context->key));
  auto keyBuf = fbb.buf();

  auto rowFn = RowFn::mvFn(ZuMv(context),
    [](ZmRef<Context> context, RowResult result) {
      auto table = context->table;
      auto &dlq = *(table->m_findDLQs.template p<KeyID>());
      if (ZuUnlikely(result.is<Event>())) {
	ZeLogEvent(ZuMv(result).p<Event>());
	dlq.unshift(ZuMv(context)); // unshift, not push
	table->run([table]() {
	  table->template retryRetrieve_<KeyID>();
	}, Zm::now(table->db()->config().retryFreq));
	return;
      }
      if (ZuLikely(result.is<RowData>())) {
	auto buf = ZuMv(ZuMv(result).p<RowData>().buf);
	table->invoke(
	  [table, fn = ZuMv(context->fn), buf = ZuMv(buf)]() mutable {
	    fn(table->objLoad(ZuMv(buf)));
	  });
      } else
	table->invoke([fn = ZuMv(context->fn)]() mutable { fn(nullptr); });
      if (dlq.count_())
	table->run([table]() {
	  table->template retryRetrieve_<KeyID>();
	});
    });

  storeTbl()->find(KeyID, ZuMv(keyBuf).constRef(), ZuMv(rowFn));
}

// --- printing

template <typename S>
inline void DBState::print(S &s) const {
  s << "{sn=" << ZuBoxed(sn) << " dbs={";
  unsigned n = count_();
  if (ZuLikely(n)) {
    unsigned j = 0;
    auto i = readIterator();
    while (auto state = i.iterate()) {
      if (j++) s << ',';
      s << '{'
	<< state->template p<0>() << " "
	<< ZuBoxed(state->template p<1>()) << '}';
    }
  }
  s << "}}";
}

struct Record_Print {
  const fbs::Record *record = nullptr;
  const AnyTable *table = nullptr;
  template <typename S> void print(S &s) const {
    auto id = Zfb::Load::id(record->table());
    auto data = Zfb::Load::bytes(record->data());
    s << "{db=" << id
      << " un=" << record->un()
      << " sn=" << ZuBoxed(Zfb::Load::uint128(record->sn()))
      << " vn=" << record->vn() << "}";
    if (data) {
      s << " data=";
      if (table) {
	ZuMStream s_{s};
	table->objPrintFB(s_, data);
      } else {
	s << "{...}";
      }
    } else {
      s << " data=(null)}";
    }
  }
  friend ZuPrintFn ZuPrintType(Record_Print *);
};

struct HB_Print {
  const fbs::Heartbeat *hb = nullptr;
  template <typename S> void print(S &s) const {
    auto id = Zfb::Load::id(hb->host());
    s << "{host=" << id
      << " state=" << HostState::name(hb->state())
      << " dbState=" << DBState{hb->dbState()} << "}";
  }
  friend ZuPrintFn ZuPrintType(HB_Print *);
};

template <typename S>
void IOBuf_::Print::print(S &s) const {
  auto msg = Zdb_::msg(buf->ptr<Hdr>());
  if (!msg) { s << "corrupt{}"; return; }
  if (auto record = Zdb_::record(msg)) {
    s << "record=" << Record_Print{record, table};
    return;
  }
  if (auto hb = Zdb_::hb(msg)) {
    s << "heartbeat=" << HB_Print{hb};
    return;
  }
  s << "unknown{}";
}

template <typename S>
void AnyObject::print(S &s) const {
  s << "{table=" << m_table->id()
    << " state=" << ObjState::name(m_state)
    << " un=" << m_un
    << " sn=" << m_sn
    << " vn=" << m_vn;
  if (m_origUN != nullUN()) s << " origUN=" << m_origUN;
  s << " data={";
  {
    ZuMStream s_{s};
    m_table->objPrint(s_, ptr_());
  }
  s << "}}";
};

} // Zdb_

// external API

using ZdbAnyObject = Zdb_::AnyObject;
template <typename T> using ZdbObject = Zdb_::Object<T>;
template <typename T> using ZdbObjRef = ZmRef<ZdbObject<T>>;

using ZdbAnyTable = Zdb_::AnyTable;
template <typename T> using ZdbTable = Zdb_::Table<T>;
using ZdbTableCf = Zdb_::TableCf;

using Zdb = Zdb_::DB;
using ZdbHandler = Zdb_::DBHandler;
using ZdbCf = Zdb_::DBCf;

using ZdbUpFn = Zdb_::UpFn;
using ZdbDownFn = Zdb_::DownFn;
using ZdbHandler = Zdb_::DBHandler;

using ZdbHost = Zdb_::Host;

namespace ZdbCacheMode = Zdb_::CacheMode;
namespace ZdbHostState = Zdb_::HostState;

#endif /* Zdb_HH */
