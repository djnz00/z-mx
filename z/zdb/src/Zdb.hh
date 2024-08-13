//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z database

// Zdb is a clustered/replicated in-process/in-memory DB/ORM that includes
// RAFT-like leader election and failover. Zdb dynamically organizes
// cluster hosts into a replication chain from the leader to the
// lowest-priority follower. Replication is async. ZmEngine is used for
// start/stop state management. Zdb applications are stateful back-end
// services that defer to Zdb for activation/deactivation.
// Restart/recovery is from backing data store, then from the cluster
// leader (if the local host itself is not elected leader).

// Principal features:
// - Plug-in backing data store (mocked for unit-testing)
//   - Currently Postgres
// - In-memory write-through object cache
//   - Deferred async writes
//   - In-memory write queue of I/O buffers
// - Async replication independent of backing store
//   (can be disabled for replicated backing stores)
// - Primary and multiple-secondary unique in-memory and on-disk indices
// - Find, insert, update, delete operations (Find and CRUD)
// - Batched select and count queries (index-based, optionally grouped)
// - Front-end shares threads with the application
// - Optional data sharding for multi-threaded concurrency

// select() is an un-cached backing data store query that
// returns 0..N immutable ZuTuples for read-only purposes
// - cache consistency is assured by enqueuing the select on the
//   back-end write queue, ensuring results reflect any pending updates
//   outstanding at the time of the call; the results may become outdated
//   when eventually processed if further updates are concurrently performed
//   while the select itself is outstanding - an intentional limitation

// insert() inserts new objects (rows)
// find() returns 0..1 mutable ZdbObjects for read-modify-write
// update() updates existing objects
// del() deletes existing objects

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

#include <zlib/zdb_telemetry_fbs.h>

#if defined(ZDEBUG) && !defined(Zdb_DEBUG)
#define Zdb_DEBUG 1
#endif

#if Zdb_DEBUG
#define ZdbDEBUG(db, e) \
  do { if ((db)->debug()) ZeLOG(Debug, (e)); } while (0)
#else
#define ZdbDEBUG(db, e) (void())
#endif

// heap and hash configuration
//
// ZdbHeapID<T>		- heap ID for objects
// ZdbBufSize<T>	- buffer size
// ZdbBufHeapID<T>	- heap ID for buffers

// type-specific cache ID - specialize to override default
template <typename>
struct ZdbHeapID {
  static constexpr const char *id() { return "Zdb.Object"; }
};

namespace Zdb_ {

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

  using BufAlloc = Zdb_::RxBufAlloc;

  using Rx = ZiRx<Cxn_, BufAlloc>;
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

using DBState_ = ZmLHashKV<ZuTuple<ZuID, unsigned>, UN, ZmLHashLocal<>>;
struct DBState : public DBState_ {
  SN		sn = 0;

  DBState() = delete;

  DBState(unsigned size) : DBState_{ZmHashParams{size}} { }

  DBState(const fbs::DBState *dbState) :
      DBState_{ZmHashParams{dbState->tableStates()->size()}},
      sn{Zfb::Load::uint128(dbState->sn())} {
    Zfb::Load::all(dbState->tableStates(),
	[this](unsigned, const fbs::TableState *tableState) {
	  add(Zfb::Load::id(&(tableState->table())), tableState->un());
	});
  }
  void load(const fbs::DBState *dbState) {
    sn = Zfb::Load::uint128(dbState->sn());
    Zfb::Load::all(dbState->tableStates(),
	[this](unsigned, const fbs::TableState *tableState) {
	  update(Zfb::Load::id(&(tableState->table())), tableState->un());
	});
  }
  Zfb::Offset<fbs::DBState> save(Zfb::Builder &fbb) const {
    auto sn_ = Zfb::Save::uint128(sn);
    auto i = readIterator();
    return fbs::CreateDBState(
      fbb, &sn_, Zfb::Save::structVecIter<fbs::TableState>(
	fbb, i.count(),
	[&i](fbs::TableState *ptr, unsigned) {
	  if (auto state = i.iterate())
	    new (ptr)
	      fbs::TableState{
		Zfb::Save::id(state->template p<0>().template p<0>()),
		state->template p<1>(),
		uint16_t(state->template p<0>().template p<1>())};
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
  bool update(ZuTuple<ZuID, unsigned> key, UN un_) {
    auto state = find(key);
    if (!state) {
      add(key, un_);
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

// --- generic object

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

  enum { Evicted = 0x01, Pinned = 0x02 };

public:
  AnyObject(AnyTable *table) : m_table{table} { }

  AnyTable *table() const { return m_table; }
  unsigned shard() const { return m_shard; }
  UN un() const { return m_un; }
  SN sn() const { return m_sn; }
  VN vn() const { return m_vn; }
  int state() const { return m_state; }	// ObjState
  UN origUN() const { return m_origUN; }
  bool evicted() const { return m_flags & Evicted; }
  bool pinned() const { return m_flags & Pinned; }

  ZmRef<const IOBuf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<AnyObject *>(this)->ptr_(); }

  virtual void evict() { m_flags |= Evicted; }	// must call this if overridden
  void pin() { m_flags |= Pinned; }
  void unpin() { m_flags &= ~Pinned; }

  template <typename S> void print(S &s) const;
  friend ZuPrintFn ZuPrintType(AnyObject *);

private:
  void init(unsigned shard, UN un, SN sn, VN vn) {
    m_shard = shard;
    m_un = un;
    m_sn = sn;
    m_vn = vn;
    m_state = ObjState::Committed;
  }

  bool insert_(unsigned shard, UN un);
  bool update_(UN un);
  bool del_(UN un);
  bool commit_();
  bool abort_();

  AnyTable		*m_table;
  UN			m_un = nullUN();
  SN			m_sn = nullSN();
  VN			m_vn = 0;
  UN			m_origUN = nullUN();
  uint8_t		m_shard = 0;
  int8_t		m_state = ObjState::Undefined;
  uint8_t		m_flags = 0;
};

inline UN AnyObject_UNAxor(const ZmRef<AnyObject> &object) {
  return object->un();
}

inline constexpr const char *CacheUN_HeapID() { return "Zdb.UpdCache"; }

// temporarily there may be more than one UN referencing a cached object
using CacheUN =
  ZmHashKV<UN, ZmRef<AnyObject>,
    ZmHashLock<ZmPLock,
      ZmHashHeapID<CacheUN_HeapID>>>;

// --- typed object

// Zdf data-frames are comprised of series fields that do not form part of
// a primary or secondary key for the object - Zdb skips Zdf fields
// and does not persist them
template <typename Field>
struct FieldFilter :
    public ZuBool<!ZuTypeIn<ZuFieldProp::Series, typename Field::Props>{}> { };

template <typename T>
using Fields = ZuTypeGrep<FieldFilter, ZuFields<T>>;

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
  using Fields_ = Zdb_::Fields<T>;
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
  friend Fields ZuFields_(Object_ *);

private:
  uint8_t	m_data[sizeof(T)];
}
#ifdef __GNUC__
  __attribute__ ((aligned(alignof(T_))))
#endif
;

// typed object cache
template <typename T>
using Cache =
  ZmPolyCache<Object_<T>,
    ZmPolyCacheHeapID<ZdbHeapID<T>::id>>;

// typed object
template <typename T>
struct Object : public Cache<T>::Node {
  using Base = Cache<T>::Node;
  using Base::Base;
  using Base::operator =;
  using Object_<T>::data;	// disambiguate from Node::data
};

// --- table configuration

struct TableCf {
  using ThreadArray = ZtArray<ZtString>;
  using SIDArray = ZtArray<unsigned>;

  // nShards and threads.length() must both be a power of 2
  // threads.length() must be <= nShards
  // nShards must be <= 64
  // nShards is immutable for the table, i.e. is an upper concurrency limit

  ZuID			id;
  unsigned		nShards = 1;	// #shards
  ThreadArray		thread;		// threads
  mutable SIDArray	sid = 0;	// thread slot IDs
  int			cacheMode = CacheMode::Normal;

  class InvalidNThreads : public ZvError {
  public:
    InvalidNThreads(unsigned nThreads, unsigned nShards) :
      m_nThreads{nThreads}, m_nShards{nShards} { }

    void print_(ZuVStream &s) const {
      s << "invalid threads array size " << m_nThreads
	<< " (" << m_nShards << " shards)";
    }

  private:
    unsigned	m_nThreads;
    unsigned	m_nShards;
  };

  TableCf() = default;
  TableCf(ZuString id_) : id{id_} { }
  TableCf(ZuString id_, const ZvCf *cf) : id{id_} {
    nShards = cf->getScalar<unsigned>("shards", 1, 64, 1);
    unsigned nThreads = cf->count("threads", 1, 64);
    if (nThreads) {
      // ensure nThreads is a power of 2 and <= nShards
      if ((nThreads & (nThreads - 1)) ||
	  nThreads > nShards)
	throw InvalidNThreads{nThreads, nShards};
      thread.size(nThreads);
      cf->all("threads", [this](ZtString thread_) {
	thread.push(ZuMv(thread_));
      });
    }
    cacheMode = cf->getEnum<CacheMode::Map>(
	"cacheMode", CacheMode::Normal);
  }

  static ZuID IDAxor(const TableCf &cf) { return cf.id; }
};

// --- table configuration

inline constexpr const char *TableCfs_HeapID() { return "Zdb.TableCf"; }
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

protected:
  AnyTable(DB *db, TableCf *cf, IOBufAllocFn);

public:
  ~AnyTable();

private:
  template <typename L> void open(L l);		// l(OpenResult)
  bool opened(OpenResult);
  template <typename L> void close(L l);	// l()
protected:
  void warmup() { m_storeTbl->warmup(); }

public:
  DB *db() const { return m_db; }
  ZiMultiplex *mx() const { return m_mx; }
  const TableCf &config() const { return *m_cf; }
  IOBufAllocFn bufAllocFn() const { return m_bufAllocFn; }

  static ZuID IDAxor(AnyTable *table) { return table->config().id; }

  ZuID id() const { return config().id; }
  auto sid(unsigned shard) const {
    const auto &config = this->config();
    return config.sid[shard & (config.sid.length() - 1)];
  }

  // DB thread (may be shared)
  template <typename ...Args>
  void run(unsigned shard, Args &&...args) const {
    m_mx->run(sid(shard), ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(unsigned shard, Args &&...args) const {
    m_mx->invoke(sid(shard), ZuFwd<Args>(args)...);
  }
  bool invoked(unsigned shard) const {
    return m_mx->invoked(sid(shard));
  }

  // record count - SWMR
  uint64_t count() const { return m_count.load_(); }

  // allocate I/O buffer
  ZmRef<IOBuf> allocBuf() { return m_bufAllocFn(); }

private:
  IOBuf *findBufUN(unsigned shard, UN un) {
    return static_cast<IOBuf *>(m_bufCacheUN[shard]->find(un));
  }
protected:
  void cacheBufUN(unsigned shard, IOBuf *buf) {
    m_bufCacheUN[shard]->addNode(buf);
  }
  auto evictBufUN(unsigned shard, UN un) {
    return m_bufCacheUN[shard]->del(un);
  }

public:
  // next UN that will be allocated
  UN nextUN(unsigned shard) const { return m_nextUN[shard]; }

  // enable/disable writing to cache (temporarily)
  void writeCache(bool enabled) { m_writeCache = enabled; }

  // all transactions begin with a insert(), update() or del(),
  // and complete with object->commit() or object->abort()

protected:
  // -- implemented by Table<T>

  // objSave(fbb, ptr) - save object into flatbuffer, return offset
  virtual Zfb::Offset<void> objSave(Zfb::Builder &, const void *) const = 0;
  virtual Zfb::Offset<void> objSaveUpd(Zfb::Builder &, const void *) const = 0;
  virtual Zfb::Offset<void> objSaveDel(Zfb::Builder &, const void *) const = 0;
  // objRecover(record) - process recovered FB record (untrusted source)
  virtual void objRecover(const fbs::Record *) = 0;

  // objFields() - run-time field array
  virtual ZtVFieldArray objFields() const = 0;
  // objKeyFields() - run-time key field arrays
  virtual ZtVKeyFieldArray objKeyFields() const = 0;
  // objSchema() - flatbuffer reflection schema
  virtual const reflection::Schema *objSchema() const = 0;

  // objPrint(stream, ptr) - print object
  virtual void objPrint(ZuVStream &, const void *) const = 0;
  // objPrintFB(stream, data) - print flatbuffer
  virtual void objPrintFB(ZuVStream &, ZuBytes) const = 0;

  // buffer cache
  virtual void cacheBuf_(unsigned shard, ZmRef<const IOBuf>) = 0;
  virtual ZmRef<const IOBuf> evictBuf_(unsigned shard, IOBuf *) = 0;

  // cache statistics
  virtual void cacheStats(unsigned shard, ZmCacheStats &stats) const = 0;

public:
  Zfb::Offset<void> telemetry(Zfb::Builder &fbb, bool update) const;

protected:
  bool writeCache() const { return m_writeCache; }

  auto findUN(unsigned shard, UN un) const {
    return m_cacheUN[shard]->findVal(un);
  }
  void cacheUN(unsigned shard, UN un, AnyObject *object) {
    m_cacheUN[shard]->add(un, object);
  }
  void evictUN(unsigned shard, UN un) {
    m_cacheUN[shard]->del(un);
  }

  StoreTbl *storeTbl() const { return m_storeTbl; }

protected:
  // cache replication buffer
  void cacheBuf(unsigned shard, ZmRef<const IOBuf>);
  // evict replication buffer
  void evictBuf(unsigned shard, UN un);

  // outbound replication / write to backing data store
  void write(unsigned shard, ZmRef<const IOBuf> buf, bool active);

  // maintain record count
  void incCount() { ++m_count; }
  void decCount() { --m_count; }

private:
  // low-level write to backing data store
  void store(unsigned shard, ZmRef<const IOBuf>);
  void store_(unsigned shard, ZmRef<const IOBuf>);
  void committed(ZmRef<const IOBuf>, CommitResult);

  // outbound recovery / replication
  void recSend(ZmRef<Cxn> cxn, unsigned shard, UN un, UN endUN);
  void recSend_(ZmRef<Cxn> cxn, unsigned shard, UN un, UN endUN, ZmRef<const IOBuf> buf);
  void recNext(ZmRef<Cxn> cxn, unsigned shard, UN un, UN endUN);
  ZmRef<const IOBuf> mkBuf(unsigned shard, UN un);
  void commitSend(unsigned shard, UN un);

  // inbound replication
  void repRecordRcvd(unsigned shard, ZmRef<const IOBuf> buf);
  void repCommitRcvd(unsigned shard, UN un);

  // recovery - DB thread
  void recover(unsigned shard, const fbs::Record *record);

  // UN
  bool allocUN(unsigned shard, UN un) {
    if (ZuUnlikely(un != m_nextUN[shard])) return false;
    ++m_nextUN[shard];
    return true;
  }
  void recoveredUN(unsigned shard, UN un) {
    if (ZuUnlikely(un == nullUN())) return;
    if (m_nextUN[shard] <= un) m_nextUN[shard] = un + 1;
  }

  // immutable
  DB			*m_db;
  const TableCf		*m_cf;
  ZiMultiplex		*m_mx;

  // Table threads SWMR
  ZtArray<ZmAtomic<UN>>	m_nextUN;		// UN allocator

  // open/closed state, record count
  ZmAtomic<unsigned>	m_open = 0;		// Table threads SWMR
  ZmAtomic<uint64_t>	m_count = 0;		// ''

  // backing data store table
  StoreTbl		*m_storeTbl = nullptr;	// Table threads

  // object cache indexed by UN (sharded)
  using CacheUNArray = ZtArray<ZmRef<CacheUN>>;
  bool			m_writeCache = true;
  CacheUNArray		m_cacheUN;

  // buffer cache indexed by UN (sharded)
  using BufCacheUNArray = ZtArray<ZmRef<BufCacheUN>>;
  BufCacheUNArray	m_bufCacheUN;

  // I/O buffer allocation
  IOBufAllocFn		m_bufAllocFn;
};

// replication buffer base
// - replication buffers contain a reference to the underlying I/O buffer
// - type information permits type-specific key indexing and caching
template <typename T_>
struct Buf_ : public ZmPolymorph {
  ZmRef<const IOBuf>	buf;
  bool			stale = false;	// true if outdated by subsequent txn

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
  using Fields_ = Zdb_::Fields<FB>;
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
  friend Fields ZuFields_(Buf_ *);

  // override printing
  friend ZtFieldPrint ZuPrintType(Buf_ *);
};

// buffer heap ID
inline constexpr const char *Buf_HeapID() { return "Zdb.Buf"; }

// buffer cache
template <typename T>
using BufCache = ZmPolyHash<Buf_<T>, ZmPolyHashHeapID<ZdbBufHeapID<T>::id>>;

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

// backing data store find() context
template <typename T, typename Key> struct Find__ {
  Table<T>				*table;
  unsigned				shard;
  Key					key;
  ZmFn<void(ZmRef<Object<T>>)>		fn;
  ZmFn<ZmRef<Object<T>>(Table<T> *)>	ctor;
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
  using KeyFields = ZuFields<Key>;
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

  using Fields = Zdb_::Fields<T>;
  using Keys = ZuFieldKeys<T>;
  using KeyIDs = ZuFieldKeyIDs<T>;
  template <int KeyID> using Key = ZuFieldKeyT<T, KeyID>;
  using Tuple = ZuFieldTuple<T>; // same as ZuFieldKeyT<T, ZuFieldKeyID::All>

  ZuAssert(Fields::N < maxFields());
  ZuAssert(KeyIDs::N < maxKeys());

private:
  // - grouping key for a KeyID
  template <unsigned KeyID>
  using GroupKey = typename SplitKey<T, KeyID>::GroupKey;
  // - grouped key for a KeyID
  template <unsigned KeyID>
  using MemberKey = typename SplitKey<T, KeyID>::MemberKey;

public:
  static ZmRef<IOBuf> allocBuf() { return new IOBufAlloc<T>{}; }

  Table(DB *db, TableCf *cf) : AnyTable{db, cf, Table::allocBuf} {
    unsigned n = cf->nShards;
    ZmIDString cacheID = "Zdb.Cache."; cacheID << cf->id;
    ZmIDString bufCacheID = "Zdb.BufCache."; bufCacheID << cf->id;
    m_cache.size(n);
    m_bufCache.size(n);
    for (unsigned i = 0; i < n; i++) {
      new (m_cache.push()) Cache<T>{cacheID};
      new (m_bufCache.push()) BufCache<T>{bufCacheID};
    }
  }
  ~Table() = default;

  // buffer allocator
private:
  // objLoad(fbo) - construct object from flatbuffer (trusted source)
  ZmRef<Object<T>> objLoad(
    const IOBuf *buf, ZmFn<ZmRef<Object<T>>(Table *)> ctor)
  {
    using namespace Zfb::Load;

    auto record = record_(msg_(buf->hdr()));
    if (record->vn() < 0) return {}; // deleted
    auto data = bytes(record->data());
    if (ZuUnlikely(!data)) return {}; // should never happen
    auto fbo = ZfbField::root<T>(&data[0]);
    if (ZuUnlikely(!fbo)) return {}; // should never happen
    ZmRef<Object<T>> object = ctor(this);
    ZfbField::ctor<T>(object->ptr(), fbo);
    object->init(
      record->shard(), record->un(), uint128(record->sn()), record->vn());
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
    unsigned shard = record->shard();
    // mark outdated buffers as stale
    ZuUnroll::all<KeyIDs>([this, shard, fbo](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(*fbo);
      auto i = m_bufCache[shard].template iterator<KeyID>(ZuMv(key));
      while (auto typedBuf = i.iterate()) {
	if (!typedBuf->stale) {
	  typedBuf->stale = true;
	  break;
	}
      }
    });
    // maintain cache consistency
    if (record->vn() >= 0) {
      // primary key is immutable
      if constexpr (KeyIDs::N > 1)
	// no load or eviction here, this is just a key lookup in the cache
	if (ZmRef<Object<T>> object =
	    m_cache[shard].find(ZuFieldKey<0>(*fbo)))
	  m_cache[shard].template update<ZuTypeTail<1, KeyIDs>>(ZuMv(object),
	    [fbo](const ZmRef<Object<T>> &object) {
	      ZfbField::update(object->data(), fbo);
	    });
    } else {
      m_cache[shard].template del<0>(ZuFieldKey<0>(*fbo));
    }
  }

  // objFields() - run-time field array
  ZtVFieldArray objFields() const { return ZtVFields<T>(); }
  // objKeyFields() - run-time key field arrays
  ZtVKeyFieldArray objKeyFields() const { return ZtVKeyFields<T>(); }
  // objSchema() - flatbuffer reflection schema
  const reflection::Schema *objSchema() const {
    return reflection::GetSchema(ZfbSchema<T>::data());
  }

  // objPrint(stream, ptr) - print object
  void objPrint(ZuVStream &s, const void *ptr) const {
    ZtFieldPrint::print(s, *static_cast<const T *>(ptr));
  }
  // objPrintFB(stream, data) - print flatbuffer
  void objPrintFB(ZuVStream &s, ZuBytes data) const {
    auto fbo = ZfbField::verify<T>(data);
    if (!fbo) return;
    s << *fbo;
  }

  // find buffer in buffer cache
  template <unsigned KeyID>
  ZuTuple<ZmRef<const IOBuf>, bool>
  findBuf(unsigned shard, const Key<KeyID> &key) const {
    auto i = m_bufCache[shard].template iterator<KeyID>(key);
    bool found = false;
    while (auto typedBuf = i.iterate()) {
      if (!typedBuf->stale) return {typedBuf->buf, true};
      found = true;
    }
    return {ZmRef<const IOBuf>{}, found};
  }

  // find, falling through object cache, buffer cache, backing data store
  template <
    unsigned KeyID, bool UpdateLRU, bool Evict, typename L, typename Ctor>
  void find_(unsigned shard, Key<KeyID>, L l, Ctor ctor);
  // find from backing data store (retried on failure)
  template <unsigned KeyID, typename L, typename Ctor>
  void retrieve(unsigned shard, Key<KeyID>, L, Ctor);
  template <unsigned KeyID>
  void retrieve_(ZmRef<Find<T, Key<KeyID>>> context);

  // buffer cache
  void cacheBuf_(unsigned shard, ZmRef<const IOBuf> buf) {
    m_bufCache[shard].add(new Buf<T>{ZuMv(buf)});
  }
  ZmRef<const IOBuf> evictBuf_(unsigned shard, IOBuf *buf) {
    if (auto typedBuf = m_bufCache[shard].delNode(
	static_cast<Buf<T> *>(static_cast<Buf_<T> *>(buf->typed))))
      return ZuMv(typedBuf->buf);
    return nullptr;
  }

  // cache statistics
  void cacheStats(unsigned shard, ZmCacheStats &stats) const {
    m_cache[shard].stats(stats);
  }

  // ameliorate cold start
  void warmup(ZmFn<ZmRef<Object<T>>()> ctorFn) {
    AnyTable::warmup();
    unsigned n = config().nShards;
    for (unsigned i = 0; i < n; i++)
      run(i, [this, i, ctorFn]() mutable { warmup_(i, ZuMv(ctorFn)); });
  }
  void warmup() {
    return warmup([](Table *this_) { return new Object<T>{this_}; });
  }
private:
  void warmup_(unsigned shard, ZmFn<ZmRef<Object<T>>()> ctorFn) {
    // warmup heaps
    ZmRef<Object<T>> object = ctorFn(this);
    object->init(shard, 0, 0, 0);
    new (object->ptr()) T{};
    // warmup caches
    m_cache[shard].add(object);
    m_cache[shard].delNode(object);
    // warmup UN cache
    cacheUN(shard, 0, object);
    evictUN(shard, 0);
    // warmup buffer cache
    ZmRef<const IOBuf> buf = object->replicate(int(fbs::Body::Replication));
    cacheBuf(shard, buf);
    evictBuf(shard, 0);
  }

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
  template <unsigned KeyID, typename L, typename Ctor>
  ZuInline void find(unsigned shard, Key<KeyID> key, L l, Ctor ctor) {
    config().cacheMode == CacheMode::All ?
      find_<KeyID, true, false>(shard, ZuMv(key), ZuMv(l), ZuMv(ctor)) :
      find_<KeyID, true, true >(shard, ZuMv(key), ZuMv(l), ZuMv(ctor));
  }
  template <unsigned KeyID, typename L>
  ZuInline void find(unsigned shard, Key<KeyID> key, L l) {
    find<KeyID>(
      shard, ZuMv(key),
      ZuMv(l), [](Table *this_) { return new Object<T>{this_}; });
  }

private: // RMU version used by findUpd() and findDel()
  template <unsigned KeyID, typename L, typename Ctor>
  void findUpd_(unsigned shard, Key<KeyID> key, L l, Ctor ctor) {
    config().cacheMode == CacheMode::All ?
      find_<KeyID, false, false>(shard, ZuMv(key), ZuMv(l), ZuMv(ctor)) :
      find_<KeyID, false, true >(shard, ZuMv(key), ZuMv(l), ZuMv(ctor));
  }

public:
  // evict from cache, even if pinned
  template <unsigned KeyID>
  void evict(unsigned shard, const Key<KeyID> &key) {
    ZmAssert(invoked(shard));

    ZmRef<Object<T>> object = m_cache[shard].template del<KeyID>(key);
    if (object) {
      object->unpin();
      evictUN(shard, object->un());
      object->evict();
    }
  }
  void evict(Object<T> *object) {
    unsigned shard = object->shard();

    ZmAssert(invoked(shard));

    m_cache[shard].delNode(object);
    object->unpin();
    evictUN(shard, object->un());
    object->evict();
  }

  // init lambda - l(ZdbObject<T> *)
private:
  static constexpr auto defltCtor() {
    return [](Table *this_) { return new Object<T>{this_}; };
  }

public:
  // create new object
  template <typename L>
  void insert(unsigned shard, Object<T> *object, L l) {
    ZmAssert(invoked(shard));

    object->insert_(shard, nextUN(shard));
    try {
      l(object);
    } catch (...) { object->abort(); throw; }
    object->abort();
  }
  // create new object (idempotent with UN as key)
  template <typename L>
  void insert(unsigned shard, UN un, ZmRef<Object<T>> object, L l) {
    ZmAssert(invoked(shard));

    if (un != nullUN() && ZuUnlikely(nextUN(shard) > un)) {
      l(nullptr);
      return;
    }
    insert(shard, ZuMv(object), ZuMv(l));
  }

  // update lambda - l(ZdbObject<T> *)
 
  // update object
  template <typename KeyIDs_ = ZuSeq<>, typename L>
  void update(ZmRef<Object<T>> object, L l) {
    unsigned shard = object->shard();

    ZmAssert(invoked(shard));

    if (!update_(object.ptr(), nextUN(object->shard()))) {
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
    ZuUnroll::all<KeyIDs>([this, shard, &object, &bufs, &nBufs](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(object->data());
      auto i = m_bufCache[shard].template iterator<KeyID>(ZuMv(key));
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
      m_cache[shard].template update<KeyIDs_>(object, [
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
    ZmAssert(invoked(object->shard()));

    if (un != nullUN() && ZuUnlikely(nextUN(object->shard()) > un)) {
      l(nullptr);
      return;
    }
    update<KeyIDs_>(ZuMv(object), ZuMv(l));
  }

  // find and update record (with key, without object)
  template <
    unsigned KeyID, typename KeyIDs_ = ZuSeq<>, typename L, typename Ctor>
  ZuInline void findUpd(unsigned shard, Key<KeyID> key, L l, Ctor ctor) {
    findUpd_<KeyID>(shard, ZuMv(key),
      [this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	update<KeyIDs_>(ZuMv(object), ZuMv(l));
      }, ZuMv(ctor));
  }
  template <unsigned KeyID, typename KeyIDs = ZuSeq<>, typename L>
  ZuInline void findUpd(unsigned shard, Key<KeyID> key, L l) {
    findUpd<KeyID, KeyIDs>(shard, ZuMv(key), ZuMv(l), defltCtor());
  }
  // find and update record (idempotent) (with key, without object)
  template <
    unsigned KeyID, typename KeyIDs_ = ZuSeq<>, typename L, typename Ctor>
  ZuInline void findUpd(unsigned shard, Key<KeyID> key, UN un, L l, Ctor ctor) {
    findUpd_<KeyID>(shard, ZuMv(key),
      [this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	update<KeyIDs_>(ZuMv(object), un, ZuMv(l));
      }, ZuMv(ctor));
  }
  template <unsigned KeyID, typename KeyIDs_ = ZuSeq<>, typename L>
  ZuInline void findUpd(unsigned shard, Key<KeyID> key, UN un, L l) {
    findUpd<KeyID, KeyIDs>(shard, ZuMv(key), un, ZuMv(l), defltCtor());
  }

  // delete lambda - l(ZdbObject<T> *)

  // delete record
  template <typename L>
  void del(ZmRef<Object<T>> object, L l) {
    unsigned shard = object->shard();

    ZmAssert(invoked(shard));

    if (!del_(object.ptr(), nextUN(object->shard()))) {
      l(nullptr);
      return;
    }
    // all object keys are being invalidated, need to:
    // - evict from cache
    // - mark pending buffers indexed by the old keys as stale
    // - revert above actions on abort
    // - note that a new buffer is written by commit(), which
    //   causes a future find() to return null
    auto bufs = ZmAlloc(ZmRef<Buf<T>>, KeyIDs::N);	// "undo" buffer
    auto nBufs = 0;
    auto abort = [&object, &bufs, &nBufs]() {
      if (!object->abort()) return;
      for (unsigned i = 0; i < nBufs; i++) {
	bufs[i]->stale = false;
	bufs[i].~ZmRef<Buf<T>>();
      }
    };
    ZuUnroll::all<KeyIDs>([this, shard, &object, &bufs, &nBufs](auto KeyID) {
      auto key = ZuFieldKey<KeyID>(object->data());
      auto i = m_bufCache[shard].template iterator<KeyID>(ZuMv(key));
      while (auto typedBuf = i.iterate()) {
	if (!typedBuf->stale) {
	  typedBuf->stale = true;
	  new (&bufs[nBufs++]) ZmRef<Buf<T>>{ZuMv(typedBuf)};
	  break;
	}
      }
    });
    try {
      l(object);
    } catch (...) { abort(); throw; }
    abort();
  }
  // delete record (idempotent) - returns true if del can proceed
  template <typename L>
  void del(ZmRef<AnyObject> object, UN un, L l) {
    if (un != nullUN() && ZuUnlikely(nextUN(object->shard()) > un)) {
      l(nullptr);
      return;
    }
    del(ZuMv(object), ZuMv(l));
  }

  // find and delete record (with key, without object)
  template <unsigned KeyID, typename L, typename Ctor>
  ZuInline void findDel(
    unsigned shard, const Key<KeyID> &key, L l, Ctor ctor)
  {
    findUpd_<KeyID>(shard, key,
      [this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	del(ZuMv(object), ZuMv(l));
      }, ZuMv(ctor));
  }
  template <unsigned KeyID, typename L>
  ZuInline void findDel(unsigned shard, const Key<KeyID> &key, L l) {
    findDel<KeyID>(shard, key, ZuMv(l), defltCtor());
  }

  // find and delete record (idempotent) (with key, without object)
  template <unsigned KeyID, typename L, typename Ctor>
  ZuInline void findDel(
    unsigned shard, const Key<KeyID> &key, UN un, L l, Ctor ctor)
  {
    findUpd_<KeyID>(shard, key,
      [this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
	if (ZuUnlikely(!object)) { l(object); return; }
	del(ZuMv(object), un, ZuMv(l));
      }, ZuMv(ctor));
  }
  template <unsigned KeyID, typename L>
  ZuInline void findDel(unsigned shard, const Key<KeyID> &key, UN un, L l) {
    findDel<KeyID>(shard, key, un, ZuMv(l), defltCtor());
  }

private:
  // commit insert/update/delete - causes replication/write
  bool commit(AnyObject *object) {
    unsigned shard = object->shard();

    ZmAssert(invoked(shard));

    int origState = object->state();
    if (!object->commit_()) return false;
    switch (origState) {
      case ObjState::Insert:
	if (writeCache()) {
	  m_cache[shard].add(object, [this](AnyObject *object) {
	    if (object->pinned()) return false;
	    evictUN(object->shard(), object->un());
	    object->evict();
	    return true;
	  });
	  cacheUN(shard, object->un(), object);
	}
	incCount();
	break;
      case ObjState::Update:
	// evictUN() already called from update_()
	if (writeCache())
	  cacheUN(shard, object->un(), object);
	break;
      case ObjState::Delete:
	// evictUN() already called from del_()
	if (m_cache[shard].delNode(static_cast<Object<T> *>(object)))
	  object->evict();
	decCount();
	break;
    }
    write(shard, object->replicate(int(fbs::Body::Replication)), true);
    return true;
  }

  // abort insert/update/delete
  bool abort(AnyObject *object) {
    ZmAssert(invoked(object->shard()));

    return object->abort_();
  }

  // low-level update, calls AnyObject::update_()
  bool update_(Object<T> *object, UN un) {
    evictUN(object->shard(), object->un());
    return object->update_(un);
  }

  // low-level delete, calls AnyObject::del_()
  bool del_(Object<T> *object, UN un) {
    evictUN(object->shard(), object->un());
    return object->del_(un);
  }

private:
  // object caches
  using CacheArray = ZtArray<Cache<T>>;
  CacheArray			m_cache;

  // pending replications
  using BufCacheArray = ZtArray<BufCache<T>>;
  BufCacheArray			m_bufCache;
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

inline constexpr const char *Tables_HeapID() { return "Zdb.Table"; }
using Tables =
  ZmRBTree<ZmRef<AnyTable>,
    ZmRBTreeKey<AnyTable::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<Tables_HeapID>>>>;

// --- DB host configuration

struct HostCf {
  ZuID		id;
  int		priority = 0;	// -1 is used internally for a failed host
  ZiIP		ip;
  uint16_t	port = 0;
  bool		standalone = false;
  ZtString	up;
  ZtString	down;

  HostCf(const ZtString &id_) : id{id_}, standalone{true} { }
  HostCf(const ZtString &id_, const ZvCf *cf) : id{id_} {
    if (!(standalone = cf->getBool("standalone", false))) {
      priority = cf->getInt<true>("priority", 0, 1<<30);
      ip = cf->get<true>("ip");
      port = cf->getInt<true>("port", 1, (1<<16) - 1);
    }
    up = cf->get("up");
    down = cf->get("down");
  }

  static ZuID IDAxor(const HostCf &cfg) { return cfg.id; }
};

inline constexpr const char *HostCfs_HeapID() { return "Zdb.HostCf"; }
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
  int priority() const { return m_cf->priority; }
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
  static ZuTuple<int, ZuID> IndexAxor(const Host &h) {
    return ZuFwdTuple(h.priority(), h.id());
  }

  Zfb::Offset<void> telemetry(Zfb::Builder &fbb, bool update) const;

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
inline constexpr const char *Hosts_HeapID() { return "Zdb.Host"; }
using Hosts =
  ZmHash<HostIndex::Node,
    ZmHashNode<HostIndex::Node,
      ZmHashKey<Host::IDAxor,
	ZmHashHeapID<Hosts_HeapID>>>>;

// --- DB handler functions

// UpFn() - activate
typedef void (*UpFn)(DB *, Host *); // db, oldMaster
// DownFn() - de-activate
typedef void (*DownFn)(DB *, bool); // db, failed

struct DBHandler {
  UpFn		upFn = [](DB *, Host *) { };
  DownFn	downFn = [](DB *, bool failed) { };
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
#if Zdb_DEBUG
    debug = cf->getBool("debug");
#endif
  }
  DBCf(DBCf &&) = default;
  DBCf &operator =(DBCf &&) = default;

  const TableCf *tableCf(ZuString id) const {
    if (auto node = tableCfs.findPtr(id)) return &node->val();
    return nullptr;
  }
  TableCf *tableCf(ZuString id) {
    auto node = tableCfs.findPtr(id);
    if (!node) tableCfs.addNode(node = new TableCfs::Node{id});
    return &node->val();
  }

  const HostCf *hostCf(ZuString id) const {
    if (auto node = hostCfs.findPtr(id)) return &node->val();
    return nullptr;
  }
  HostCf *hostCf(ZuString id) {
    auto node = hostCfs.findPtr(id);
    if (!node) hostCfs.addNode(node = new HostCfs::Node{id});
    return &node->val();
  }
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
  auto sid() const { return config().sid; }

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

  // trigger storage failure - intentionally deactivate
  void fail();

  // find table
  ZmRef<AnyTable> table(ZuID id) {
    ZmAssert(invoked());

    return m_tables.findVal(id);
  }

  using AllFn = ZmFn<void(AnyTable *, ZmFn<void(bool)>)>;
  using AllDoneFn = ZmFn<void(DB *, bool)>;

  void all(AllFn fn, AllDoneFn doneFn = AllDoneFn{});

  Zfb::Offset<void> telemetry(Zfb::Builder &fbb, bool update) const;

private:
  void storeFailed(ZeVEvent e) {
    ZeLOG(Fatal, ZuMv(e));
    run([this]() { fail(); });
  }

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
  void deactivate(bool failed);	// become follower
  void reactivate(Host *host);	// re-assert leader

  void up_(Host *oldMaster);	// run up command
  void down_(bool failed);	// run down command

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
  void replicated(Host *host, ZuID tblID, unsigned shard, UN un, SN sn);

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

  auto context = ZmMkRef(new Context{ZuMv(l)});

  using Key = GroupKey<KeyID>;

  Zfb::IOBuilder fbb{allocBuf()};
  fbb.Finish(
    ZfbField::SaveFieldsFn<Key, ZuFields<Key>>::save(fbb, key).Union());
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

  auto context = ZmMkRef(new Context{ZuMv(l)});

  Zfb::IOBuilder fbb{allocBuf()};
  fbb.Finish(
    ZfbField::SaveFieldsFn<SelectKey, ZuFields<SelectKey>>::save(
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
template <
  unsigned KeyID, bool UpdateLRU, bool Evict, typename L, typename Ctor>
inline void Table<T>::find_(unsigned shard, Key<KeyID> key, L l, Ctor ctor) {
  ZmAssert(invoked(shard));

  auto load = [
    this, shard, ctor = ZuMv(ctor)
  ]<typename L_>(const Key<KeyID> &key, L_ l) mutable {
    auto [buf, found] = findBuf<KeyID>(shard, key);
    if (buf) {
      l(objLoad(buf, ZuMv(ctor)));
      return;
    }
    if (found) {
      l(nullptr);
      return;
    }
    retrieve<KeyID>(shard, key, ZuMv(l), ZuMv(ctor));
  };
  if constexpr (Evict) {
    m_cache[shard].template find<KeyID, UpdateLRU>(
      ZuMv(key), ZuMv(l), ZuMv(load),
      [this](AnyObject *object) {
	if (object->pinned()) return false;
	evictUN(object->shard(), object->un());
	object->evict();
	return true;
      });
  } else
    m_cache[shard].template find<KeyID, UpdateLRU, false>(
	ZuMv(key), ZuMv(l), ZuMv(load));
}

template <typename T>
template <unsigned KeyID, typename L, typename Ctor>
inline void Table<T>::retrieve(unsigned shard, Key<KeyID> key, L l, Ctor ctor)
{
  using Key_ = Key<KeyID>;
  using Context = Find<T, Key_>;

  auto context =
    ZmMkRef(new Context{this, shard, ZuMv(key), ZuMv(l), ZuMv(ctor)});

  retrieve_<KeyID>(ZuMv(context));
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

  storeTbl()->find(KeyID, ZuMv(keyBuf).constRef(), RowFn::mvFn(ZuMv(context),
    [](ZmRef<Context> context, RowResult result) {
      auto table = context->table;
      if (ZuUnlikely(result.is<Event>())) {
	ZeLogEvent(ZuMv(result).p<Event>());
	auto db = context->table->db();
	ZeLOG(Fatal, ([context = ZuMv(context)](auto &s) {
	  s << "Zdb find of " << context->table->id()
	    << '/' << context->key << " failed";
	}));
	db->run([db]() { db->fail(); }); // trigger failover
	return;
      }
      if (ZuLikely(result.is<RowData>())) {
	auto buf = ZuMv(ZuMv(result).p<RowData>().buf);
	auto shard = context->shard;
	table->run(shard, [
	  table,
	  context = ZuMv(context),
	  buf = ZuMv(buf)
	]() mutable {
	  ZmRef<Object<T>> object =
	    table->objLoad(ZuMv(buf), ZuMv(context->ctor));
	  if (object->shard() != context->shard) {
	    auto fn = ZuMv(context->fn);
	    // sharding inconsistency is fatal, the app is broken
	    ZeLOG(Fatal, ([
	      context = ZuMv(context), object = ZuMv(object)
	    ](auto &s) {
	      s << "Zdb find of " << context->table->id()
		<< '/' << context->key << " failed: object " << *object
		<< " shard != find context shard " << context->shard;
	    }));
	    fn(nullptr);
	  } else
	    context->fn(ZuMv(object));
	});
      } else
	table->run(context->shard, [fn = ZuMv(context->fn)]() mutable {
	  fn(nullptr);
	});
    }));
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
      << " shard=" << ZuBoxed(record->shard())
      << " un=" << record->un()
      << " sn=" << ZuBoxed(Zfb::Load::uint128(record->sn()))
      << " vn=" << record->vn() << "}";
    if (data) {
      s << " data=";
      if (table) {
	ZuVStream s_{s};
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
    << " shard=" << ZuBoxed(m_shard)
    << " un=" << m_un
    << " sn=" << m_sn
    << " vn=" << m_vn;
  if (m_origUN != nullUN()) s << " origUN=" << m_origUN;
  s << " data=";
  {
    ZuVStream s_{s};
    m_table->objPrint(s_, ptr_());
  }
  s << '}';
}

} // Zdb_

// external API

using ZdbAnyObject = Zdb_::AnyObject;
template <typename T> using ZdbObject = Zdb_::Object<T>;
template <typename T> using ZdbObjRef = ZmRef<ZdbObject<T>>;

using ZdbAnyTable = Zdb_::AnyTable;
template <typename T> using ZdbTable = Zdb_::Table<T>;
using ZdbTableCf = Zdb_::TableCf;
template <typename T> using ZdbTblRef = ZmRef<ZdbTable<T>>;

using Zdb = Zdb_::DB;
using ZdbHandler = Zdb_::DBHandler;
using ZdbCf = Zdb_::DBCf;

using ZdbUpFn = Zdb_::UpFn;
using ZdbDownFn = Zdb_::DownFn;
using ZdbHandler = Zdb_::DBHandler;

using ZdbHost = Zdb_::Host;

#endif /* Zdb_HH */
