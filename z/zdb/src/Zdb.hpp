//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Z Database

#ifndef Zdb_HPP
#define Zdb_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuInt.hpp>
#include <zlib/ZuBitmap.hpp>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmFn.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmEngine.hpp>
#include <zlib/ZmCache.hpp>
#include <zlib/ZmPLock.hpp>

#include <zlib/ZtString.hpp>
#include <zlib/ZtEnum.hpp>

#include <zlib/ZePlatform.hpp>
#include <zlib/ZeLog.hpp>

#include <zlib/ZiFile.hpp>
#include <zlib/ZiMultiplex.hpp>
#include <zlib/ZiIOBuf.hpp>
#include <zlib/ZiRx.hpp>
#include <zlib/ZiTx.hpp>

#include <zlib/Zfb.hpp>
#include <zlib/ZfbField.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvTelemetry.hpp>
#include <zlib/ZvTelServer.hpp>

#include <zlib/ZdbTypes.hpp>
#include <zlib/ZdbMsg.hpp>
#include <zlib/ZdbBuf.hpp>
#include <zlib/ZdbStore.hpp>

// Zdb is a clustered/replicated in-process/in-memory DB that
// includes leader election and failover. Zdb dynamically organizes
// cluster hosts into a replication chain from the leader to the
// lowest-priority follower. Replication is async. ZmEngine is used for
// start/stop state management. Zdb applications are stateful back-end
// services that defer to Zdb for activation/deactivation.
// Restart/recovery is from back-end data store, then from the cluster
// leader (if the local host is not itself elected leader).

//  host state		engine state
//  ==========		============
//  Instantiated	Stopped
//  Initialized		Stopped
//  Electing		!Stopped
//  Active		!Stopped
//  Inactive		!Stopped
//  Stopping		Stopping | StartPending

#if defined(ZDEBUG) && !defined(ZdbRep_DEBUG)
#define ZdbRep_DEBUG
#endif

#ifdef ZdbRep_DEBUG
#define ZdbDEBUG(env, e) \
  do { if ((env)->debug()) ZeLOG(Debug, (e)); } while (0)
#else
#define ZdbDEBUG(env, e) (void())
#endif

#include <zlib/telemetry_fbs.h>

namespace Zdb_ {

// --- pre-declarations

class DB;	// database
class Env;	// database environment
class Host;	// cluster host
class Cxn_;	// network connection

// --- main replication connection class

class Cxn_ :
    public ZiConnection,
    public ZiRx<Cxn_, Buf>,
    public ZiTx<Cxn_, Buf> {
friend Env;
friend Host;
friend DB;

  using Buf = Zdb_::Buf; // de-conflict with ZiConnection

  using Rx = ZiRx<Cxn_, Buf>;
  using Tx = ZiTx<Cxn_, Buf>;

  using Rx::recv; // de-conflict with ZiConnection
  using Tx::send; // ''

protected:
  Cxn_(Env *env, Host *host, const ZiCxnInfo &ci);

private:
  Env *env() const { return m_env; }
  void host(Host *host) { m_host = host; }
  Host *host() const { return m_host; }

  void connected(ZiIOContext &);
  void disconnected();

  void msgRead(ZiIOContext &);
  int msgRead2(ZmRef<Buf>);
  void msgRead3(ZmRef<Buf>);

  void hbRcvd(const fbs::Heartbeat *);
  void hbTimeout();
  void hbSend();

  void repRecordRcvd(ZmRef<Buf>);
  void repCommitRcvd(ZmRef<Buf>);

  Env			*m_env;
  Host			*m_host;	// nullptr if not yet associated

  ZmScheduler::Timer	m_hbTimer;
};
inline constexpr const char *CxnHeapID() { return "Zdb.Cxn"; }
using CxnList =
  ZmList<Cxn_,
    ZmListNode<Cxn_,
      ZmListHeapID<CxnHeapID>>>;
using Cxn = CxnList::Node;

// --- DB environment state (key/value linear hash from DBID -> RN)

using EnvState_ = ZmLHashKV<ZuID, UN, ZmLHashLocal<>>;
struct EnvState : public EnvState_ {
  SN		sn = 0;

  EnvState() = delete;

  EnvState(unsigned size) : EnvState_{ZmHashParams{size}} { }

  EnvState(const fbs::EnvState *envState) :
      EnvState_{ZmHashParams{envState->dbStates()->size()}},
      sn{Zfb::Load::uint128(envState->sn())} {
    using namespace Zdb_;
    using namespace Zfb::Load;

    all(envState->dbStates(), [this](unsigned, const fbs::DBState *dbState) {
      add(id(&(dbState->db())), dbState->un());
    });
  }
  void load(const fbs::EnvState *envState) {
    using namespace Zdb_;
    using namespace Zfb::Load;

    sn = uint128(envState->sn());
    all(envState->dbStates(), [this](unsigned, const fbs::DBState *dbState) {
      update(id(&(dbState->db())), dbState->un());
    });
  }
  Zfb::Offset<fbs::EnvState> save(Zfb::Builder &fbb) const {
    using namespace Zdb_;
    using namespace Zfb::Save;

    auto sn_ = uint128(sn);
    auto i = readIterator();
    return fbs::CreateEnvState(fbb, &sn_, structVecIter<fbs::DBState>(
	fbb, i.count(), [&i](fbs::DBState *ptr, unsigned) {
      if (auto state = i.iterate())
	new (ptr)
	  fbs::DBState{id(state->template p<0>()), state->template p<1>()};
      else
	new (ptr) fbs::DBState{}; // unused
    }));
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
  EnvState &operator |=(const EnvState &r) {
    if (ZuLikely(this != &r)) {
      updateSN(r.sn);
      auto i = r.readIterator();
      while (auto rstate = i.iterate())
	update(rstate->template p<0>(), rstate->template p<1>());
    }
    return *this;
  }
  EnvState &operator =(const EnvState &r) {
    if (ZuLikely(this != &r)) {
      clean();
      this->operator |=(r);
    }
    return *this;
  }

  int cmp(const EnvState &r) const {
    return (sn > r.sn) - (sn < r.sn);
  }

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(EnvState *);
};
template <typename S>
inline void EnvState::print(S &s) const {
  s << "{sn=" << ZuBoxed(sn) << ", dbs={";
  unsigned n = count_();
  if (ZuLikely(n)) {
    unsigned j = 0;
    auto i = readIterator();
    while (auto state = i.iterate()) {
      if (j++) s << ',';
      s << '{'
	<< state->template p<0>() << ", "
	<< ZuBoxed(state->template p<1>()) << '}';
    }
  }
  s << "}}";
}

// --- replication message printing

namespace HostState {
  using namespace ZvTelemetry::ZdbHostState;
}

struct Record_Print {
  const fbs::Record *record = nullptr;
  template <typename S> void print(S &s) const {
    auto id = Zfb::Load::id(record->db());
    auto data = Zfb::Load::bytes(record->data());
    s << "record{db=" << id
      << " rn=" << record->rn()
      << " un=" << record->un()
      << " sn=" << ZuBoxed(Zfb::Load::uint128(record->sn()))
      << " vn=" << record->vn() << "}";
    if (data) s << ZtHexDump("\n", data.data(), data.length());
  }
  friend ZuPrintFn ZuPrintType(Record_Print *);
};

struct HB_Print {
  const fbs::Heartbeat *hb = nullptr;
  template <typename S> void print(S &s) const {
    auto id = Zfb::Load::id(hb->host());
    s << "heartbeat{host=" << id
      << " state=" << HostState::name(hb->state())
      << " envState=" << EnvState{hb->envState()} << "}";
  }
  friend ZuPrintFn ZuPrintType(HB_Print *);
};

template <typename S>
inline void Buf::print(S &s) const {
  auto msg = Zdb_::msg(ptr<Hdr>());
  if (!msg) { s << "corrupt{}"; return; }
  if (auto record = Zdb_::record(msg)) { s << Record_Print{record}; return; }
  if (auto hb = Zdb_::hb(msg)) { s << HB_Print{hb}; return; }
  s << "unknown{}";
}

// --- DB generic object

namespace ObjState {
  ZtEnumValues(ObjState,
      Undefined = 0,
      Push,
      Update,
      Committed,
      Delete,
      Deleted);
}

const char *Object_HeapID() { return "Zdb.Object"; }

// possible state paths:
//
// Undefined > Push			push
// Push > Committed			push committed
// Push > Undefined			push aborted
// Committed > Update > Committed	update committed or aborted
// Committed > Delete > Deleted		delete committed
// Committed > Delete > Committed	delete aborted
//
// path forks:
//
// Push   > (Committed|Undefined)
// Delete > (Deleted|Committed)
//
// possible event sequences:
//
// push, put
// push, abort
// update, put
// update, abort
// del, put
// del, abort
//
// events and state transitions:
//
// push		Undefined > Push
// put		Push > Committed
// abort	Push > Undefined
// update	Committed > Update
// put		Update > Committed
// abort	Update > Committed
// del		Committed > Delete
// put		Delete > Deleted
// abort	Delete > Committed

class ZdbAPI AnyObject_ : public ZmPolymorph {
  AnyObject_() = delete;
  AnyObject_(const AnyObject_ &) = delete;
  AnyObject_ &operator =(const AnyObject_ &) = delete;
  AnyObject_(AnyObject_ &&) = delete;
  AnyObject_ &operator =(AnyObject_ &&) = delete;

  friend DB;

public:
  AnyObject_(DB *db) : m_db{db} { }

  DB *db() const { return m_db; }
  RN rn() const { return m_rn; }
  UN un() const { return m_un; }
  SN sn() const { return m_sn; }
  VN vn() const { return m_vn; }
  int state() const { return m_state; }
  UN origUN() const { return m_origUN; }

  ZmRef<Buf> replicate(int type);

  virtual void *ptr_() { return nullptr; }
  const void *ptr_() const { return const_cast<AnyObject_ *>(this)->ptr_(); }

  template <typename S> void print(S &s) const {
    using namespace Zdb_;
    s << " rn=" << m_rn;
  }
  friend ZuPrintFn ZuPrintType(AnyObject_ *);

  static RN RNAxor(const AnyObject_ &object) { return object.rn(); }

public:
  void put();
  void abort();

private:
  void init(RN rn, UN un, SN sn, VN vn) {
    m_rn = rn;
    m_un = un;
    m_sn = sn;
    m_vn = vn;
    m_state = ObjState::Committed;
  }

  bool push_(RN rn, UN un);
  bool update_(UN un);
  bool del_(UN un);
  bool put_();
  bool abort_();

  DB		*m_db;
  RN		m_rn = nullRN();
  UN		m_un = nullUN();
  SN		m_sn = nullSN();
  VN		m_vn = 0;
  int		m_state = ObjState::Undefined;
  UN		m_origUN = nullUN();
};
using Cache =
  ZmCache<AnyObject_,
    ZmCacheNode<AnyObject_,
      ZmCacheKey<AnyObject_::RNAxor,
	ZmCacheLock<ZmPLock,
	  ZmCacheHeapID<ZmHeapDisable()>>>>>;
using AnyObject = Cache::Node;
inline UN AnyObject_UNAxor(const ZmRef<AnyObject> &object) {
  return object->un();
}
// temporarily there may be more than one UN referencing a cached object
using CacheUN =
  ZmHashKV<UN, ZmRef<AnyObject>,
    ZmHashLock<ZmPLock,
      ZmHashHeapID<Object_HeapID>>>;

// --- DB type-specific object

template <typename T_, typename Heap>
class Object_ : public Heap, public AnyObject {
  Object_() = delete;
  Object_(const Object_ &) = delete;
  Object_ &operator =(const Object_ &) = delete;
  Object_(Object_ &&) = delete;
  Object_ &operator =(Object_ &&) = delete;

public:
  using T = T_;

  Object_(DB *db_) : AnyObject{db_} { }

  template <typename L>
  Object_(DB *db_, L l) : AnyObject{db_} {
    l(static_cast<void *>(&m_data[0]));
  }

  void *ptr_() { return &m_data[0]; }
  const void *ptr_() const { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~Object_() { ptr()->~T(); }

  const T &data() const & { return *ptr(); }
  T &data() & { return *ptr(); }
  T &&data() && { return ZuMv(*ptr()); }

  template <typename S> void print(S &s) const {
    AnyObject::print(s);
    s << ' ' << this->data();
  }

private:
  uint8_t	m_data[sizeof(T)];
};
template <typename T>
using Object = Object_<T, ZmHeap<Object_HeapID, sizeof(Object_<T, ZuNull>)>>;

// --- DB application handler functions

// CtorFn(db) - construct new object
typedef AnyObject *(*CtorFn)(DB *);
// LoadFn(db, data, length) - reconstruct object from flatbuffer
typedef AnyObject *(*LoadFn)(DB *, const uint8_t *, unsigned);
// UpdateFn(object, data, length) - update object from flatbuffer
typedef AnyObject *(*UpdateFn)(AnyObject *, const uint8_t *, unsigned);
// SaveFn(fbb, ptr) - save object into flatbuffer builder, return offset
typedef Zfb::Offset<void> (*SaveFn)(Zfb::Builder &, const void *);
// FieldsFn() - inform fields
typedef ZtMFieldArray (*FieldsFn)();
// ImportFn(db, import) - import object from data store
typedef AnyObject *(*ImportFn)(DB *, const ZtField::Import &);
// Note: ExportFn forms part of the Table interface in ZdbStore.hpp
// ExportFn(ptr, export) - export object to data store
// ScanFn(object) - object scanned
typedef void (*ScanFn)(AnyObject *);
// DeleteFn(RN) - object deleted
typedef void (*DeleteFn)(RN);

struct DBHandler {
  CtorFn	ctorFn = nullptr;
  LoadFn	loadFn = nullptr;
  UpdateFn	updateFn = nullptr;
  SaveFn	saveFn = nullptr;
  FieldsFn	fieldsFn = nullptr;
  ImportFn	importFn = nullptr;
  ExportFn	exportFn = nullptr;
  ExportFn	exportUpdateFn = nullptr;
  ScanFn	scanFn = nullptr;
  DeleteFn	deleteFn = nullptr;

  template <typename T>
  static DBHandler bind(
      ScanFn scanFn_ = nullptr,
      DeleteFn deleteFn_ = nullptr) {
    return DBHandler{
      .ctorFn = [](DB *db) -> AnyObject * {
	return new Object<T>{db};
      },
      .loadFn = [](DB *db, const uint8_t *data, unsigned len) ->
	  AnyObject * {
	auto fbo = ZfbField::verify<T>(data, len);
	if (ZuUnlikely(!fbo)) return nullptr;
	return new Object<T>{db, [fbo](void *ptr) {
	  ZfbField::ctor<T>(ptr, fbo);
	}};
      },
      .updateFn = [](AnyObject *object, const uint8_t *data_, unsigned len) ->
	  AnyObject * {
	auto fbo = ZfbField::verify<T>(data_, len);
	if (ZuUnlikely(!fbo)) return nullptr;
	auto &data = static_cast<Object<T> *>(object)->data();
	ZfbField::load<T>(data, fbo);
	return object;
      },
      .saveFn = [](Zfb::Builder &fbb, const void *ptr) -> Zfb::Offset<void> {
	return ZfbField::save<T>(fbb, *static_cast<const T *>(ptr)).Union();
      },
      .fieldsFn = []() -> ZtMFieldArray {
	return ZtMFields<T>();
      },
      .importFn = [](DB *db, const ZtField::Import &import_) -> AnyObject * {
	return new Object<T>{db, [&import_](void *ptr) {
	  ZtField::ctor<T>(ptr, import_);
	}};
      },
      .exportFn = [](const void *ptr, const ZtField::Export &export_) {
	ZtField::save<T>(*static_cast<const T *>(ptr), export_);
      },
      .exportUpdateFn = [](const void *ptr, const ZtField::Export &export_) {
	ZtField::saveUpdate<T>(*static_cast<const T *>(ptr), export_);
      },
      .scanFn = ZuMv(scanFn_),
      .deleteFn = ZuMv(deleteFn_)
    };
  }
};

// --- DB configuration

namespace ZdbCacheMode {
  using namespace ZvTelemetry::ZdbCacheMode;
}

struct DBCf {
  ZuID			id;
  ZmThreadName		thread;		// in-memory thread
  ZmThreadName		writeThread;	// data store write thread
  mutable unsigned	sid = 0;	// in-memory thread slot ID
  mutable unsigned	writeSID = 0;	// data store write thread slot ID
  int			cacheMode = ZdbCacheMode::Normal;
  bool			warmup = false;	// warm-up back-end

  DBCf() = default;
  DBCf(ZuString id_) : id{id_} { }
  DBCf(ZuString id_, const ZvCf *cf) : id{id_} {
    thread = cf->get("thread");
    writeThread = cf->get("writeThread");
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", ZdbCacheMode::Normal);
    warmup = cf->getBool("warmup");
  }

  static ZuID IDAxor(const DBCf &cf) { return cf.id; }
};

// --- DB configurations

inline constexpr const char *DBCfs_HeapID() { return "ZdbEnv.DBCfs"; }
using DBCfs =
  ZmRBTree<DBCf,
    ZmRBTreeKey<DBCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<DBCfs_HeapID>>>>;

// --- main DB class

class ZdbAPI DB : public ZmPolymorph {
friend Cxn_;
friend AnyObject_;
friend Env;

protected:
  DB(Env *env, DBCf *cf);

public:
  ~DB();

private:
  void init(DBHandler);
  void final();

  template <typename L>
  void open(Store *store, L l);
  bool opened(Store_::OpenResult result);
  void close();

public:
  Env *env() const { return m_env; }
  ZiMultiplex *mx() const { return m_mx; }
  const DBCf &config() const { return *m_cf; }

  static ZuID IDAxor(const DB &db) { return db.config().id; }

  ZuID id() const { return config().id; }
  ZtMFieldArray fields() const { return m_handler.fieldsFn(); }

  // DB thread (may be shared)
  template <typename ...Args>
  void run(Args &&... args) const {
    m_mx->run(m_cf->sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&... args) const {
    m_mx->invoke(m_cf->sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_cf->sid); }

  // back-end data store write thread (may be shared)
  template <typename ...Args>
  void writeRun(Args &&... args) const {
    m_mx->run(m_cf->writeSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void writeInvoke(Args &&... args) const {
    m_mx->invoke(m_cf->writeSID, ZuFwd<Args>(args)...);
  }
  bool writeInvoked() const { return m_mx->invoked(m_cf->writeSID); }

private:
  ZmRef<Buf> findBuf(RN rn) { return {m_repBufs->find(rn)}; }
  ZmRef<Buf> findBufUN(UN un) { return {m_repBufsUN->findVal(un)}; }

  // get falling through cache, replication buffers, back-end data store
  template <typename T, bool UpdateLRU, bool Evict, typename L>
  void get_(RN rn, L l);
  // fallback get from back-end data store
  template <typename T>
  void get__(RN rn, ZmFn<ZmRef<Object<T>>>);

public:
  // get lambda - l(ZmRef<ZdbObject<T>>)

  template <typename T, typename L>
  void get(RN rn, L l) {
    config().cacheMode == ZdbCacheMode::All ?
      get_<T, true, false>(rn, ZuMv(l)) :
      get_<T, true, true >(rn, ZuMv(l));
  }

  // get (RMU version) - do not update LRU (yet)
  template <typename T, typename L>
  void getUpdate(RN rn, L l) {
    config().cacheMode == ZdbCacheMode::All ?
      get_<T, false, false>(rn, ZuMv(l)) :
      get_<T, false, true >(rn, ZuMv(l));
  }

public:
  // next RN that will be allocated
  RN nextRN() const { return m_nextRN; }
  // next UN that will be allocated
  RN nextUN() const { return m_nextUN; }

  // create placeholder record (null RN, in-memory, never persisted/replicated)
  ZmRef<AnyObject> placeholder();

  // enable/disable writing to cache (temporarily)
  void writeCache(bool enabled) { m_writeCache = enabled; }

  // all transactions begin with a push(), update() or del(),
  // and complete with a object->put() or object->abort():
  // put() commits the operation
  // abort() aborts it

private:
  ZmRef<AnyObject> push_(RN rn, UN un);
public:
  // push lambda - l(const ZmRef<ZdbObject<T>> &)

  // create new record
  template <typename T, typename L> void push(L l) {
    ZmAssert(invoked());

    ZmRef<Object<T>> object = push_(m_nextRN, m_nextUN);
    if (!object) { l(object); return; }
    try {
      l(object);
    } catch (...) { object->abort(); throw; }
    object->abort();
  }
  // create new record (idempotent with RN as key)
  template <typename T, typename L> void pushRN(RN rn, L l) {
    if (rn != nullRN() && ZuUnlikely(m_nextRN > rn)) {
      l(static_cast<Object<T> *>(nullptr));
      return;
    }
    push<T>(ZuMv(l));
  }
  // create new record (idempotent with UN as key)
  template <typename T, typename L> void pushUN(UN un, L l) {
    if (un != nullUN() && ZuUnlikely(m_nextUN > un)) {
      l(static_cast<Object<T> *>(nullptr));
      return;
    }
    push<T>(ZuMv(l));
  }

private:
  bool update_(AnyObject *object, UN un);
public:
  // update lambda - l(const ZmRef<ZdbObject<T>> &)
 
  // update record
  template <typename T, typename L>
  void update(ZmRef<Object<T>> object, L l) {
    ZmAssert(invoked());

    if (!update_(object.ptr(), m_nextUN)) { l(ZmRef<Object<T>>{}); return; }
    try {
      l(object);
    } catch (...) { object->abort(); throw; }
    object->abort();
  }
  // update record (idempotent) - returns true if update can proceed
  template <typename T, typename L>
  void update(ZmRef<Object<T>> object, UN un, L l) {
    if (un != nullUN() && ZuUnlikely(m_nextUN > un)) {
      l(ZmRef<Object<T>>{});
      return;
    }
    update<T>(ZuMv(object), ZuMv(l));
  }

  // update record (with RN, without object)
  template <typename T, typename L> void update(RN rn, L l) {
    getUpdate(rn, [this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
      if (ZuUnlikely(!object)) { l(object); return; }
      update<T>(ZuMv(object), ZuMv(l));
    });
  }
  // update record (idempotent) (with RN, without object)
  template <typename T, typename L> void update(RN rn, UN un, L l) {
    getUpdate(rn, [this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
      if (ZuUnlikely(!object)) { l(object); return; }
      update<T>(ZuMv(object), un, ZuMv(l));
    });
  }

private:
  bool del_(AnyObject *object, UN un);
public:
  // del lambda - l(const ZmRef<ZdbObject<T>> &)

  // del record
  template <typename T, typename L>
  void del(ZmRef<Object<T>> object, L l) {
    ZmAssert(invoked());

    if (!del_(object.ptr(), m_nextUN)) { l(ZmRef<Object<T>>{}); return; }
    try { l(object); } catch (...) { object->abort(); throw; }
    object->abort();
  }
  // del record (idempotent) - returns true if del can proceed
  template <typename T, typename L>
  void del(ZmRef<AnyObject> object, UN un, L l) {
    if (un != nullUN() && ZuUnlikely(m_nextUN > un)) {
      l(ZmRef<Object<T>>{});
      return;
    }
    del(ZuMv(object), ZuMv(l));
  }

  // del record (with RN, without object)
  template <typename T, typename L> void del(RN rn, L l) {
    getUpdate(rn, [this, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
      if (ZuUnlikely(!object)) { l(object); return; }
      del(ZuMv(object), ZuMv(l));
    });
  }
  // del record (idempotent) (with RN, without object)
  template <typename T, typename L> void del(RN rn, UN un, L l) {
    getUpdate(rn, [this, un, l = ZuMv(l)](ZmRef<Object<T>> object) mutable {
      if (ZuUnlikely(!object)) { l(object); return; }
      del(ZuMv(object), un, ZuMv(l));
    });
  }

private:
  // commit push/update/delete - causes replication/write
  bool put(AnyObject *object);

  // abort push/update/delete
  bool abort(AnyObject *);

private:
  Zfb::Offset<ZvTelemetry::fbs::Zdb>
  telemetry(ZvTelemetry::IOBuilder &, bool update) const;

  // load object from buffer, bypassing cache
  ZmRef<AnyObject> load_(const fbs::Record *record);
  // load object from buffer, updating cache
  ZmRef<AnyObject> load(const fbs::Record *record);
  // save object to buffer
  Zfb::Offset<void> save(Zfb::Builder &fbb, AnyObject_ *object);

  // outbound replication / write to data store
  void write(ZmRef<Buf> buf);
  // low-level write to data store - write thread
  void commit(ZmRef<Buf> buf);
  // data store committed
  void committed(UN un, Table_::CommitResult &);
  // evict replication buffer
  void evictRepBuf(UN un);

  // outbound recovery / replication
  void recSend(ZmRef<Cxn> cxn, RN rn, RN endRN);
  void recSendGet(ZmRef<Cxn> cxn, RN rn, RN endRN);
  void recSend_(ZmRef<Cxn> cxn, RN rn, RN endRN, ZmRef<Buf> buf);
  void recNext(ZmRef<Cxn> cxn, RN rn, RN endRN);
  ZmRef<Buf> repBuf(UN un);
  void repSendCommit(UN un);

  // inbound replication
  void repRecordRcvd(ZmRef<Buf> buf);
  void repCommitRcvd(UN un);

  // recovery - DB thread
  void recover(const fbs::Record *record);

  // RN
  bool allocRN(RN rn) {
    if (ZuUnlikely(rn != m_nextRN)) return false;
    ++m_nextRN;
    return true;
  }
  void recoveredRN(RN rn) {
    if (ZuUnlikely(rn == nullRN())) return;
    if (m_nextRN <= rn) m_nextRN = rn + 1;
  }

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
  Env			*m_env;
  ZiMultiplex		*m_mx;
  const DBCf		*m_cf;
  DBHandler		m_handler;

  // DB thread exclusive - no need for atomics
  RN			m_nextRN = 0;		 // RN allocator
  UN			m_nextUN = 0;		 // UN allocator

  // open/closed state
  ZmAtomic<unsigned>	m_open = 0;		// DB thread SWMR

  // back-end data store table
  Table			*m_table = nullptr;	// DB thread

  // object cache
  bool			m_writeCache = true;
  Cache			m_cache;		// object cache indexed by RN
  ZmRef<CacheUN>	m_cacheUN;		// '' indexed by UN

  // pending replications
  ZmRef<RepBufs>	m_repBufs;		// buffers indexed by RN
  ZmRef<RepBufsUN>	m_repBufsUN;		// '' indexed by UN
};

inline void AnyObject_::put() {
  m_db->put(static_cast<AnyObject *>(this));
}
inline void AnyObject_::abort() {
  m_db->abort(static_cast<AnyObject *>(this));
}

// --- DB container

inline constexpr const char *DBs_HeapID() { return "Env.DBs"; }
using DBs =
  ZmRBTree<DB,
    ZmRBTreeNode<DB,
      ZmRBTreeKey<DB::IDAxor,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<DBs_HeapID>>>>>;

// --- DB host configuration

struct HostCf {
  ZuID		id;
  unsigned	priority = 0;
  ZiIP		ip;
  uint16_t	port = 0;
  ZtString	up;
  ZtString	down;

  HostCf(const ZtString &key, const ZvCf *cf) {
    id = key;
    priority = cf->getInt<true>("priority", 0, 1<<30);
    ip = cf->get<true>("ip");
    port = cf->getInt<true>("port", 1, (1<<16) - 1);
    up = cf->get("up");
    down = cf->get("down");
  }

  static ZuID IDAxor(const HostCf &cfg) { return cfg.id; }
};

inline constexpr const char *HostCfs_HeapID() { return "ZdbEnv.HostCfs"; }
using HostCfs =
  ZmRBTree<HostCf,
    ZmRBTreeKey<HostCf::IDAxor,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<HostCfs_HeapID>>>>;

// --- main DB host class

class ZdbAPI Host {
friend Cxn_;
friend Env;

protected:
  Host(Env *env, const HostCf *cf, unsigned dbCount);

public:
  const HostCf &config() const { return *m_cf; }

  ZuID id() const { return m_cf->id; }
  unsigned priority() const { return m_cf->priority; }
  ZiIP ip() const { return m_cf->ip; }
  uint16_t port() const { return m_cf->port; }

  bool voted() const { return m_voted; }
  int state() const { return m_state; }

  bool replicating() const { return m_cxn; }
  static bool replicating(const Host *host) {
    return host ? host->replicating() : false;
  }

  static const char *stateName(int);

  // FIXME
  template <typename S> void print(S &s) const {
    s << "[ID:" << id() << " PRI:" << priority() << " V:" << voted() <<
      " S:" << state() << "] " << envState();
  }
  friend ZuPrintFn ZuPrintType(Host *);

  static ZuID IDAxor(const Host &h) { return h.id(); }
  static ZuPair<unsigned, ZuID> IndexAxor(const Host &h) {
    return ZuFwdPair(h.priority(), h.id());
  }

private:
  Zfb::Offset<ZvTelemetry::fbs::ZdbHost>
  telemetry(ZvTelemetry::IOBuilder &, bool update) const;

  ZmRef<Cxn> cxn() const { return m_cxn; }

  void state(int s) { m_state = s; }

  const EnvState &envState() const { return m_envState; }
  EnvState &envState() { return m_envState; }

  bool active() const { return m_state == HostState::Active; }

  int cmp(const Host *host) const {
    if (ZuUnlikely(host == this)) return 0;
    int i;
    if (i = m_envState.cmp(host->m_envState)) return i;
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

  Env			*m_env;
  const HostCf		*m_cf;
  ZiMultiplex		*m_mx;

  ZmScheduler::Timer	m_connectTimer;

  // guarded by Env

  ZmRef<Cxn>		m_cxn;
  int			m_state = HostState::Instantiated;
  EnvState		m_envState;
  bool			m_voted = false;
};

// host container
using HostIndex =
  ZmRBTree<Host,
    ZmRBTreeNode<Host,
      ZmRBTreeShadow<true,
	ZmRBTreeKey<Host::IndexAxor,
	  ZmRBTreeUnique<true>>>>>;
inline constexpr const char *Hosts_HeapID() { return "ZdbEnv.Hosts"; }
using Hosts =
  ZmHash<HostIndex::Node,
    ZmHashNode<HostIndex::Node,
      ZmHashKey<Host::IDAxor,
	ZmHashHeapID<Hosts_HeapID>>>>;

// --- DB environment handler functions

// UpFn() - activate
typedef void (*UpFn)(Env *, Host *); // env, oldMaster
// DownFn() - de-activate
typedef void (*DownFn)(Env *);

struct EnvHandler {
  UpFn		upFn = [](Env *, Host *) { };
  DownFn	downFn = [](Env *) { };
};

// --- DB environment configuration

struct EnvCf {
  ZmThreadName		thread;
  ZmThreadName		writeThread;
  mutable unsigned	sid = 0;
  mutable unsigned	writeSID = 0;
  ZmRef<ZvCf>		storeCf;
  DBCfs			dbCfs;
  HostCfs		hostCfs;
  ZuID			hostID;
  unsigned		nAccepts = 0;
  unsigned		heartbeatFreq = 0;
  unsigned		heartbeatTimeout = 0;
  unsigned		reconnectFreq = 0;
  unsigned		electionTimeout = 0;
  unsigned		retryFreq = 0;
  ZmHashParams		cxnHash;
#ifdef ZdbRep_DEBUG
  bool			debug = 0;
#endif

  EnvCf() = default;
  EnvCf(const ZvCf *cf) {
    thread = cf->get<true>("thread");
    writeThread = cf->get("writeThread");
    storeCf = cf->getCf("store");
    cf->getCf<true>("dbs")->all([this](ZvCfNode *node) {
      if (auto dbCf = node->getCf())
	dbCfs.addNode(new DBCfs::Node{node->key, ZuMv(dbCf)});
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
#ifdef ZdbRep_DEBUG
    debug = cf->getBool("debug");
#endif
  }
  EnvCf(EnvCf &&) = default;
  EnvCf &operator =(EnvCf &&) = default;
};

// --- main DB environment class

class ZdbAPI Env : public ZmPolymorph, public ZmEngine<Env> {
  Env(const Env &);
  Env &operator =(const Env &);		// prevent mis-use

public:
  using Engine = ZmEngine<Env>;

  using Engine::start;
  using Engine::stop;

friend Engine;
friend Cxn_;
friend AnyObject_;
friend DB;
friend Host;

private:
  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

private:
  static const char *CxnHash_HeapID() { return "ZdbEnv.CxnHash"; }
  using CxnHash =
    ZmHash<ZmRef<Cxn>,
      ZmHashLock<ZmPLock,
	  ZmHashHeapID<CxnHash_HeapID>>>;

public:
#ifdef ZdbRep_DEBUG
  bool debug() const { return m_cf.debug; }
#endif

  Env() { }
  ~Env() { }

  // init() and final() throw ZtString on error
  void init(
      EnvCf config,
      ZiMultiplex *mx,
      EnvHandler handler,
      Store *store = nullptr);
  void final();

  template <typename T>
  ZmRef<DB> initDB(
      ZuID id,
      ScanFn scanFn = nullptr,
      DeleteFn deleteFn = nullptr) {
    return initDB_(id, DBHandler::bind<T>(scanFn, deleteFn));
  }

private:
  ZmRef<DB> initDB_(ZuID, DBHandler);

  void opened(DB *, RN, UN, SN);

public:
  template <typename ...Args>
  void run(Args &&... args) const {
    m_mx->run(m_cf.sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&... args) const {
    m_mx->invoke(m_cf.sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_cf.sid); }

  const EnvCf &config() const { return m_cf; }
  ZiMultiplex *mx() const { return m_mx; }

  int state() const {
    return ZuLikely(m_self) ? m_self->state() : HostState::Instantiated;
  }
  void state(int n) {
    if (ZuUnlikely(!m_self)) {
      ZeLOG(Fatal, ([n](auto &s) {
	s << "ZdbEnv::state(" << HostState::name(n) <<
	  ") called out of order";
      }));
      return;
    }
    m_self->state(n);
  }
  bool active() const { return state() == HostState::Active; }

  Host *self() const { return m_self; }
  template <typename L> void allHosts(L l) const {
    auto i = m_hosts->readIterator();
    while (auto node = i.iterate()) l(node);
  }

public:
  // find database
  ZmRef<DB> db(ZuID id) {
    ZmAssert(invoked());

    return m_dbs.find(id);
  }

private:
  template <typename L> void all_(L l) const {
    // ZmAssert(invoked()); // called from final(), after stop()

    auto i = m_dbs.readIterator();
    while (auto db = i.iterate()) l(db);
  }
public:
  template <typename L> void all(L l) const {
    ZmAssert(invoked());

    auto i = m_dbs.readIterator();
    while (auto db = i.iterate()) db->invoke([l = l(db)]() { l(); });
  }
  template <typename L> void allSync(L l) const {
    ZmAssert(invoked());

    auto i = m_dbs.readIterator();
    ZmBlock<>{}(m_dbs.count_(), [&l, &i](unsigned, auto wake) {
      if (auto db = i.iterate())
	db->invoke([l = l(db), wake = ZuMv(wake)]() mutable { l(); wake(); });
    });
  }

  Zfb::Offset<ZvTelemetry::fbs::ZdbEnv>
  telemetry(ZvTelemetry::IOBuilder &, bool update) const;

public:
  ZvTelemetry::ZdbEnvFn telFn();

  // debug printing
  template <typename S> void print(S &);
  friend ZuPrintFn ZuPrintType(Env *);

private:
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

  void stop_1();
  void stop_2();

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

  void envStateRefresh();	// refresh m_self->envState()

  Host *setMaster();		// returns old leader
  void setNext(Host *host);
  void setNext();

  // outbound replication
  void repStart();
  void repStop();
  void recEnd();

  bool replicate(ZmRef<Buf> buf);

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

  EnvCf			m_cf;
  ZiMultiplex		*m_mx = nullptr;
  Store			*m_store = nullptr;
  bool			m_repStore = false;	// replicated data store

  // mutable while stopped
  EnvHandler		m_handler;
  ZmRef<Hosts>		m_hosts;
  HostIndex		m_hostIndex;

  // SN allocator - atomic
  ZmAtomic<SN>		m_nextSN = 0;

  // environment thread
  DBs			m_dbs;
  CxnList		m_cxns;

  ZmSemaphore		*m_stopping = nullptr;

  bool			m_appActive =false;
  Host			*m_self = nullptr;
  Host			*m_leader = nullptr;	// == m_self if Active
  Host			*m_prev = nullptr;	// previous-ranked host
  Host			*m_next = nullptr;	// next-ranked host
  unsigned		m_recovering = 0;	// recovering next-ranked host
  EnvState		m_recover{4};		// recovery state
  EnvState		m_recoverEnd{4};	// recovery end
  int			m_nPeers = 0;	// # up to date peers
					// # votes received (Electing)
					// # pending disconnects (Stopping)
  ZmTime		m_hbSendTime;

  bool			m_standalone = false;

  ZmScheduler::Timer	m_hbSendTimer;
  ZmScheduler::Timer	m_electTimer;

  // telemetry
  ZuID			m_selfID, m_leaderID, m_prevID, m_nextID;
};

template <typename S>
inline void Env::print(S &s)
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

template <typename T, bool UpdateLRU, bool Evict, typename L>
inline void DB::get_(RN rn, L l) {
  ZmAssert(invoked());

  if (ZuUnlikely(rn >= m_nextRN)) {
    l(nullptr);
    return;
  }
  auto load = [this]<typename L_>(RN rn, L_ l) {
    if (auto buf = findBuf(rn)) {
      l(ZmRef<Object<T>>{load_(record_(msg_(buf->template ptr<Hdr>())))});
      return;
    }

    return get__<T>(rn, ZuMv(l));
  };
  if constexpr (Evict) {
    auto evict = [this](ZmRef<AnyObject> object) {
      m_cacheUN->del(object->un());
    };
    m_cache.find<UpdateLRU>(rn, ZuMv(l), ZuMv(load), ZuMv(evict));
  } else
    m_cache.find<UpdateLRU, false>(rn, ZuMv(l), ZuMv(load));
}

template <typename T>
inline void DB::get__(RN rn, ZmFn<ZmRef<Object<T>>> fn)
{
  using namespace Table_;

  m_table->get(rn, [fn = ZuMv(fn)](DB *db, RN rn, GetResult result) mutable {
    if (ZuLikely(result.contains<GetData>())) {
      const auto &data = result.v<GetData>();
      ZmRef<AnyObject> object = db->m_handler.importFn(db, data.import_);
      if (object) object->init(rn, data.un, data.sn, data.vn);
      db->invoke([fn = ZuMv(fn), object = ZuMv(object)]() mutable {
	fn(ZmRef<Object<T>>{ZuMv(object)});
      });
      return;
    }
    if (ZuUnlikely(result.contains<Event>())) {
      ZeLogEvent(ZuMv(result).v<Event>());
      db->run([db, rn, fn = ZuMv(fn)]() mutable {
	db->get__<T>(rn, ZuMv(fn));
      }, ZmTimeNow(db->env()->config().retryFreq));
      return;
    }
    db->invoke([fn = ZuMv(fn)]() mutable { fn(nullptr); });
  });
}

} // Zdb_

// external API

using ZdbRN = Zdb_::RN;
#define ZdbNullRN Zdb_::nullRN
#define ZdbMaxRN Zdb_::maxRN
using ZdbUN = Zdb_::UN;
#define ZdbNullUN Zdb_::nullUN
#define ZdbMaxUN Zdb_::maxUN
using ZdbSN = Zdb_::SN;
using ZdbVN = Zdb_::VN;
#define ZdbNullSN Zdb_::nullSN
#define ZdbMaxSN Zdb_::maxSN

using ZdbAnyObject = Zdb_::AnyObject;
template <typename T> using ZdbObject = Zdb_::Object<T>;

using Zdb = Zdb_::DB;
using ZdbCf = Zdb_::DBCf;

using ZdbCtorFn = Zdb_::CtorFn;
using ZdbLoadFn = Zdb_::LoadFn;
using ZdbUpdateFn = Zdb_::UpdateFn;
using ZdbSaveFn = Zdb_::SaveFn;
using ZdbFieldsFn = Zdb_::FieldsFn;
using ZdbImportFn = Zdb_::ImportFn;
using ZdbExportFn = Zdb_::ExportFn;
using ZdbSaveFn = Zdb_::SaveFn;
using ZdbScanFn = Zdb_::ScanFn;
using ZdbDeleteFn = Zdb_::DeleteFn;

using ZdbHandler = Zdb_::DBHandler;

using ZdbEnv = Zdb_::Env;
using ZdbEnvCf = Zdb_::EnvCf;

using ZdbUpFn = Zdb_::UpFn;
using ZdbDownFn = Zdb_::DownFn;
using ZdbEnvHandler = Zdb_::EnvHandler;

using ZdbHost = Zdb_::Host;
namespace ZdbHostState { using namespace Zdb_::HostState; }

using ZdbBuf = Zdb_::Buf;

#endif /* Zdb_HPP */
