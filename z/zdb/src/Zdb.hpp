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

// Zdb is a clustered/replicated in-process/in-memory journal DB that
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
//  Opening		Starting | StopPending
//  Closing		Stopping | StartPending
//  Stopped		Stopped
//  Electing		!Stopped
//  Active		!Stopped
//  Inactive		!Stopped
//  Stopping		Stopping | StartPending

#if defined(ZDEBUG) && !defined(ZdbRep_DEBUG)
#define ZdbRep_DEBUG
#endif

#ifdef ZdbRep_DEBUG
#define ZdbDEBUG(env, e) do { if ((env)->debug()) ZeLOG(Debug, (e)); } while (0)
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

  void repRecRcvd(ZmRef<Buf>);
  void repGapRcvd(ZmRef<Buf>);

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
  SN		sn;

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
    auto seqLenOp = record->seqLenOp();
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

// RN is allocated at construction
// UN is allocated and assigned when committed

namespace ObjState {
  ZtEnumValues(ObjState,
      Undefined = 0,
      Push,
      Update,
      Committed,
      Delete,
      Deleted);
}

// possible state paths:
//
// Undefined, Push			push
// Push, Committed			push committed
// Push, Undefined			push aborted
// Committed, Update, Committed		update committed or aborted
// Committed, Delete, Deleted		delete committed
// Committed, Delete, Committed		delete aborted
//
// path forks:
//
// Push   > (Committed|Undefined)
// Delete > (Deleted|Committed)
//
// events:
//
// Undefined > Push	push
// Push > Committed	put
// Push > Undefined	abort
// Committed > Update	update
// Update > Committed	put
// Update > Committed	abort
// Committed > Delete	del
// Delete > Deleted	put
// Delete > Committed	abort

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
  void del();
  void abort();

private:
  void init(RN rn, UN un, SN sn, VN vn) {
    m_rn = rn;
    m_un = un;
    m_sn = sn;
    m_vn = vn;
    m_state = ObjState::Committed;
  }

  bool push_() { m_state = ObjState::Push; }
  bool update_() { m_state = ObjState::Update; }
  bool del_() { m_state = ObjState::Delete; }
  void commit_() {
    using namespace Zdb_;
    m_state = ObjState::Committed;
  }
  void abort_() {
    m_state = m_state == ObjState::Push ?
      ObjState::Undefined : ObjState::Committed;
  }

  DB		*m_db;
  RN		m_rn = nullRN();
  UN		m_un = 0;
  SN		m_sn = 0;
  VN		m_vn = 0;
  int		m_state = ObjState::Undefined;
};
const char *Object_HeapID() { return "Zdb.Object"; }
using ObjCache =
  ZmCache<AnyObject_,
    ZmCacheNode<AnyObject_,
      ZmCacheKey<AnyObject_::RNAxor,
	ZmCacheLock<ZmPLock,
	  ZmCacheHeapID<Object_HeapID>>>>>;
using AnyObject = ObjCache::Node;

// --- DB type-specific object

template <typename T_>
class Object : public AnyObject {
  Object() = delete;
  Object(const Object &) = delete;
  Object &operator =(const Object &) = delete;
  Object(Object &&) = delete;
  Object &operator =(Object &&) = delete;

public:
  using T = T_;

  template <typename L>
  Object(DB *db_, L l) : AnyObject{db_} {
    l(static_cast<void *>(&m_data[0]));
  }

  void *ptr_() { return &m_data[0]; }
  const void *ptr_() const { return &m_data[0]; }

  T *ptr() { return reinterpret_cast<T *>(&m_data[0]); }
  const T *ptr() const { return reinterpret_cast<const T *>(&m_data[0]); }

  ~Object() { ptr()->~T(); }

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
typedef ZtVFieldArray (*FieldsFn)();
// ImportFn(db, import) - import object from data store
typedef AnyObject *(*ImportFn)(DB *, const ZtField::Import &);
// ExportFn(export, ptr) - export object to data store
typedef void (*ExportFn)(ZtField::Export &, const void *);
// RecoverFn(object) - object recovered
typedef void (*RecoverFn)(AnyObject *);
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
  RecoverFn	recoverFn = nullptr;
  DeleteFn	deleteFn = nullptr;

  template <typename T>
  static DBHandler bind(
      RecoverFn recoverFn_ = nullptr,
      DeleteFn deleteFn_ = nullptr) {
    return DBHandler{
      .ctorFn = [](DB *db) -> AnyObject * {
	return new Object<T>{db, [](void *ptr) { new (ptr) T{}; }};
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
      .fieldsFn = []() -> ZtVFieldArray {
	return ZtVFields<T>();
      },
      .importFn = [](DB *db, const ZtField::Import &import_) -> AnyObject * {
	return new Object<T>{db, [&i](void *ptr) {
	  ZtField::ctor<T>(ptr, i);
	}};
      },
      .exportFn = [](ZtField::Export &export_, const void *ptr) {
	ZtField::save<T>(*static_cast<const T *>(ptr), export_);
      },
      .recoverFn = ZuMv(recoverFn_),
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
  mutable unsigned	sid = 0;	// in-memory thread slot ID
  int			cacheMode = ZdbCacheMode::Normal;
  bool			warmup = false;	// warm-up back-end
  uint8_t		repMode = 0;	// 0 - deferred, 1 - in put()

  DBCf() = default;
  DBCf(ZuString id_) : id{id_} { }
  DBCf(ZuString id_, const ZvCf *cf) : id{id_} {
    thread = cf->get("thread");
    cacheMode = cf->getEnum<ZdbCacheMode::Map>(
	"cacheMode", ZdbCacheMode::Normal);
    warmup = cf->getBool("warmup");
    repMode = cf->getBool("repMode");
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
friend File_;
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

  bool open(Table *table);
  void close();

public:
  Env *env() const { return m_env; }
  ZiMultiplex *mx() const { return m_mx; }
  const DBCf &config() const { return *m_cf; }

  static ZuID IDAxor(const DB &db) { return db.config().id; }

  ZuID id() const { return config().id; }
  ZtVFieldArray fields() const { return m_handler.fieldsFn(); }

  template <typename ...Args>
  void run(Args &&... args) const {
    m_mx->run(m_cf->sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&... args) const {
    m_mx->invoke(m_cf->sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_cf->sid); }

private:
  ZmRef<Buf> findBuf(RN rn) { return {m_repBufs->find(rn)}; }

  template <bool UpdateLRU, bool Evict, typename L>
  void get_(RN rn, L l) {
    if (ZuUnlikely(rn >= m_nextRN.load_())) {
      l(nullptr);
      return;
    }
    m_objCache.find<UpdateLRU, Evict>(
	rn, ZuMv(l), [this]<typename L_>(RN rn, L_ l) {
      if (auto buf = findBuf(rn)) {
	l(load_(record_(msg_(buf->template ptr<Hdr>()))));
	return;
      }
      m_table->get(rn, [this, l = ZuMv(l)](RN rn, GetResult r) mutable {
	if (r.contains<const ZtField::Import &>()) {
	  const auto &import_ = r.v<const ZtField::Import &>();
	  ZmRef<AnyObject> o = m_handler.importFn(this, import_);
	  if (o) o->init(rn);
	  invoke([this, l = ZuMv(l), o = ZuMv(o)]() mutable { l(ZuMv(o)); });
	  return;
	}
	if (r.contains<ZuPair<ZeEvent, ZtString>>()) {
	  auto &error = r.v<ZuPair<ZeEvent, ZtString>>();
	  log(ZuMv(error).p<0>(), ZuMv(error).p<1>());
	}
	invoke([this, l = ZuMv(l)]() mutable { l(nullptr); });
      });
    });
  }

public:
  template <typename L>
  void get(RN rn, L l) {
    config().cacheMode == ZdbCacheMode::All ?
      get_<true, false>(rn, ZuMv(l)) :
      get_<true, true >(rn, ZuMv(l));
  }

  // get (RMU version) - do not update LRU (yet)
  template <typename L>
  void getUpdate(RN rn, L l) {
    config().cacheMode == ZdbCacheMode::All ?
      get_<false, false>(rn, ZuMv(l)) :
      get_<false, true >(rn, ZuMv(l));
  }

public:
  // next RN that will be allocated
  RN nextRN() const { return m_nextRN; }

  // create placeholder record (null RN, in-memory, never persisted/replicated)
  ZmRef<AnyObject> placeholder();

private:
  ZmRef<AnyObject> push_(RN rn);
public:
  // create new record
  template <typename L> void push(L l) {
    ZmRef<AnyObject> object = push_(m_nextRN.load_());
    if (!object) { l(nullptr); return; }
    l(object);
    object->abort();
  }
  // create new record (idempotent)
  template <typename L> void push(RN rn, L l) {
    RN nextRN = m_nextRN.load_();
    if (ZuUnlikely(rn != nullRN() && nextRN > rn)) { l(nullptr); return; }
    ZmRef<AnyObject> object = push_(nextRN);
    if (!object) { l(nullptr); return; }
    l(object);
    object->abort();
  }

private:
  bool update_(AnyObject *object, RN rn);
public:
  // update record
  template <typename L> void update(ZmRef<AnyObject> object, L l) {
    if (!update_(object, m_nextRN.load_())) { l(nullptr); return; }
    l(object);
    object->abort();
  }
  // update record (idempotent) - returns true if update can proceed
  template <typename L> void update(ZmRef<AnyObject> object, UN un, L l) {
    RN nextRN = m_nextRN.load_(); // FIXME
    if (ZuUnlikely(rn != nullRN() && nextRN > rn)) { l(nullptr); return; }
    if (!update_(object, nextRN)) { l(nullptr); return; }
    l(object);
    object->abort();
  }

  // update record (with prevRN, without object)
  template <typename L> void update(RN prevRN, L l) {
    getUpdate(prevRN, [this, l = ZuMv(l)](ZmRef<AnyObject> object) mutable {
      if (ZuUnlikely(!object)) { l(nullptr); return; }
      update(ZuMv(object), ZuMv(l));
    });
  }
  // update record (idempotent) (with prevRN, without object)
  template <typename L> void update(RN prevRN, RN rn, L l) {
    getUpdate(prevRN, [this, rn, l = ZuMv(l)](ZmRef<AnyObject> object) mutable {
      if (ZuUnlikely(!object)) { l(nullptr); return; }
      update(ZuMv(object), rn, ZuMv(l));
    });
  }

  // all transactions begin with a push() or update(),
  // and complete with a put(), del() or abort()
  // put() and del() commit the respective operations
  // abort() aborts the pending push() or update()

private:
  // commit push() or update() - causes replication / write
  void put(ZmRef<AnyObject>);
  // commit delete following push() or update()
  void del(ZmRef<AnyObject>);
  // abort push() or update()
  void abort(ZmRef<AnyObject>);

private:
  Zfb::Offset<ZvTelemetry::fbs::Zdb>
  telemetry(ZvTelemetry::IOBuilder &, bool update) const;

  // load object from buffer, bypassing cache
  ZmRef<AnyObject> load_(const fbs::Record *record);
  // load object from buffer, updating cache
  ZmRef<AnyObject> load(const fbs::Record *record);
  // save object to buffer
  Zfb::Offset<void> save(Zfb::Builder &fbb, AnyObject_ *object);

  // outbound recovery / replication
  void recSend(ZmRef<Cxn> cxn, RN rn, RN endRN);
  void recSendGet(ZmRef<Cxn> cxn, RN rn, RN endRN);
  void recSend_(ZmRef<Cxn> cxn, RN rn, RN endRN, ZmRef<Buf> buf);
  void recNext(ZmRef<Cxn> cxn, RN rn, RN endRN);
  ZmRef<Buf> repBuf(RN rn);

  // inbound replication
  void repRecRcvd(ZmRef<Buf> buf);
  void repGapRcvd(ZmRef<Buf> buf);

  // recovery - DB thread
  void recovered(ZmRef<Buf>);
  void recover(const fbs::Record *record);

  // RN allocator
  void allocatedRN(RN rn) {
    m_nextRN = rn + 1;
  }

  // immutable
  Env			*m_env;
  ZiMultiplex		*m_mx;
  const DBCf		*m_cf;
  DBHandler		m_handler;
  ZtString		m_path;

  // RN allocator
  ZmAtomic<RN>		m_nextRN = 0;

  // open/closed state
  bool			m_open = false;		// DB thread

  // back-end data store table
  Table			*m_table = nullptr;	// DB thread

  // object cache
  ObjCache		m_objCache;		// MT locked

  // pending replications
  ZmRef<RepBufs>	m_repBufs;		// MT locked
};

inline void AnyObject_::put() { m_db->put(this); }
inline void AnyObject_::del() { m_db->del(this); }
inline void AnyObject_::abort() { m_db->abort(this); }

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
    id = cf->get("id", true);
    priority = cf->getInt<true>("priority", 0, 1<<30);
    ip = cf->get("ip", true);
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
  Host(Env *env, const HostCf *config, unsigned dbCount);

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
  ZmThreadName			thread;
  mutable unsigned		sid = 0;
  ZmRef<ZvCf>			storeCf;
  DBCfs				dbCfs;
  HostCfs			hostCfs;
  ZuID				hostID;
  unsigned			nAccepts = 0;
  unsigned			heartbeatFreq = 0;
  unsigned			heartbeatTimeout = 0;
  unsigned			reconnectFreq = 0;
  unsigned			electionTimeout = 0;
  ZmHashParams			cxnHash;
#ifdef ZdbRep_DEBUG
  bool				debug = 0;
#endif

  EnvCf() = default;
  EnvCf(const ZvCf *cf) {
    path = cf->get<true>("path");
    thread = cf->get<true>("thread");
    storeCf = cf->getCf<true>("store");
    cf->getCf<true>("dbs")->all([this](ZvCfNode *node) {
      if (auto dbCf = node->getCf())
	dbCfs.addNode(new DBCfs::Node{node->key, ZuMv(dbCf)});
    });
    cf->getCf<true>("hosts")->all([this](ZvCfNode *node) {
      if (auto hostCf = node->getCf())
	hostCfs.addNode(new HostCfs::Node{node->key, ZuMv(hostCf)});
    });
    hostID = cf->get<true>("hostID");
    nAccepts = cf->getInt("nAccepts", 1, 1<<10, 8);
    heartbeatFreq = cf->getInt("heartbeatFreq", 1, 3600, 1);
    heartbeatTimeout = cf->getInt("heartbeatTimeout", 1, 14400, 4);
    reconnectFreq = cf->getInt("reconnectFreq", 1, 3600, 1);
    electionTimeout = cf->getInt("electionTimeout", 1, 3600, 8);
#ifdef ZdbRep_DEBUG
    debug = cf->getBool("debug");
#endif
  }
  EnvCf(EnvCf &&) = default;
  EnvCf &operator =(EnvCf &&) = default;
};

// --- main DB environment class

// FIXME - FE<>BE API
//
// init -> <- initialized
// open -> <- opened (success/failure - success includes next UN, next RN)
//   // Note: next UN is used by Env, not DB
//   // Note: we assume that no replication commences until all DBs are opened,
//   // Note: open() is triggered by DB binding, 
//   //   ensuring accurate recovery of last UN
//   // Note: app may want a full table scan to populate in-memory indices
//   //   or the cache (if caching mode is fully-cached) - BE will need to
//   //   select *, using batching/paging, and populate app with recovered
//   //   callbacks
//   // Note: recovered callback is also used by inactive hosts to maintain
//   //   app in-memory indices
//   <- recovered
// get (checks repBufs before requesting) -> <- gotten
// push/update/del committed (DB::write) -> <- written (DB::write_ removes from repBufs)
// close -> <- closed
// final -> <- finalized

class ZdbAPI Env : public ZmPolymorph, public ZmEngine<Env> {
  Env(const Env &);
  Env &operator =(const Env &);		// prevent mis-use

  using Engine = ZmEngine<Env>;

friend Engine;
friend Cxn_;
friend AnyObject_;
friend DB;
friend Host;

  using Engine::start;
  using Engine::stop;

  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  static const char *CxnHash_HeapID() { return "ZdbEnv.CxnHash"; }
  using CxnHash =
    ZmHash<ZmRef<Cxn>,
      ZmHashLock<ZmPLock,
	  ZmHashHeapID<CxnHash_HeapID>>>;

#ifdef ZdbRep_DEBUG
  bool debug() const { return m_cf.debug; }
#endif

public:
  Env() { }
  ~Env() { }

  // init() and final() throw ZtString on error
  void init(EnvCf config, ZiMultiplex *mx, EnvHandler handler);
  void final();

  template <typename T>
  ZmRef<DB> initDB(
      ZuID id,
      RecoverFn recoverFn = nullptr,
      DeleteFn deleteFn = nullptr) {
    return initDB_(id, DBHandler::bind<T>(recoverFn, deleteFn));
  }

private:
  ZmRef<DB> initDB_(ZuID, DBHandler);

  void opened(DB *, UN, RN);

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

  void checkpoint();

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

private:
  Host *self() const {
    ZmAssert(invoked());
    return m_self;
  }
  template <typename L> void allHosts(L l) const {
    ZmAssert(invoked());
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
    ZmAssert(invoked());

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
	db->invoke([l = l(db), wake = ZuMv(wake)]() { l(); wake(); });
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
  void replicated(Host *host, ZuID id, RN rn);

  bool isStandalone() const { return m_standalone; }

  // UN
  UN nextUN() const { return m_nextUN; }
  UN allocUN() { return m_nextUN++; }
  void recoveredUN(UN un) { m_nextUN.minimum(un + 1); }

  EnvCf			m_cf;
  ZiMultiplex		*m_mx = nullptr;

  // mutable while stopped
  EnvHandler		m_handler;
  ZmRef<Hosts>		m_hosts;
  HostIndex		m_hostIndex;

  // atomic update number
  ZmAtomic<uint128_t>	m_nextUN = 0;

  // environment thread
  DBs			m_dbs;
  CxnList		m_cxns;

  ZmSemaphore		*m_stopping = nullptr;

  bool			m_appActive =false;
  Host			*m_self = nullptr;
  Host			*m_leader = nullptr;	// == m_self if Active
  Host			*m_prev = nullptr;	// previous-ranked host
  Host			*m_next = nullptr;	// next-ranked host
  ZmRef<Cxn>		m_nextCxn;		// replica peer's cxn
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

} // Zdb_

// external API

using ZdbRN = Zdb_::RN;

using ZdbAnyObject = Zdb_::AnyObject;
template <typename T> using ZdbObject = Zdb_::Object<T>;

using Zdb = Zdb_::DB;
using ZdbCf = Zdb_::DBCf;

using ZdbCtorFn = Zdb_::CtorFn;
using ZdbLoadFn = Zdb_::LoadFn;
using ZdbUpdateFn = Zdb_::UpdateFn;
using ZdbSaveFn = Zdb_::SaveFn;
using ZdbRecoverFn = Zdb_::RecoverFn;
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
