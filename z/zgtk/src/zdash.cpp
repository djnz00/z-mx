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

#include <stdio.h>
#include <stdlib.h>

#include <libintl.h>

#include <zlib/ZuArrayN.hpp>
#include <zlib/ZuPolymorph.hpp>
#include <zlib/ZuByteSwap.hpp>
#include <zlib/ZuVersion.hpp>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTrap.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmScheduler.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiMultiplex.hpp>
#include <zlib/ZiModule.hpp>
#include <zlib/ZiRing.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvCSV.hpp>
#include <zlib/ZvRingCf.hpp>
#include <zlib/ZvMxParams.hpp>
#include <zlib/ZvUserDB.hpp>
#include <zlib/ZvCmdClient.hpp>
#include <zlib/ZvCmdServer.hpp>

#include <zlib/Zdf.hpp>

#include <zlib/ZGtkApp.hpp>
#include <zlib/ZGtkCallback.hpp>
#include <zlib/ZGtkTreeModel.hpp>
#include <zlib/ZGtkValue.hpp>

#include "request_fbs.h"
#include "reqack_fbs.h"

// FIXME - css
//
// @define-color rag_red_bg #ff1515;

// FIXME
// - right-click row selection in tree -> watch
// - drag/drop rowset in watchlist -> graph
// - field select in graph
//   - defaults to no field selected
//   - until field selected, no trace

static void usage()
{
  static const char *usage =
    "usage: zdash\n";
  std::cerr << usage << std::flush;
  ZeLog::stop();
  Zm::exit(1);
}

namespace ZDash {

namespace Telemetry {
  struct Watch {
    void	*ptr_ = nullptr;

    template <typename T> const T *ptr() const {
      return static_cast<const T *>(ptr_);
    }
    template <typename T> T *ptr() {
      return static_cast<T *>(ptr_);
    }
  };
  struct Watch_Accessor {
    static auto get(const Watch &v) { return v.ptr_; }
  };
  struct Watch_HeapID {
    static constexpr const char *id() { return "zdash.Telemetry.Watch"; }
  };
  template <typename T>
  using WatchList =
    ZmList<T,
      ZmListKey<Watch_Accessor,
	ZmListNodeDerive<true,
	  ZmListHeapID<Watch_HeapID,
	    ZmListLock<ZmNoLock> > > > >;

  // display - contains pointer to tree array
  struct Display_ : public Watch {
    unsigned		row = 0;
  };
  using DispList = WatchList<Display_>;
  using Display = DispList::Node;

  // graph - contains pointer to graph - graph contains field selection
  struct Graph_ : public Watch { };
  using GraphList = WatchList<Graph_>;
  using Graph = GraphList::Node;

  using TypeList = ZvTelemetry::TypeList;
  using FBTypeList = ZuTypeMap<ZfbType, TypeList>;

  template <typename Data> struct Item__ {
    using TelKey = ZuFieldKey<Data>;
    static TelKey telKey(const Data &data) { return TelKey{data}; }
    static int rag(const Data &data) { return data.rag(); }
  };
  template <> struct Item__<ZvTelemetry::App> {
    using TelKey = ZuTuple<const ZtString &>;
    void initTelKey(const ZtString &server, uint16_t port) {
      telKey_ << server << ':' << port;
    }
    TelKey telKey(const ZvTelemetry::App &) const {
      return TelKey{telKey_};
    }
    static int rag(const ZvTelemetry::App &data) {
      return data.rag;
    }
    ZtString	telKey_;
  };
  template <> struct Item__<ZvTelemetry::DBEnv> {
    using TelKey = ZuTuple<const char *>;
    static TelKey telKey(const ZvTelemetry::DBEnv &) {
      return TelKey{"dbenv"};
    }
    static int rag(const ZvTelemetry::DBEnv &) {
      return ZvTelemetry::RAG::Off;
    }
  };
  template <typename Data_> class Item_ : public Item__<Data_> {
  public:
    using Data = Data_;
    using Load = ZfbField::Load<Data>;
    using TelKey = typename Item__<Data_>::TelKey;

    Item_(void *link__) : link_{link__} { }
    template <typename FBType>
    Item_(void *link__, FBType *fbo) : link_{link__}, data{fbo} { }

  private:
    Item_(const Item_ &) = delete;
    Item_ &operator =(const Item_ &) = delete;
    Item_(Item_ &&) = delete;
    Item_ &operator =(Item_ &&) = delete;
  public:
    ~Item_() {
      if (dataFrame) {
	dfWriter.final();
	dataFrame->close();
	dataFrame = nullptr;
      }
    }

    template <typename Link>
    Link *link() const { return static_cast<Link *>(link_); }

    template <typename T>
    T *gtkRow() const { return static_cast<T *>(gtkRow_); }
    template <typename T>
    void gtkRow(T *node) { gtkRow_ = node; }

    TelKey telKey() const { return Item__<Data_>::telKey(data); }
    int rag() const { return Item__<Data_>::rag(data); }

    bool record(ZuString name, Zdf::Mgr *mgr, ZeError *e = nullptr) {
      dataFrame = new Zdf::DataFrame{Data::fields(), name, true};
      dataFrame->init(mgr);
      if (!dataFrame->open(e)) return false;
      dfWriter = dataFrame->writer();
      return true;
    }

    void			*link_;
    Load			data;

    void			*gtkRow_ = nullptr;
    DispList			dispList;
    GraphList			graphList;

    ZuPtr<Zdf::DataFrame>	dataFrame;
    Zdf::DataFrame::Writer	dfWriter;
  };

  struct ItemTree_HeapID {
    static constexpr const char *id() { return "zdash.Telemetry.Tree"; }
  };
  template <typename T>
  struct KeyAccessor {
    static auto get(const T &v) { return v.telKey(); }
  };
  template <typename T>
  using ItemTree_ =
    ZmRBTree<Item_<T>,
      ZmRBTreeKey<KeyAccessor<Item_<T>>,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeNodeDerive<true,
	    ZmRBTreeUnique<true,
	      ZmRBTreeLock<ZmNoLock,
		ZmRBTreeHeapID<ItemTree_HeapID> > > > > > >;
  template <typename T>
  class ItemTree : public ItemTree_<T> {
  public:
    using Node = typename ItemTree_<T>::Node;
    template <typename FBType>
    Node *lookup(const FBType *fbo) const {
      return this->find(ZfbField::key<T>(fbo));
    }
  };
  template <typename T> class ItemSingleton {
    ItemSingleton(const ItemSingleton &) = delete;
    ItemSingleton &operator =(const ItemSingleton &) = delete;
    ItemSingleton(ItemSingleton &&) = delete;
    ItemSingleton &operator =(ItemSingleton &&) = delete;
  public:
    using Node = Item_<T>;
    ItemSingleton() = default;
    ~ItemSingleton() { if (m_node) delete m_node; }
    template <typename FBType>
    Node *lookup(const FBType *) const { return m_node; }
    void add(Node *node) { if (m_node) delete m_node; m_node = node; }
  private:
    Node	*m_node = nullptr;
  };
  class AlertArray {
    AlertArray(const AlertArray &) = delete;
    AlertArray &operator =(const AlertArray &) = delete;
    AlertArray(AlertArray &&) = delete;
    AlertArray &operator =(AlertArray &&) = delete;
  public:
    using T = ZvTelemetry::Alert;
    using Node = T;
    AlertArray() = default;
    ~AlertArray() = default;
    ZtArray<T>	data;
  };

  template <typename U> struct Container_ { // default
    using T = ItemTree<U>;
  };
  template <> struct Container_<ZvTelemetry::App> {
    using T = ItemSingleton<ZvTelemetry::App>;
  };
  template <> struct Container_<ZvTelemetry::DBEnv> {
    using T = ItemSingleton<ZvTelemetry::DBEnv>;
  };
  template <> struct Container_<ZvTelemetry::Alert> {
    using T = AlertArray;
  };
  template <typename U>
  using Container = typename Container_<U>::T;

  using ContainerList = ZuTypeMap<Container, TypeList>;
  using Containers = ZuTypeApply<ZuTuple, ContainerList>;

  template <typename T> using Item = typename Container<T>::Node;
}

namespace GtkTree {
  template <typename Impl, typename Item>
  class Row {
    Impl *impl() { return static_cast<Impl *>(this); }
    const Impl *impl() const { return static_cast<const Impl *>(this); }

  public:
    Item	*item = nullptr;

    Row() = default;
    Row(Item *item_) : item{item_} { init_(); }
    void init(Item *item_) { item = item_; init_(); }
    void init_() { item->gtkRow(impl()); }

    using TelKey = typename Item::TelKey;
    TelKey telKey() const { return item->telKey(); }
    int rag() const { return item->rag(); }
    int cmp(const Impl &v) const {
      return item->telKey().cmp(v.telKey());
    }
  };

  template <unsigned Depth, typename Item>
  struct Leaf :
      public Row<Leaf<Depth, Item>, Item>,
      public ZGtk::TreeHierarchy::Leaf<Leaf<Depth, Item>, Depth> {
    Leaf() = default;
    Leaf(Item *item) : Row<Leaf, Item>{item} { }
    using Row<Leaf, Item>::cmp;
  };

  template <unsigned Depth, typename Item, typename Child>
  struct Parent :
      public Row<Parent<Depth, Item, Child>, Item>,
      public ZGtk::TreeHierarchy::Parent<
	Parent<Depth, Item, Child>, Depth, Child> {
    Parent() = default;
    Parent(Item *item) : Row<Parent, Item>{item} { }
    using Row<Parent, Item>::cmp;
    using Base = ZGtk::TreeHierarchy::Parent<Parent, Depth, Child>;
    using Base::add;
    using Base::del;
  };

  template <unsigned Depth, typename Item, typename Tuple>
  struct Branch :
      public Row<Branch<Depth, Item, Tuple>, Item>,
      public ZGtk::TreeHierarchy::Branch<
	Branch<Depth, Item, Tuple>, Depth, Tuple> {
    Branch() = default; 
    Branch(Item *item) : Row<Branch, Item>{item} { }
    using Row<Branch, Item>::cmp;
    using Base = ZGtk::TreeHierarchy::Branch<Branch, Depth, Tuple>;
    using Base::add;
    using Base::del;
  };
  template <typename TelKey_>
  struct BranchChild {
    using TelKey = TelKey_;
    static int rag() { return ZvTelemetry::RAG::Off; }
  };

  template <typename T> using TelItem = Telemetry::Item<T>;

  using Heap = Leaf<3, TelItem<ZvTelemetry::Heap>>;
  using HashTbl = Leaf<3, TelItem<ZvTelemetry::HashTbl>>;
  using Thread = Leaf<3, TelItem<ZvTelemetry::Thread>>;
  using Socket = Leaf<4, TelItem<ZvTelemetry::Socket>>;
  using Mx = Parent<3, TelItem<ZvTelemetry::Mx>, Socket>;
  using Queue = Leaf<3, TelItem<ZvTelemetry::Queue>>;
  using Link = Leaf<4, TelItem<ZvTelemetry::Link>>;
  using Engine = Parent<3, TelItem<ZvTelemetry::Engine>, Link>;
  using DBHost = Leaf<4, TelItem<ZvTelemetry::DBHost>>;
  using DB = Leaf<4, TelItem<ZvTelemetry::DB>>;

  // DB hosts
  struct DBHosts : public BranchChild<ZuTuple<const char *>> {
    static auto telKey() { return TelKey{"hosts"}; }
  };
  using DBHostParent = Parent<3, DBHosts, DBHost>;
  // DBs
  struct DBs : public BranchChild<ZuTuple<const char *>> {
    static auto telKey() { return TelKey{"dbs"}; }
  };
  using DBParent = Parent<3, DBs, DB>;

  // heaps
  struct Heaps :
      public BranchChild<ZuTuple<const char *, const char *, const char *>> {
    static auto telKey() { return TelKey{"heaps", "partition", "size"}; }
  };
  using HeapParent = Parent<2, Heaps, Heap>;
  // hashTbls
  struct HashTbls : public BranchChild<ZuTuple<const char *, const char *>> {
    static auto telKey() { return TelKey{"hashTbls", "addr"}; }
  };
  using HashTblParent = Parent<2, HashTbls, HashTbl>;
  // threads
  struct Threads : public BranchChild<ZuTuple<const char *>> {
    static auto telKey() { return TelKey{"threads"}; }
  };
  using ThreadParent = Parent<2, Threads, Thread>;
  // multiplexers
  struct Mxs : public BranchChild<ZuTuple<const char *>> {
    static auto telKey() { return TelKey{"multiplexers"}; }
  };
  using MxParent = Parent<2, Mxs, Mx>;
  // queues
  struct Queues : public BranchChild<ZuTuple<const char *, const char *>> {
    static auto telKey() { return TelKey{"queues", "type"}; }
  };
  using QueueParent = Parent<2, Queues, Queue>;
  // engines
  struct Engines : public BranchChild<ZuTuple<const char *>> {
    static auto telKey() { return TelKey{"engines"}; }
  };
  using EngineParent = Parent<2, Engines, Engine>;

  // dbEnv
  ZuDeclTuple(DBEnvTuple,
      (DBHostParent, hosts),
      (DBParent, dbs));
  using DBEnv = Branch<2, TelItem<ZvTelemetry::DBEnv>, DBEnvTuple>;
  // applications
  ZuDeclTuple(AppTuple,
      (HeapParent, heaps),
      (HashTblParent, hashTbls),
      (ThreadParent, threads),
      (MxParent, mxs),
      (QueueParent, queues),
      (EngineParent, engines),
      (DBEnv, dbEnv));
  using App = Branch<1, TelItem<ZvTelemetry::App>, AppTuple>;

  struct Root : public ZGtk::TreeHierarchy::Parent<Root, 0, App> { };

  // map telemetry items to corresponding tree nodes
  inline Heap *row(TelItem<ZvTelemetry::Heap> *item) {
    return item->template gtkRow<Heap>();
  }
  inline HashTbl *row(TelItem<ZvTelemetry::HashTbl> *item) {
    return item->template gtkRow<HashTbl>();
  }
  inline Thread *row(TelItem<ZvTelemetry::Thread> *item) {
    return item->template gtkRow<Thread>();
  }
  inline Mx *row(TelItem<ZvTelemetry::Mx> *item) {
    return item->template gtkRow<Mx>();
  }
  inline Socket *row(TelItem<ZvTelemetry::Socket> *item) {
    return item->template gtkRow<Socket>();
  }
  inline Queue *row(TelItem<ZvTelemetry::Queue> *item) {
    return item->template gtkRow<Queue>();
  }
  inline Engine *row(TelItem<ZvTelemetry::Engine> *item) {
    return item->template gtkRow<Engine>();
  }
  inline Link *row(TelItem<ZvTelemetry::Link> *item) {
    return item->template gtkRow<Link>();
  }
  inline DB *row(TelItem<ZvTelemetry::DB> *item) {
    return item->template gtkRow<DB>();
  }
  inline DBHost *row(TelItem<ZvTelemetry::DBHost> *item) {
    return item->template gtkRow<DBHost>();
  }
  inline DBEnv *row(TelItem<ZvTelemetry::DBEnv> *item) {
    return item->template gtkRow<DBEnv>();
  }
  inline App *row(TelItem<ZvTelemetry::App> *item) {
    return item->template gtkRow<App>();
  }

  enum { Depth = 5 };

  // (*) - branch
  using Iter = ZuUnion<
    // Root *,
    App *,		// [app]

    // app children
    HeapParent *,	// app->heaps (*)
    HashTblParent *,	// app->hashTbls (*)
    ThreadParent *,	// app->threads (*)
    MxParent *,		// app->mxs (*)
    QueueParent *,	// app->queues (*)
    EngineParent *,	// app->engines (*)
    DBEnv *,		// app->dbEnv (*)

    // app grandchildren
    Heap *,		// app->heaps->[heap]
    HashTbl *,		// app->hashTbls->[hashTbl]
    Thread *,		// app->threads->[thread]
    Mx *,		// app->mxs->[mx]
    Queue *,		// app->queues->[queue]
    Engine *,		// app->engines->[engine]

    // app great-grandchildren
    Socket *,		// app->mxs->mx->[socket]
    Link *,		// app->engines->engine->[link]

    // DBEnv children
    DBHostParent *,	// app->dbEnv->hosts (*)
    DBParent *,		// app->dbEnv->dbs (*)

    // DBEnv grandchildren
    DBHost *,		// app->dbEnv->hosts->[host]
    DB *>;		// app->dbEnv->dbs->[db]

  class Model : public ZGtk::TreeHierarchy::Model<Model, Iter, Depth> {
  public:
    enum { RAGCol = 0, IDCol0, IDCol1, IDCol2, NCols };

    // root()
    Root *root() { return &m_root; }

    // parent() - child->parent type map
    template <typename T>
    static ZuSame<T, App, Root> *parent(void *ptr) {
      return static_cast<Root *>(ptr);
    }
    template <typename T>
    static ZuIfT<
	ZuConversion<T, HeapParent>::Same ||
	ZuConversion<T, HashTblParent>::Same ||
	ZuConversion<T, ThreadParent>::Same ||
	ZuConversion<T, MxParent>::Same ||
	ZuConversion<T, QueueParent>::Same ||
	ZuConversion<T, EngineParent>::Same ||
	ZuConversion<T, DBEnv>::Same, App> *parent(void *ptr) {
      return static_cast<App *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Heap, HeapParent> *parent(void *ptr) {
      return static_cast<HeapParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, HashTbl, HashTblParent> *parent(void *ptr) {
      return static_cast<HashTblParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Thread, ThreadParent> *parent(void *ptr) {
      return static_cast<ThreadParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Mx, MxParent> *parent(void *ptr) {
      return static_cast<MxParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Queue, QueueParent> *parent(void *ptr) {
      return static_cast<QueueParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Engine, EngineParent> *parent(void *ptr) {
      return static_cast<EngineParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Socket, Mx> *parent(void *ptr) {
      return static_cast<Mx *>(ptr);
    }
    template <typename T>
    static ZuSame<T, Link, Engine> *parent(void *ptr) {
      return static_cast<Engine *>(ptr);
    }
    template <typename T>
    static ZuIfT<
	ZuConversion<T, DBHostParent>::Same ||
	ZuConversion<T, DBParent>::Same, DBEnv> *parent(void *ptr) {
      return static_cast<DBEnv *>(ptr);
    }
    template <typename T>
    static ZuSame<T, DBHost, DBHostParent> *parent(void *ptr) {
      return static_cast<DBHostParent *>(ptr);
    }
    template <typename T>
    static ZuSame<T, DB, DBParent> *parent(void *ptr) {
      return static_cast<DBParent *>(ptr);
    }

    // key printing
    template <typename Impl, typename Key>
    class KeyPrint_ {
      Impl *impl() { return static_cast<Impl *>(this); }
      const Impl *impl() const { return static_cast<const Impl *>(this); }
    public:
      Key key;
      template <typename Key_>
      KeyPrint_(Key_ &&key_) : key{ZuFwd<Key_>(key_)} { }
      auto p0() const { return key.template p<0>(); }
      template <unsigned N = Key::N>
      auto p1(ZuIfT<(N <= 1)> *_ = nullptr) const { return ""; }
      template <unsigned N = Key::N>
      auto p1(ZuIfT<(N > 1)> *_ = nullptr) const {
	return key.template p<1>();
      }
      template <unsigned N = Key::N>
      auto p2(ZuIfT<(N <= 2)> *_ = nullptr) const { return ""; }
      template <unsigned N = Key::N>
      auto p2(ZuIfT<(N > 2)> *_ = nullptr) const {
	return key.template p<2>();
      }
    };
    // generic key printing
    template <typename Key>
    struct KeyPrint : public KeyPrint_<KeyPrint<Key>, Key> {
      using KeyPrint_<KeyPrint, Key>::KeyPrint_;
    };
    template <typename T, typename Key>
    static ZuIfT<
      !ZuConversion<T, HashTbl>::Same &&
      !ZuConversion<T, Queue>::Same,
      KeyPrint<Key>> keyPrintType();
    // override addr for hash tables
    template <typename Key>
    struct HashTblKeyPrint : public KeyPrint_<HashTblKeyPrint<Key>, Key> {
      using KeyPrint_<HashTblKeyPrint<Key>, Key>::KeyPrint_;
      auto p1() { return ZuBoxed(this->key.template p<1>()).hex(); }
    };
    template <typename T, typename Key>
    static ZuIfT<
      ZuConversion<T, HashTbl>::Same, HashTblKeyPrint<Key>>
    keyPrintType();
    // override type for queues
    template <typename Key>
    struct QueueKeyPrint : public KeyPrint_<QueueKeyPrint<Key>, Key> {
      using KeyPrint_<QueueKeyPrint<Key>, Key>::KeyPrint_;
      auto p1() {
	return ZvTelemetry::QueueType::name(this->key.template p<1>());
      }
    };
    template <typename T, typename Key>
    static ZuIfT<
      ZuConversion<T, Queue>::Same, QueueKeyPrint<Key>>
    keyPrintType();

    gint get_n_columns() { return NCols; }
    GType get_column_type(gint i) {
      switch (i) {
	case RAGCol: return G_TYPE_INT;
	case IDCol0: return G_TYPE_STRING;
	case IDCol1: return G_TYPE_STRING;
	case IDCol2: return G_TYPE_STRING;
	default: return G_TYPE_NONE;
      }
    }
    template <typename T>
    void value(const T *ptr, gint i, ZGtk::Value *v) {
      using Key = decltype(ptr->telKey());
      using KeyPrint = decltype(keyPrintType<T, Key>());
      KeyPrint print{ptr->telKey()};
      switch (i) {
	case RAGCol: 
	  v->init(G_TYPE_INT);
	  v->set_int(ptr->rag());
	  break;
	case IDCol0:
	  m_value.length(0);
	  v->init(G_TYPE_STRING);
	  m_value << print.p0();
	  v->set_static_string(m_value);
	  break;
	case IDCol1:
	  m_value.length(0);
	  v->init(G_TYPE_STRING);
	  m_value << print.p1();
	  v->set_static_string(m_value);
	  break;
	case IDCol2:
	  m_value.length(0);
	  v->init(G_TYPE_STRING);
	  m_value << print.p2();
	  v->set_static_string(m_value);
	  break;
	default:
	  v->init(G_TYPE_NONE);
	  break;
      }
    }

  private:
    Root	m_root;		// root of tree
    ZtString	m_value;	// re-used string buffer
  };

  class View {
    View(const View &) = delete;
    View &operator =(const View &) = delete;
    View(View &&) = delete;
    View &operator =(View &&) = delete;
  public:
    View() = default;
    ~View() = default;

  private:
    template <unsigned RagCol, unsigned TextCol>
    void addCol(const char *id) {
      auto col = gtk_tree_view_column_new();
      gtk_tree_view_column_set_title(col, gettext(id));

      auto cell = gtk_cell_renderer_text_new();
      gtk_tree_view_column_pack_start(col, cell, true);

      gtk_tree_view_column_set_cell_data_func(col, cell, [](
	    GtkTreeViewColumn *col, GtkCellRenderer *cell,
	    GtkTreeModel *model, GtkTreeIter *iter, gpointer this_) {
	reinterpret_cast<View *>(this_)->render<RagCol, TextCol>(
	    col, cell, model, iter);
      }, reinterpret_cast<gpointer>(this), nullptr);

      gtk_tree_view_append_column(m_treeView, col);

      // normally would add columns in order of saved column order;
      // and not add columns unselected by user
      //
      // need
      // - array of available columns (including ID)
      // - array of selected columns in display order
    }

    template <unsigned RagCol, unsigned TextCol>
    void render(
	GtkTreeViewColumn *col, GtkCellRenderer *cell,
	GtkTreeModel *model, GtkTreeIter *iter) {
      m_values[0].unset();
      gtk_tree_model_get_value(model, iter, TextCol, &m_values[0]);

      gint rag;
      {
	ZGtk::Value rag_;
	gtk_tree_model_get_value(model, iter, RagCol, &rag_);
	rag = rag_.get_int();
      }
      switch (rag) {
	case ZvTelemetry::RAG::Red:
	  m_values[1].set_static_boxed(&m_rag_red_bg);
	  m_values[2].set_static_boxed(&m_rag_red_fg);
	  g_object_setv(G_OBJECT(cell), 3, m_props, m_values);
	  break;
	case ZvTelemetry::RAG::Amber:
	  m_values[1].set_static_boxed(&m_rag_amber_bg);
	  m_values[2].set_static_boxed(&m_rag_amber_fg);
	  g_object_setv(G_OBJECT(cell), 3, m_props, m_values);
	  break;
	case ZvTelemetry::RAG::Green:
	  m_values[1].set_static_boxed(&m_rag_green_bg);
	  m_values[2].set_static_boxed(&m_rag_green_fg);
	  g_object_setv(G_OBJECT(cell), 3, m_props, m_values);
	  break;
	default:
	  m_values[1].set_static_boxed(&m_rag_off_bg);
	  m_values[2].set_static_boxed(&m_rag_off_fg);
	  g_object_setv(G_OBJECT(cell), 3, m_props, m_values);
	  break;
      }
    }

  public:
    void init(GtkTreeView *view, GtkStyleContext *context) {
      m_treeView = view;

      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_red_fg", &m_rag_red_fg))
	m_rag_red_fg = { 0.0, 0.0, 0.0, 1.0 }; // #000000
      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_red_bg", &m_rag_red_bg))
	m_rag_red_bg = { 1.0, 0.0820, 0.0820, 1.0 }; // #ff1515

      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_amber_fg", &m_rag_amber_fg))
	m_rag_amber_fg = { 0.0, 0.0, 0.0, 1.0 }; // #000000
      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_amber_bg", &m_rag_amber_bg))
	m_rag_amber_bg = { 1.0, 0.5976, 0.0, 1.0 }; // #ff9900

      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_green_fg", &m_rag_green_fg))
	m_rag_green_fg = { 0.0, 0.0, 0.0, 1.0 }; // #000000
      if (!context || !gtk_style_context_lookup_color(
	    context, "rag_green_bg", &m_rag_green_bg))
	m_rag_green_bg = { 0.1835, 0.8789, 0.2304, 1.0 }; // #2fe13b

      addCol<Model::RAGCol, Model::IDCol0>("ID");
      addCol<Model::RAGCol, Model::IDCol1>("");
      addCol<Model::RAGCol, Model::IDCol2>("");

      static const gchar *props[] = {
	"text", "background-rgba", "foreground-rgba"
      };

      m_props = props;
      m_values[1].init(GDK_TYPE_RGBA);
      m_values[2].init(GDK_TYPE_RGBA);

      {
	auto cell = gtk_cell_renderer_text_new();
	g_object_getv(G_OBJECT(cell), 2, &m_props[1], &m_values[1]);
	m_rag_off_bg =
	  *reinterpret_cast<const GdkRGBA *>(m_values[1].get_boxed());
	m_rag_off_fg =
	  *reinterpret_cast<const GdkRGBA *>(m_values[2].get_boxed());
	g_object_unref(G_OBJECT(cell));
      }

      g_signal_connect(
	  G_OBJECT(m_treeView), "destroy",
	  ZGtk::callback([](GObject *, gpointer this_) {
	    reinterpret_cast<View *>(this_)->destroyed();
	  }), reinterpret_cast<gpointer>(this));
    }

    void destroyed() {
      m_treeView = nullptr;
    }

    void final() {
      if (m_treeView) g_object_unref(G_OBJECT(m_treeView));
    }

    void bind(GtkTreeModel *model) {
      gtk_tree_view_set_model(m_treeView, model);
    }

  private:
    GtkTreeView	*m_treeView = nullptr;
    GdkRGBA	m_rag_red_fg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_red_bg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_amber_fg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_amber_bg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_green_fg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_green_bg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_off_fg = { 0.0, 0.0, 0.0, 0.0 };
    GdkRGBA	m_rag_off_bg = { 0.0, 0.0, 0.0, 0.0 };
    const gchar	**m_props;
    ZGtk::Value	m_values[3];
  };
}

class App_Cli;
class App_Srv;
class SrvLink;

class CliLink_ : public ZvCmdCliLink<App_Cli, CliLink_> {
public:
  using Base = ZvCmdCliLink<App_Cli, CliLink_>;
  using ID = unsigned;
  using Key = ID;
  Key key() const { return id; }

  template <typename Server>
  CliLink_(App_Cli *, ID, Server &&server, uint16_t port, SrvLink *);

  void loggedIn();
  void disconnected();
  void connectFailed(bool transient);

  int processTelemetry(const uint8_t *data, unsigned len);
  int processDeflt(ZuID id, const uint8_t *data, unsigned len);

  ID			id;
  ZvSeqNo		seqNo = 0;
  Telemetry::Containers	telemetry;
  SrvLink		*srvLink = nullptr;
  bool			connecting = false; // prevent overlapping connects
};
struct CliLink_KeyAccessor {
  static CliLink_::Key get(const CliLink_ &link) { return link.key(); }
};
struct CliLink_HeapID {
  static constexpr const char *id() { return "CliLink"; }
};
using CliLinks =
  ZmRBTree<CliLink_,
    ZmRBTreeKey<CliLink_KeyAccessor,
      ZmRBTreeUnique<true,
	ZmRBTreeObject<ZmPolymorph,
	  ZmRBTreeNodeDerive<true,
	    ZmRBTreeLock<ZmPLock,
	      ZmRBTreeHeapID<CliLink_HeapID>>>>>>>;
using CliLink = CliLinks::Node;

class SrvLink : public ZvCmdSrvLink<App_Srv, SrvLink> {
public:
  using Base = ZvCmdSrvLink<App_Srv, SrvLink>;
  SrvLink(App_Srv *app);

  int processCmd(const uint8_t *data, unsigned len);
  int processDeflt(ZuID id, const uint8_t *data, unsigned len);

  CliLink		*cliLink = nullptr;
};

class App;

class App_Cli : public ZvCmdClient<App_Cli, CliLink_> { };
class App_Srv : public ZvCmdServer<App_Srv, SrvLink> {
public:
  void telemetry(ZvTelemetry::App &data);
};

class App :
    public ZmPolymorph,
    public App_Cli,
    public App_Srv,
    public ZGtk::App {
public:
  using Client = ZvCmdClient<App_Cli, CliLink_>;
  using FBB = typename Client::FBB;
  using Server = ZvCmdServer<App_Srv, SrvLink>;
  using User = Server::User;

  class TelRing : public ZiRing {
    TelRing() = delete;
    TelRing(const TelRing &) = delete;
    TelRing &operator =(const TelRing &) = delete;
    TelRing(TelRing &&) = delete;
    TelRing &operator =(TelRing &&) = delete;
  public:
    ~TelRing() = default;
    TelRing(const ZiRingParams &params) :
	ZiRing{[](const void *ptr) -> unsigned {
	  return *static_cast<const uint16_t *>(ptr) + sizeof(uintptr_t) + 2;
	}, params} { }
    bool push(CliLink_ *cliLink, ZuArray<const uint8_t> msg) {
      unsigned n = msg.length();
      if (void *ptr = ZiRing::push(n + sizeof(uintptr_t) + 2)) {
	*static_cast<uintptr_t *>(ptr) = reinterpret_cast<uintptr_t>(cliLink);
	ptr = static_cast<uint8_t *>(ptr) + sizeof(uintptr_t);
	*static_cast<uint16_t *>(ptr) = n;
	ptr = static_cast<uint8_t *>(ptr) + 2;
	memcpy(static_cast<uint8_t *>(ptr), msg.data(), n);
	push2();
	return true;
      }
      auto i = writeStatus();
      if (i < 0)
	ZeLOG(Error, ZtString{} <<
	    "ZiRing::push() failed - " << Zi::resultName(i));
      else
	ZeLOG(Error, ZtString{} <<
	    "ZiRing::push() failed - writeStatus=" << i);
      return false;
    }
    template <typename L>
    bool shift(L l) {
      if (const void *ptr = ZiRing::shift()) {
	CliLink_ *cliLink =
	  reinterpret_cast<CliLink_ *>(*static_cast<const uintptr_t *>(ptr));
	ptr = static_cast<const uint8_t *>(ptr) + sizeof(uintptr_t);
	unsigned n = *static_cast<const uint16_t *>(ptr);
	ptr = static_cast<const uint8_t *>(ptr) + 2;
	l(cliLink, ZuArray{static_cast<const uint8_t *>(ptr), n});
	shift2();
	return true;
      }
      return false;
    }
  };

  template <typename T> using TelItem = Telemetry::Item<T>;

  using AppItem = TelItem<ZvTelemetry::App>;
  using DBEnvItem = TelItem<ZvTelemetry::DBEnv>;

  void init(ZiMultiplex *mx, const ZvCf *cf) {
    if (ZmRef<ZvCf> telRingCf = cf->subset("telRing"))
      m_telRingParams.init(telRingCf);
    else
      m_telRingParams.name("zdash").size(131072);
    // 131072 is ~100mics at 1Gbit/s
    m_telRing = new TelRing{m_telRingParams};
    {
      ZeError e;
      if (m_telRing->open(
	    ZiRing::Read | ZiRing::Write | ZiRing::Create, &e) != Zi::OK)
	throw ZtString{} << m_telRingParams.name() << ": " << e;
      int r;
      if ((r = m_telRing->reset()) != Zi::OK)
	throw ZtString{} << m_telRingParams.name() <<
	  ": reset failed - " << Zi::resultName(r);
    }

    m_role = cf->getEnum<ZvTelemetry::AppRole::Map>(
	"appRole", false, ZvTelemetry::AppRole::Dev);

    m_gladePath = cf->get("gtkGlade", true);
    m_stylePath = cf->get("gtkStyle");

    {
      int64_t refreshRate =
	cf->getInt64("gtkRefresh", 1, 60000, false, 1) * (int64_t)1000000;
      m_refreshQuantum = ZmTime{ZmTime::Nano, refreshRate>>1};
      if (m_refreshQuantum < mx->params().quantum()) {
	m_refreshQuantum = mx->params().quantum();
	m_refreshRate = m_refreshQuantum + m_refreshQuantum;
      } else
	m_refreshRate = ZmTime{ZmTime::Nano, refreshRate};
    }
    unsigned gtkTID;
    {
      unsigned nThreads = mx->params().nThreads();
      m_tid = cf->getInt("thread", 1, nThreads, true);
      gtkTID = cf->getInt("gtkThread", 1, nThreads, true);
    }

    // both server and client are initialized with the same mx, cf
    // so that all command and cxn processing on both sides is
    // handled by the same thread

    Server::init(mx, cf);
    static_cast<Server *>(this)->Dispatcher::map("zdash",
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<SrvLink *>(link)->processCmd(data, len);
	});
    static_cast<Server *>(this)->Dispatcher::deflt(
	[](void *link, ZuID id, const uint8_t *data, unsigned len) {
	  return static_cast<SrvLink *>(link)->processDeflt(id, data, len);
	});

    for (unsigned i = 0; i < CmdPerm::N; i++)
      m_cmdPerms[i] = findPerm(
	  ZtString{} << "ZDash." <<
	  fbs::EnumNamesReqData()[i - CmdPerm::Offset]);

    Client::init(mx, cf);
    static_cast<Client *>(this)->Dispatcher::deflt(
	[](void *link, ZuID id, const uint8_t *data, unsigned len) {
	  return static_cast<CliLink_ *>(link)->processDeflt(id, data, len);
	});

    ZmTrap::sigintFn(ZmFn<>{this, [](App *this_) { this_->post(); }});
    ZmTrap::trap();

    m_uptime.now();

    i18n(
	cf->get("i18n_domain", false, "zdash"),
	cf->get("dataDir", false, DATADIR));

    attach(mx, gtkTID);
    mx->run(gtkTID, [this]() { gtkInit(); });
  }

  void final() {
    detach(ZmFn<>{this, [](App *this_) {
      this_->gtkFinal();
      this_->m_executed.post();
    }});
    m_executed.wait();

    m_telRing->close();

    Client::final();
    Server::final();
  }

  void gtkInit() {
    gtk_init(nullptr, nullptr);

    auto builder = gtk_builder_new();
    GError *e = nullptr;

    if (!gtk_builder_add_from_file(builder, m_gladePath, &e)) {
      if (e) {
	ZeLOG(Error, e->message);
	g_error_free(e);
      }
      post();
      return;
    }

    m_mainWindow = GTK_WINDOW(gtk_builder_get_object(builder, "window"));
    auto view_ = GTK_TREE_VIEW(gtk_builder_get_object(builder, "treeview"));
    // m_watchlist = GTK_TREE_VIEW(gtk_builder_get_object(builder, "watchlist"));
    g_object_unref(G_OBJECT(builder));

    if (m_stylePath) {
      auto file = g_file_new_for_path(m_stylePath);
      auto provider = gtk_css_provider_new();
      g_signal_connect(G_OBJECT(provider), "parsing-error",
	  ZGtk::callback([](
	      GtkCssProvider *, GtkCssSection *,
	      GError *e, gpointer) { ZeLOG(Error, e->message); }), 0);
      gtk_css_provider_load_from_file(provider, file, nullptr);
      g_object_unref(G_OBJECT(file));
      m_styleContext = gtk_style_context_new();
      gtk_style_context_add_provider(
	  m_styleContext, GTK_STYLE_PROVIDER(provider), G_MAXUINT);
      g_object_unref(G_OBJECT(provider));
    }

    m_gtkModel = GtkTree::Model::ctor();
    m_gtkView.init(view_, m_styleContext);
    m_gtkView.bind(GTK_TREE_MODEL(m_gtkModel));

    m_mainDestroy = g_signal_connect(
	G_OBJECT(m_mainWindow), "destroy",
	ZGtk::callback([](GObject *, gpointer this_) {
	  reinterpret_cast<App *>(this_)->gtkDestroyed();
	}), reinterpret_cast<gpointer>(this));

    gtk_widget_show_all(GTK_WIDGET(m_mainWindow));

    gtk_window_present(m_mainWindow);

    m_telRing->attach();
  }

  void gtkDestroyed() {
    m_mainWindow = nullptr;
    post();
  }

  void gtkFinal() {
    m_telRing->detach();

    ZGtk::App::sched()->del(&m_refreshTimer);

    if (m_mainWindow) {
      if (m_mainDestroy)
	g_signal_handler_disconnect(G_OBJECT(m_mainWindow), m_mainDestroy);
      gtk_window_close(m_mainWindow);
      gtk_widget_destroy(GTK_WIDGET(m_mainWindow));
      m_mainWindow = nullptr;
    }
    m_gtkView.final();
    if (m_gtkModel) g_object_unref(G_OBJECT(m_gtkModel));
    if (m_styleContext) g_object_unref(G_OBJECT(m_styleContext));
  }

  void post() { m_done.post(); }
  void wait() { m_done.wait(); }

  void telemetry(ZvTelemetry::App &data) {
    using namespace ZvTelemetry;
    data.id = "ZDash";
    data.version = ZuVerName();
    data.uptime = m_uptime;
    data.role = m_role;
    data.rag = RAG::Green;
  }

  template <typename ...Args>
  void gtkRun(Args &&... args) {
    ZGtk::App::run(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void gtkInvoke(Args &&... args) {
    ZGtk::App::invoke(ZuFwd<Args>(args)...);
  }

  void loggedIn(CliLink_ *cliLink) {
    cliLink->connecting = false;
  }

  void disconnected(CliLink_ *cliLink) {
    cliLink->connecting = false;
    if (auto srvLink = cliLink->srvLink) srvLink->cliLink = nullptr;
    cliLink->srvLink = nullptr;
    m_telRing->push(cliLink, ZuArray<const uint8_t>{});
  }
  void disconnected2(CliLink_ *cliLink) {
    // FIXME - update App RAG to red (in caller)
  }

  void connectFailed(CliLink_ *cliLink, bool transient) {
    cliLink->connecting = false;
  }

  void disconnected(SrvLink *srvLink) {
    auto i = m_cliLinks.readIterator();
    while (auto cliLink = i.iterate())
      if (cliLink->srvLink == srvLink)
	cliLink->srvLink = nullptr;
    srvLink->cliLink = nullptr;
  }

  int processTelemetry(CliLink_ *cliLink, const uint8_t *data, unsigned len) {
    using namespace Zfb;
    {
      Verifier verifier(data, len);
      if (!ZvTelemetry::fbs::VerifyTelemetryBuffer(verifier)) return -1;
    }
    if (m_telRing->push(cliLink, {data, len}))
      if (!m_telCount++)
	gtkRun(ZmFn<>{this, [](App *this_) { this_->gtkRefresh(); }},
	    ZmTimeNow() + m_refreshRate, ZmScheduler::Advance, &m_refreshTimer);
    return len;
  }

  int rejectCmd(SrvLink *srvLink, unsigned len, uint64_t seqNo,
      unsigned code, ZtString text) {
    auto text_ = Zfb::Save::str(m_fbb, text);
    fbs::ReqAckBuilder fbb_(m_fbb);
    fbb_.add_seqNo(seqNo);
    fbb_.add_rejCode(code);
    fbb_.add_rejText(text_);
    m_fbb.Finish(fbb_.Finish());
    ZvCmd::saveHdr(m_fbb, m_id);
    srvLink->send_(m_fbb.buf());
    return len;
  }

  int processCmd(SrvLink *srvLink, const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;

    {
      Verifier verifier(data, len);
      if (!fbs::VerifyRequestBuffer(verifier)) return -1;
    }

    auto request_ = fbs::GetRequest(data);
    uint64_t seqNo = request_->seqNo();
    int reqType = request_->data_type();

    {
      auto perm = m_cmdPerms[CmdPerm::Offset + reqType];
      if (ZuUnlikely(perm < 0)) {
	ZtString permName;
	permName << "ZDash." << fbs::EnumNamesReqData()[reqType];
	perm = m_cmdPerms[CmdPerm::Offset + reqType] = findPerm(permName);
	if (ZuUnlikely(perm < 0)) {
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "permission denied (\"" << permName << "\" missing)");
	}
      }
      if (ZuUnlikely(!ok(srvLink->user(), srvLink->interactive(), perm))) {
	ZtString text = "permission denied";
	if (srvLink->user()->flags & ZvUserDB::User::ChPass)
	  text << " (user must change password)";
	return rejectCmd(srvLink, len, seqNo, __LINE__, ZuMv(text));
      }
    }

    const void *reqData_ = request_->data();
    fbs::ReqAckData ackType = fbs::ReqAckData_NONE;
    Offset<void> ackData = 0;

    switch (reqType) {
      case fbs::ReqData_Version:
	ackType = fbs::ReqAckData_VersionAck;
	ackData = fbs::CreateVersion(m_fbb,
	    Save::str(m_fbb, ZuVerName())).Union();
	break;
      case fbs::ReqData_MkLink: {
	auto reqData = static_cast<const fbs::LinkData *>(reqData_);
	auto cliLink =
	  new CliLink{this, m_cliLinkID++,
	    str(reqData->server()), reqData->port(), srvLink};
	m_cliLinks.add(cliLink);
	cliLink->srvLink = srvLink;
	srvLink->cliLink = cliLink;
	ackType = fbs::ReqAckData_MkLinkAck;
	ackData = fbs::CreateLink(m_fbb, true, cliLink->id,
	    fbs::CreateLinkData(m_fbb,
	      Save::str(m_fbb, cliLink->server()), cliLink->port())).Union();
      } break;
      case fbs::ReqData_RmLink: {
	auto reqData = static_cast<const fbs::LinkID *>(reqData_);
	auto cliLink = m_cliLinks.del(reqData->id());
	if (!cliLink)
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "unknown link " << reqData->id());
	ackType = fbs::ReqAckData_RmLinkAck;
	ackData = fbs::CreateLink(m_fbb, false, cliLink->id,
	    fbs::CreateLinkData(m_fbb,
	      Save::str(m_fbb, cliLink->server()), cliLink->port())).Union();
      } break;
      case fbs::ReqData_Connect: {
	auto reqData = static_cast<const fbs::Connect *>(reqData_);
	auto cliLink = m_cliLinks.findPtr(reqData->link()->id());
	if (!cliLink)
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "unknown link " << reqData->link()->id());
	if (cliLink->connecting)
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "connect in progress " << reqData->link()->id());
	cliLink->connecting = true;
	cliLink->srvLink = srvLink;
	srvLink->cliLink = cliLink;
	auto loginReq = reqData->loginReq();
	switch ((int)loginReq->data_type()) {
	  case ZvUserDB::fbs::LoginReqData_Login: {
	    auto login =
	      static_cast<const ZvUserDB::fbs::Login *>(loginReq->data());
	    cliLink->login(
		str(login->user()), str(login->passwd()), login->totp());
	  } break;
	  case ZvUserDB::fbs::LoginReqData_Access: {
	    auto access =
	      static_cast<const ZvUserDB::fbs::Access *>(loginReq->data());
	    cliLink->access_(
		str(access->keyID()),
		bytes(access->token()),
		access->stamp(),
		bytes(access->hmac()));
	  } break;
	  default:
	    return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
		"unknown credentials type " << loginReq->data_type());
	}
	ackType = fbs::ReqAckData_ConnectAck;
	ackData = fbs::CreateLink(m_fbb,
	    cliLink->srvLink == srvLink, cliLink->id,
	    fbs::CreateLinkData(m_fbb,
	      Save::str(m_fbb, cliLink->server()), cliLink->port())).Union();
      } break;
      case fbs::ReqData_Disconnect: {
	auto reqData = static_cast<const fbs::LinkID *>(reqData_);
	auto cliLink = m_cliLinks.findPtr(reqData->id());
	if (!cliLink)
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "unknown link " << reqData->id());
	cliLink->disconnect();
	ackType = fbs::ReqAckData_ConnectAck;
	ackData = fbs::CreateLink(m_fbb,
	    cliLink->srvLink == srvLink, cliLink->id,
	    fbs::CreateLinkData(m_fbb,
	      Save::str(m_fbb, cliLink->server()), cliLink->port())).Union();
      } break;
      case fbs::ReqData_Links: {
	ZtArray<Zfb::Offset<fbs::Link>> v;
	auto i = m_cliLinks.readIterator();
	while (auto cliLink = i.iterate())
	  v.push(fbs::CreateLink(m_fbb,
		cliLink->srvLink == srvLink, cliLink->id,
		fbs::CreateLinkData(m_fbb,
		  Save::str(m_fbb, cliLink->server()), cliLink->port())));
	auto list_ = m_fbb.CreateVector(v.data(), v.length());
	ackType = fbs::ReqAckData_LinksAck;
	ackData = fbs::CreateLinkList(m_fbb, list_).Union();
      } break;
      case fbs::ReqData_Select: {
	auto reqData = static_cast<const fbs::LinkID *>(reqData_);
	auto cliLink = m_cliLinks.findPtr(reqData->id());
	if (!cliLink)
	  return rejectCmd(srvLink, len, seqNo, __LINE__, ZtString{} <<
	      "unknown link " << reqData->id());
	cliLink->srvLink = srvLink;
	srvLink->cliLink = cliLink;
	ackType = fbs::ReqAckData_SelectAck;
	ackData = fbs::CreateLink(m_fbb, true, cliLink->id,
	    fbs::CreateLinkData(m_fbb,
	      Save::str(m_fbb, cliLink->server()), cliLink->port())).Union();
      } break;
    }

    {
      fbs::ReqAckBuilder fbb_(m_fbb);
      fbb_.add_seqNo(seqNo);
      fbb_.add_data_type(ackType);
      fbb_.add_data(ackData);
      m_fbb.Finish(fbb_.Finish());
    }
    ZvCmd::saveHdr(m_fbb, m_id);
    srvLink->send_(m_fbb.buf());
    return len;
  }

  // fwd unknown app messages client <-> server using client-selected
  // server-side link (from zdash perspective, SrvLink selects CliLink)
  int processDeflt(
      CliLink_ *cliLink, ZuID, const uint8_t *data, unsigned len) {
    if (auto srvLink = cliLink->srvLink)
      srvLink->send_(data - sizeof(ZvCmd::Hdr), len + sizeof(ZvCmd::Hdr));
    return len;
  }
  int processDeflt(
      SrvLink *srvLink, ZuID id, const uint8_t *data, unsigned len) {
    if (auto cliLink = srvLink->cliLink)
      cliLink->send_(data - sizeof(ZvCmd::Hdr), len + sizeof(ZvCmd::Hdr));
    return len;
  }

private:
  void gtkRefresh() {
    // FIXME - freeze, save sort col, unset sort col

    bool busy = false;
    ZmTime deadline = ZmTimeNow() + m_refreshQuantum;
    unsigned i = 0, n;
    while (m_telRing->shift([](
	    CliLink_ *cliLink, const ZuArray<const uint8_t> &msg) {
      static_cast<App *>(cliLink->app())->processTel2(cliLink, msg);
    })) {
      n = --m_telCount;
      if (ZuUnlikely(n == ~0U)) n = m_telCount = 0;
      if (!(++i & 0xf) && ZmTimeNow() >= deadline) {
	busy = true;
	break;
      }
    }

    // FIXME - restore sort col, thaw

    if (busy && n)
      gtkRun(ZmFn<>{this, [](App *this_) { this_->gtkRefresh(); }},
	  ZmTimeNow() + m_refreshRate, ZmScheduler::Defer, &m_refreshTimer);
  }
  void processTel2(CliLink_ *cliLink, const ZuArray<const uint8_t> &msg_) {
    if (ZuUnlikely(!msg_)) {
      disconnected2(cliLink);
      return;
    }
    using namespace ZvTelemetry;
    auto msg = ZvTelemetry::fbs::GetTelemetry(msg_);
    int i = msg->data_type();
    if (ZuUnlikely(i < TelData::First)) return;
    if (ZuUnlikely(i > TelData::MAX)) return;
    ZuSwitch::dispatch<TelData::N - TelData::First>(i - TelData::First,
	[this, cliLink, msg](auto i) {
      using FBType = TelData::Type<i + TelData::First>;
      auto fbo = static_cast<const FBType *>(msg->data());
      processTel3(cliLink, fbo);
    });
  }
  template <typename FBType>
  ZuIsNot<ZvTelemetry::fbs::Alert, FBType>
  processTel3(CliLink_ *cliLink, const FBType *fbo) {
    ZuConstant<ZuTypeIndex<FBType, Telemetry::FBTypeList>::I> i;
    using T = ZuType<i, Telemetry::TypeList>;
    auto &container = cliLink->telemetry.p<i>();
    using Item = TelItem<T>;
    Item *item;
    if (item = container.lookup(fbo)) {
      ZfbField::loadUpdate(item->data, fbo);
      m_gtkModel->updated(GtkTree::row(item));
    } else {
      item = new Item{cliLink, fbo};
      container.add(item);
      addGtkRow(cliLink, item);
    }
  }
  void addGtkRow(CliLink_ *cliLink, AppItem *item) {
    item->initTelKey(cliLink->server(), cliLink->port());
    m_gtkModel->add(new GtkTree::App{item}, m_gtkModel->root());
  }
  AppItem *appItem(CliLink_ *cliLink) {
    ZuConstant<ZuTypeIndex<ZvTelemetry::App, ZvTelemetry::TypeList>::I> i;
    auto &container = cliLink->telemetry.p<i>();
    auto item = container.lookup(
	static_cast<const ZvTelemetry::fbs::App *>(nullptr));
    if (!item) {
      item = new TelItem<ZvTelemetry::App>{cliLink};
      container.add(item);
      addGtkRow(cliLink, item);
    }
    return item;
  }
  DBEnvItem *dbEnvItem(CliLink_ *cliLink) {
    ZuConstant<ZuTypeIndex<ZvTelemetry::DBEnv, ZvTelemetry::TypeList>::I> i;
    auto &container = cliLink->telemetry.p<i>();
    auto item = container.lookup(
	static_cast<const ZvTelemetry::fbs::DBEnv *>(nullptr));
    if (!item) {
      item = new TelItem<ZvTelemetry::DBEnv>{cliLink};
      container.add(item);
      addGtkRow(cliLink, item);
    }
    return item;
  }
  template <typename Item, typename ParentFn>
  void addGtkRow_(AppItem *appItem, Item *item, ParentFn parentFn) {
    auto appGtkRow = GtkTree::row(appItem);
    auto &parent = parentFn(appGtkRow);
    if (parent.row() < 0) m_gtkModel->add(&parent, appGtkRow);
    using GtkRow = ZuDecay<decltype(*GtkTree::row(item))>;
    m_gtkModel->add(new GtkRow{item}, &parent);
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Heap> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::HeapParent & {
	  return _->heaps();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::HashTbl> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::HashTblParent & {
	  return _->hashTbls();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Thread> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::ThreadParent & {
	  return _->threads();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Mx> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::MxParent & { return _->mxs(); });
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Socket> *item) {
    ZuConstant<ZuTypeIndex<ZvTelemetry::Mx, ZvTelemetry::TypeList>::I> i;
    auto &mxContainer = cliLink->telemetry.p<i>();
    auto mxItem = mxContainer.find(ZuFwdTuple(item->data.mxID));
    if (!mxItem) {
      auto mxItem = new TelItem<ZvTelemetry::Mx>{cliLink};
      mxItem->data.id = item->data.mxID;
      mxContainer.add(mxItem);
      addGtkRow(cliLink, mxItem);
    }
    m_gtkModel->add(new GtkTree::Socket{item}, GtkTree::row(mxItem));
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Queue> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::QueueParent & {
	  return _->queues();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Engine> *item) {
    addGtkRow_(appItem(cliLink), item,
	[](GtkTree::App *_) -> GtkTree::EngineParent & {
	  return _->engines();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::Link> *item) {
    ZuConstant<ZuTypeIndex<ZvTelemetry::Engine, ZvTelemetry::TypeList>::I> i;
    auto &engContainer = cliLink->telemetry.p<i>();
    auto engItem =
      engContainer.find(ZuFwdTuple(item->data.engineID));
    if (!engItem) {
      auto engItem = new TelItem<ZvTelemetry::Mx>{cliLink};
      engItem->data.id = item->data.engineID;
      engContainer.add(engItem);
      addGtkRow(cliLink, engItem);
    }
    m_gtkModel->add(new GtkTree::Link{item}, GtkTree::row(engItem));
  }
  void addGtkRow(CliLink_ *cliLink, DBEnvItem *item) {
    auto appGtkRow = GtkTree::row(appItem(cliLink));
    auto &dbEnv = appGtkRow->dbEnv();
    dbEnv.init(item);
    m_gtkModel->add(&dbEnv, appGtkRow);
  }
  template <typename Item, typename ParentFn>
  void addGtkRow_(DBEnvItem *dbEnvItem, Item *item, ParentFn parentFn) {
    auto dbEnvGtkRow = GtkTree::row(dbEnvItem);
    auto &parent = parentFn(dbEnvGtkRow);
    if (parent.row() < 0) m_gtkModel->add(&parent, dbEnvGtkRow);
    using GtkRow = ZuDecay<decltype(*GtkTree::row(item))>;
    m_gtkModel->add(new GtkRow{item}, &parent);
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::DBHost> *item) {
    addGtkRow_(dbEnvItem(cliLink), item,
	[](GtkTree::DBEnv *_) -> GtkTree::DBHostParent & {
	  return _->hosts();
	});
  }
  void addGtkRow(CliLink_ *cliLink, TelItem<ZvTelemetry::DB> *item) {
    addGtkRow_(dbEnvItem(cliLink), item,
	[](GtkTree::DBEnv *_) -> GtkTree::DBParent & {
	  return _->dbs();
	});
  }
  template <typename FBType>
  ZuIs<ZvTelemetry::fbs::Alert, FBType>
  processTel3(CliLink_ *cliLink, const FBType *fbo) {
    ZuConstant<ZuTypeIndex<FBType, Telemetry::FBTypeList>::I> i;
    using T = ZuType<i, Telemetry::TypeList>;
    auto &container = cliLink->telemetry.p<i>();
    processAlert(new (container.data.push()) ZfbField::Load<T>{fbo});
  }

  void processAlert(const ZvTelemetry::Alert *) {
    // FIXME - update alerts in UX
  }

private:
  ZmSemaphore		m_done;
  ZmSemaphore		m_executed;

  CliLink::ID		m_cliLinkID = 0;
  CliLinks		m_cliLinks;

  struct CmdPerm {
    enum {
      Offset = -(fbs::ReqData_NONE + 1),
      N = fbs::ReqData_MAX - fbs::ReqData_NONE
    };
  };
  int			m_cmdPerms[CmdPerm::N];
  ZuID			m_id = "zdash";
  FBB			m_fbb;

  int			m_role;	// ZvTelemetry::AppRole
  ZtDate		m_uptime;
  unsigned		m_tid = 0;

  ZvRingParams		m_telRingParams;
  ZuPtr<TelRing>	m_telRing;
  ZmAtomic<unsigned>	m_telCount = 0;

  // FIXME - need Zdf in-memory manager (later, file manager)

  ZtString		m_gladePath;
  ZtString		m_stylePath;
  GtkStyleContext	*m_styleContext = nullptr;
  GtkWindow		*m_mainWindow = nullptr;
  gulong		m_mainDestroy = 0;

  ZmTime		m_refreshQuantum;
  ZmTime		m_refreshRate;
  ZmScheduler::Timer	m_refreshTimer;

  GtkTree::View		m_gtkView;
  GtkTree::Model	*m_gtkModel;
};

inline void App_Srv::telemetry(ZvTelemetry::App &data)
{
  static_cast<ZDash::App *>(this)->telemetry(data);
}

template <typename Server>
inline CliLink_::CliLink_(
    App_Cli *app, ID id_, Server &&server, uint16_t port, SrvLink *srvLink_) :
    Base{app, ZuFwd<Server>(server), port}, id{id_}, srvLink{srvLink_} { }

inline void CliLink_::loggedIn()
{
  static_cast<ZDash::App *>(this->app())->loggedIn(this);
}
inline void CliLink_::disconnected()
{
  static_cast<ZDash::App *>(this->app())->disconnected(this);
  Base::disconnected();
}
inline void CliLink_::connectFailed(bool transient)
{
  static_cast<ZDash::App *>(this->app())->connectFailed(this, transient);
}

inline int CliLink_::processTelemetry(const uint8_t *data, unsigned len)
{
  return static_cast<ZDash::App *>(
      this->app())->processTelemetry(this, data, len);
}
inline int CliLink_::processDeflt(ZuID id, const uint8_t *data, unsigned len)
{
  return static_cast<ZDash::App *>(
      this->app())->processDeflt(this, id, data, len);
}

inline SrvLink::SrvLink(App_Srv *app) : Base{app} { }

inline int SrvLink::processCmd(const uint8_t *data, unsigned len)
{
  return static_cast<ZDash::App *>(
      this->app())->processCmd(this, data, len);
}
inline int SrvLink::processDeflt(ZuID id, const uint8_t *data, unsigned len)
{
  return static_cast<ZDash::App *>(
      this->app())->processDeflt(this, id, data, len);
}

} // namespace ZDash

int main(int argc, char **argv)
{
  if (argc != 1) usage();

  ZeLog::init("zcmd");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::lambdaSink(
	[](ZeEvent *e) { std::cerr << e->message() << '\n' << std::flush; }));
  ZeLog::start();

  ZiMultiplex *mx = new ZiMultiplex(
      ZiMxParams()
	.scheduler([](auto &s) {
	  s.nThreads(5)
	  .thread(1, [](auto &t) { t.isolated(1); })
	  .thread(2, [](auto &t) { t.isolated(1); })
	  .thread(3, [](auto &t) { t.isolated(1); })
	  .thread(4, [](auto &t) { t.isolated(1); }); })
	.rxThread(1).txThread(2));

  mx->start();

  ZmRef<ZDash::App> app = new ZDash::App{};

  {
    ZmRef<ZvCf> cf = new ZvCf();
    cf->set("timeout", "1");
    cf->set("thread", "3");
    cf->set("gtkThread", "4");
    cf->set("gtkGlade", "zdash.glade");
    if (auto caPath = ::getenv("ZCMD_CAPATH"))
      cf->set("caPath", caPath);
    else
      cf->set("caPath", "/etc/ssl/certs");
    try {
      app->init(mx, cf);
    } catch (const ZvError &e) {
      std::cerr << e << '\n' << std::flush;
      ::exit(1);
    } catch (const ZtString &e) {
      std::cerr << e << '\n' << std::flush;
      ::exit(1);
    } catch (...) {
      std::cerr << "unknown exception\n" << std::flush;
      ::exit(1);
    }
  }

  app->wait();

  app->final();

  mx->stop(true);

  ZeLog::stop();

  delete mx;

  ZmTrap::sigintFn({});
}
