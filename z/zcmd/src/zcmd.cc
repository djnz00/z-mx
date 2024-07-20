//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#include <zlib/ZuPolymorph.hh>
#include <zlib/ZuByteSwap.hh>
#include <zlib/ZuBase32.hh>
#include <zlib/ZuBase64.hh>

#include <zlib/ZmPlatform.hh>
#include <zlib/ZmTrap.hh>

#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiModule.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvCSV.hh>

#include <zlib/ZtlsTOTP.hh>

#include <zlib/ZrlCLI.hh>
#include <zlib/ZrlGlobber.hh>
#include <zlib/ZrlHistory.hh>

#include <zlib/Zum.hh>

#include <zlib/ZcmdClient.hh>
#include <zlib/ZcmdHost.hh>

#ifdef _MSC_VER
#pragma warning(disable:4800)
#endif

static void usage()
{
  static const char *usage =
    "Usage: zcmd [USER@][HOST:]PORT [CMD [ARGS]]\n"
    "  USER\t- user (not needed if API key used)\n"
    "  HOST\t- target host (default localhost)\n"
    "  PORT\t- target port\n"
    "  CMD\t- command to send to target\n"
    "  \t  (reads commands from standard input if none specified)\n"
    "  ARGS\t- command arguments\n\n"
    "Environment Variables:\n"
    "  ZCMD_PASSWD\t\tpassword\n"
    "  ZCMD_TOTP_SECRET\tTOTP secret\n"
    "  ZCMD_KEY_ID\t\tAPI key ID\n"
    "  ZCMD_KEY_SECRET\tAPI key secret\n"
    "  ZCMD_CAPATH\t\tCA for validating server TLS certificate\n"
    "  ZCMD_PLUGIN\t\tzcmd plugin module\n";
  std::cerr << usage << std::flush;
  ZeLog::stop();
  Zm::exit(1);
}

class TelCap {
public:
  using Fn = ZmFn<void(const void *)>;

  TelCap() { }
  TelCap(Fn fn) : m_fn{ZuMv(fn)} { }
  TelCap(TelCap &&o) : m_fn{ZuMv(o.m_fn)} { }
  TelCap &operator =(TelCap &&o) {
    m_fn(nullptr);
    m_fn = ZuMv(o.m_fn);
    return *this;
  }
  ~TelCap() { m_fn(nullptr); }

  template <typename Data_>
  static TelCap keyedFn(ZtString path) {
    using Data = ZfbField::Load<Data_>;
    using FBType = ZfbType<Data>;
    using Tree_ =
      ZmRBTree<Data,
	ZmRBTreeKey<ZuFieldAxor<Data>(),
	  ZmRBTreeUnique<true>>>;
    struct Tree : public ZuObject, public Tree_ { };
    ZmRef<Tree> tree = new Tree{};
    return TelCap{[
	tree = ZuMv(tree),
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbo_) mutable {
      if (!fbo_) {
	l(nullptr);
	tree->clean();
	return;
      }
      auto fbo = static_cast<const FBType *>(fbo_);
      auto node = tree->find(ZuFieldKey(*fbo));
      if (!node)
	tree->addNode(node = new typename Tree::Node{fbo});
      else
	ZfbField::update(node->data(), fbo);
      l(&(node->data()));
    }};
  }

  template <typename Data_>
  static TelCap singletonFn(ZtString path) {
    using Data = ZfbField::Load<Data_>;
    using FBType = ZfbType<Data>;
    return TelCap{[
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbo_) mutable {
      if (!fbo_) {
	l(nullptr);
	return;
      }
      auto fbo = static_cast<const FBType *>(fbo_);
      static Data *data = nullptr;
      if (!data)
	data = new Data{fbo};
      else
	ZfbField::update(*data, fbo);
      l(data);
    }};
  }

  template <typename Data_>
  static TelCap alertFn(ZtString path) {
    using Data = ZfbField::Load<Data_>;
    using FBType = ZfbType<Data>;
    return TelCap{[
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbo_) mutable {
      if (!fbo_) {
	l(nullptr);
	return;
      }
      auto fbo = static_cast<const FBType *>(fbo_);
      Data data{fbo};
      l(&data);
    }};
  }

  void operator ()(const void *p) { m_fn(p); }

private:
  Fn		m_fn;
};

class ZCmd;

class Link : public ZcmdCliLink<ZCmd, Link> {
public:
  using Base = ZcmdCliLink<ZCmd, Link>;
  template <typename Server>
  Link(ZCmd *app, Server &&server, uint16_t port);

  void loggedIn();
  void disconnected();
  void connectFailed(bool transient);

  int processTelemetry(ZmRef<ZiIOBuf>);
};

class ZCmd :
    public ZmPolymorph,
    public ZcmdClient<ZCmd, Link>,
    public ZcmdHost {
public:
  using Base = ZcmdClient<ZCmd, Link>;

friend Link;

  void init(ZiMultiplex *mx, const ZvCf *cf, bool interactive) {
    Base::init(mx, cf);
    m_interactive = interactive;
    ZcmdHost::init();
    initCmds();
    if (m_interactive)
      m_cli.init(Zrl::App{
	.error = [this](ZuString s) { std::cerr << s << '\n'; done(); },
	.prompt = [this](ZtArray<uint8_t> &s) {
	  ZmGuard guard(m_promptLock);
	  if (m_prompt.owned()) s = ZuMv(m_prompt);
	},
	.enter = [this](ZuString s) -> bool {
	  exec(ZtString{s}); // ignore result code
	  return false;
	},
	.end = [this]() { done(); },
	.sig = [](int sig) -> bool {
	  switch (sig) {
	    case SIGINT:
	      raise(sig);
	      return true;
#ifdef _WIN32
	    case SIGQUIT:
	      GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0);
	      return true;
#endif
	    case SIGTSTP:
	      raise(sig);
	      return false;
	    default:
	      return false;
	  }
	},
	.compInit = m_globber.initFn(),
	.compFinal = m_globber.finalFn(),
	.compStart = m_globber.startFn(),
	.compSubst = m_globber.substFn(),
	.compNext = m_globber.nextFn(),
	.histSave = m_history.saveFn(),
	.histLoad = m_history.loadFn()
      });
  }
  void final() {
    m_cli.final();
    for (unsigned i = 0; i < TelDataN; i++) m_telcap[i] = TelCap{};
    m_link = nullptr;
    ZcmdHost::final();
    Base::final();
  }

  bool interactive() const { return m_interactive; }

  void solo(ZtString s) {
    m_solo = true;
    m_soloMsg = ZuMv(s);
  }

  template <typename Server, typename ...Args>
  void login(Server &&server, uint16_t port, Args &&...args) {
    m_cli.open(); // idempotent
    ZtString passwd;
    if (auto passwd_ = ::getenv("ZCMD_PASSWD"))
      passwd = passwd_;
    else
      passwd = m_cli.getpass("password: ", 100);
    if (!passwd) return;
    ZuBox<unsigned> totp;
    if (auto secret_ = ::getenv("ZCMD_TOTP_SECRET")) {
      unsigned n = strlen(secret_);
      ZtArray<uint8_t> secret;
      secret.length(ZuBase32::declen(n));
      secret.length(ZuBase32::decode(secret, {secret_, n}));
      if (secret) totp = Ztls::TOTP::calc(secret);
    } else
      totp = m_cli.getpass("totp: ", 6);
    if (!*totp) return;
    m_link = new Link{this, ZuFwd<Server>(server), port};
    m_link->login(ZuFwd<Args>(args)..., ZuMv(passwd), totp);
  }
  template <typename Server, typename ...Args>
  void access(Server &&server, uint16_t port, Args &&...args) {
    m_link = new Link(this, ZuFwd<Server>(server), port);
    m_link->access(ZuFwd<Args>(args)...);
  }

  void disconnect() { if (m_link) m_link->disconnect(); }

  void wait() { m_done.wait(); }
  void done() { m_done.post(); }

  void sigint() { m_executed.post(); m_done.post(); }

  void exiting() { m_exiting = true; }

  // ZcmdHost virtual functions
  ZcmdDispatcher *dispatcher() { return this; }
  void send(void *link, ZmRef<ZiIOBuf> buf) {
    return static_cast<Link *>(link)->send(ZuMv(buf));
  }
  void target(ZuString s) {
    ZmGuard guard(m_promptLock);
    m_prompt = ZtArray<uint8_t>{} << s << "] ";
  }
  ZtString getpass(ZuString prompt, unsigned passLen) {
    return m_cli.getpass(prompt, passLen);
  }
  Ztls::Random *rng() { return this; }

private:
  void loggedIn() {
    if (auto plugin = ::getenv("ZCMD_PLUGIN")) {
      auto cmd = ZtString{} << "loadmod " << plugin;
      if (exec(cmd) != 0) std::cerr << cmd << " failed\n";
    }
    start();
  }

  void start() {
    if (m_solo) {
      int code = exec(ZuMv(m_soloMsg));
      done();
      Zm::exit(code);
    } else {
      if (m_interactive) {
	std::cout <<
	  "For a list of valid commands: help\n"
	  "For help on a particular command: COMMAND --help\n" << std::flush;
	m_cli.start();
      } else {
	ZtString cmd{4096};
	while (fgets(cmd, cmd.size() - 1, stdin)) {
	  cmd.calcLength();
	  cmd.chomp();
	  if (exec(ZuMv(cmd))) break;
	  cmd.size(4096);
	}
	done();
      }
    }
  }

  enum {
    ReqTypeN = Ztel::ReqType::N,
    TelDataN = Ztel::TelData::N
  };

  int processTelemetry(ZmRef<ZiIOBuf> buf) {
    using namespace Ztel;
    {
      Zfb::Verifier verifier{buf->data(), buf->length};
      if (!fbs::VerifyTelemetryBuffer(verifier)) return -1;
    }
    auto msg = fbs::GetTelemetry(buf->data());
    int i = int(msg->data_type());
    if (ZuUnlikely(i < TelData::MIN)) return 0;
    i -= TelData::MIN;
    if (ZuUnlikely(i >= TelDataN)) return 0;
    m_telcap[i](msg->data());
    return buf->length;
  }

  void disconnected() {
    m_executed.post();
    if (m_interactive) {
      m_cli.stop();
      m_cli.close();
    }
    if (m_exiting) {
      done();
      return;
    }
    if (m_interactive) {
      m_cli.final();
      std::cerr << "server disconnected\n" << std::flush;
    }
    Zm::exit(1);
  }
  void connectFailed() {
    if (m_interactive) {
      m_cli.stop();
      m_cli.close();
      m_cli.final();
      std::cerr << "connect failed\n" << std::flush;
    }
    Zm::exit(1);
  }

  int exec(ZtString cmd) {
    if (!cmd) return 0;
    ZtString cmd_ = ZuMv(cmd);
    ZcmdContext ctx{
      .host = this,
      .seqNo = m_seqNo++,
      .interactive = m_interactive
    };
    {
      ZtRegex::Captures c;
      unsigned pos = 0, n = 0;
      if (n = ZtREGEX("\s*>>\s*").m(cmd, c, pos)) {
	ZtString path{c[2]};
	FILE *file;
	if (!(file = fopen(path, "a"))) {
	  ZeLOG(Error, ([path, e = ZeLastError](auto &s) {
	    s << path << ": " << e;
	  }));
	  return -1;
	}
	ctx.dest = file;
	cmd = c[0];
      } else if (n = ZtREGEX("\s*>\s*").m(cmd, c, pos)) {
	FILE *file;
	ZtString path{c[2]};
	if (!(file = fopen(path, "w"))) {
	  ZeLOG(Error, ([path, e = ZeLastError](auto &s) {
	    s << path << ": " << e;
	  }));
	  return -1;
	}
	ctx.dest = file;
	cmd = c[0];
      } else {
	ctx.dest = stdout;
	cmd = ZuMv(cmd_);
      }
    }
    ZtArray<ZtString> args = ZvCf::parseCLI(cmd);
    if (!args) return 0;
    bool local;
    if (args[0] == "remote") {
      args.shift();
      local = false;
    } else
      local = hasCmd(args[0]);
    if (local)
      processCmd(&ctx, args);
    else
      send(&ctx, args);
    m_executed.wait();
    return ctx.code;
  }

  void send(ZcmdContext *ctx, ZuArray<const ZtString> args) {
    Zfb::IOBuilder fbb;
    fbb.Finish(Zcmd::fbs::CreateRequest(fbb, ctx->seqNo,
	  Zfb::Save::strVecIter(fbb, args.length(),
	    [&args](unsigned i) { return args[i]; })));
    m_link->sendCmd(fbb.buf(), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	  const Zcmd::fbs::ReqAck *ack) mutable {
      ctx.out = Zfb::Load::str(ack->out());
      executed(ack->code(), &ctx);
    });
  }

  using ZcmdHost::executed;
  void executed(ZcmdContext *ctx) {
    auto &file = ctx->dest.p<FILE *>();
    if (const auto &out = ctx->out)
      fwrite(out.data(), 1, out.length(), file);
    fflush(file);
    if (file != stdout) {
      fclose(file);
      file = stdout;
    }
    m_executed.post();
  }

private:
  // built-in commands

  int filterAck(
      ZtString &out,
      const Zum::fbs::ReqAck *ack,
      int ackType1, int ackType2,
      const char *op) {
    using namespace Zum;
    if (ack->rejCode()) {
      out << '[' << ZuBox<unsigned>(ack->rejCode()) << "] "
	<< Zfb::Load::str(ack->rejText()) << '\n';
      return 1;
    }
    auto ackType = ack->data_type();
    if (int(ackType) != ackType1 &&
	ackType2 >= int(fbs::ReqAckData::MIN) && int(ackType) != ackType2) {
      ZeLOG(Error, ([ackType](auto &s) {
	s << "mismatched ack from server: "
	  << fbs::EnumNameReqAckData(ackType);
      }));
      out << op << " failed\n";
      return 1;
    }
    return 0;
  }

  void initCmds() {
    addCmd("passwd", "",
	ZcmdFn::Member<&ZCmd::passwdCmd>::fn(this),
	"change passwd", "Usage: passwd");

    addCmd("users",
        "id i i { param id } "
        "name n n { param name } "
	"exclusive x x { param exclusive } "
	"limit l l { param limit }",
	ZcmdFn::Member<&ZCmd::usersCmd>::fn(this),
	"list users", "Usage: users [OPTIONS...]\n\n"
	"  -i, --id=ID\t\tquery from user ID\n"
	"  -n, --name=NAME\t\tquery from user NAME\n"
	"  -x, --exclusive\texclude ID|NAME from results\n"
	"  -l, --limit=N\t\tlimit results to N\n");
    addCmd("useradd",
	"enabled e e { flag enabled } "
	"immutable i i { flag immutable }",
	ZcmdFn::Member<&ZCmd::userAddCmd>::fn(this),
	"add user",
	"Usage: useradd ID NAME ROLE[,ROLE]... [OPTION]...\n\n"
	"Options:\n"
	"  -e, --enabled\t\tset Enabled flag\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("resetpass", "",
	ZcmdFn::Member<&ZCmd::resetPassCmd>::fn(this),
	"reset password", "Usage: resetpass USERID");
    addCmd("usermod",
	"enabled e e { flag enabled } "
	"immutable i i { flag immutable }",
	ZcmdFn::Member<&ZCmd::userModCmd>::fn(this),
	"modify user",
	"Usage: usermod ID [OPTION]...\n\n"
	"Options:\n"
	"  -n, --name=NAME\tset name\n"
	"  -r, --roles=ROLE[,ROLE]...\tset roles\n"
	"  -e, --enabled=[0|1]\t\tset/clear Enabled flag\n"
	"  -i, --immutable=[0|1]\tset/clear Immutable flag\n");
    addCmd("userdel", "",
	ZcmdFn::Member<&ZCmd::userDelCmd>::fn(this),
	"delete user", "Usage: userdel ID");

    addCmd("roles",
	"exclusive x x { param exclusive } "
	"limit l l { param limit }",
	ZcmdFn::Member<&ZCmd::rolesCmd>::fn(this),
	"list roles", "Usage: roles [NAME] [OPTIONS...]\n\n"
	"  -x, --exclusive\texclude NAME from results\n"
	"  -l, --limit=N\t\tlimit results to N\n");
    addCmd("roleadd", "immutable i i { flag immutable }",
	ZcmdFn::Member<&ZCmd::roleAddCmd>::fn(this),
	"add role",
	"Usage: roleadd NAME PERMS APIPERMS [OPTIONS...]\n\n"
	"Options:\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("rolemod",
	"name n n { param name } "
	"perms p p { param perms } "
	"apiperms a a { param apiperms } "
	"immutable i i { param immutable }",
	ZcmdFn::Member<&ZCmd::roleModCmd>::fn(this),
	"modify role",
	"Usage: rolemod NAME [OPTIONS...]\n\n"
	"Options:\n"
	"  -p, --perms=PERMS\tset permissions\n"
	"  -a, --apiperms=PERMS\tset API permissions\n"
	"  -i, --immutable=[0|1]\tset/clear Immutable flag\n");
    addCmd("roledel", "",
	ZcmdFn::Member<&ZCmd::roleDelCmd>::fn(this),
	"delete role",
	"Usage: roledel NAME");

    addCmd("perms",
        "id i i { param id } "
        "name n n { param name } "
	"exclusive x x { param exclusive } "
	"limit l l { param limit }",
	ZcmdFn::Member<&ZCmd::permsCmd>::fn(this),
	"list permissions", "Usage: perms [OPTIONS...]\n\n"
	"  -i, --id=ID\t\tquery from permission ID\n"
	"  -n, --name=NAME\t\tquery from permission NAME\n"
	"  -x, --exclusive\texclude ID|NAME from results\n"
	"  -l, --limit=N\t\tlimit results to N\n");
    addCmd("permadd", "",
	ZcmdFn::Member<&ZCmd::permAddCmd>::fn(this),
	"add permission", "Usage: permadd NAME");
    addCmd("permmod", "",
	ZcmdFn::Member<&ZCmd::permModCmd>::fn(this),
	"modify permission", "Usage: permmod ID NAME");
    addCmd("permdel", "",
	ZcmdFn::Member<&ZCmd::permDelCmd>::fn(this),
	"delete permission", "Usage: permdel ID");

    addCmd("keys", "",
	ZcmdFn::Member<&ZCmd::keysCmd>::fn(this),
	"list keys", "Usage: keys [USERID]");
    addCmd("keyadd", "",
	ZcmdFn::Member<&ZCmd::keyAddCmd>::fn(this),
	"add key", "Usage: keyadd [USERID]");
    addCmd("keydel", "",
	ZcmdFn::Member<&ZCmd::keyDelCmd>::fn(this),
	"delete key", "Usage: keydel ID");
    addCmd("keyclr", "",
	ZcmdFn::Member<&ZCmd::keyClrCmd>::fn(this),
	"clear all keys", "Usage: keyclr [USERID]");

    addCmd("remote", "",
	ZcmdFn::Member<&ZCmd::remoteCmd>::fn(this),
	"run command remotely", "Usage: remote COMMAND...");

    addCmd("telcap",
	"interval i i { param interval } "
	"unsubscribe u u { flag unsubscribe }",
	ZcmdFn::Member<&ZCmd::telcapCmd>::fn(this),
	"telemetry capture",
	"Usage: telcap [OPTIONS...] PATH [TYPE[:FILTER]]...\n\n"
	"  PATH\tdirectory for capture CSV files\n"
	"  TYPE\t[Heap|HashTbl|Thread|Mx|Queue|Engine|DbEnv|App|Alert]\n"
	"  FILTER\tfilter specification in type-specific format\n\n"
	"Options:\n"
	"  -i, --interval=N\tset scan interval in milliseconds "
	  "(100 <= N <= 1M)\n"
	"  -u, --unsubscribe\tunsubscribe (i.e. end capture)\n");
  }

  void passwdCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 1) throw ZcmdUsage{};
    ZtString oldpw = m_cli.getpass("Current password: ", 100);
    ZtString newpw = m_cli.getpass("New password: ", 100);
    ZtString checkpw = m_cli.getpass("Re-type new password: ", 100);
    if (checkpw != newpw) {
      ctx->out << "passwords do not match\npassword unchanged!\n";
      executed(1, ctx);
      return;
    }
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::ChPass,
	    fbs::CreateUserChPass(fbb,
	      str(fbb, oldpw),
	      str(fbb, newpw)).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::ChPass), -1, "password change")) {
	executed(code, &ctx);
	return;
      }
      out << "password changed\n";
      executed(0, &ctx);
    });
  }

  void usersCmd(ZcmdContext *ctx) {
    ZuUnion<void, uint64_t, ZtString> key;
    bool exclusive;
    unsigned limit;
    try {
      ctx->args->getInt<true>("#", 1, 1);
      if (auto id = ctx->args->get("id"))
	*(key.new_<uint64_t>()) = ZuBox<uint64_t>{id};
      else if (auto name = ctx->args->get("name"))
	new (key.new_<ZtString>()) ZtString{name};
      exclusive = ctx->args->getBool("exclusive", false);
      limit = ctx->args->getInt("limit", 1, Zum::MaxQueryLimit, 1);
    } catch (...) { throw ZcmdUsage{}; }
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      Zfb::Offset<void> fbKey;
      if (key.is<uint64_t>())
	fbKey = fbs::CreateUserID(fbb, key.p<uint64_t>()).Union();
      else if (key.is<ZtString>())
	fbKey = fbs::CreateUserName(fbb,
	  Zfb::Save::str(fbb, key.p<ZtString>())).Union();
      fbs::UserQueryBuilder fbb_(fbb);
      if (key.is<uint64_t>()) {
	fbb_.add_userKey_type(fbs::UserKey::ID);
	fbb_.add_userKey(fbKey);
      } else if (key.is<ZtString>()) {
	fbb_.add_userKey_type(fbs::UserKey::Name);
	fbb_.add_userKey(fbKey);
      }
      fbb_.add_inclusive(!exclusive);
      fbb_.add_limit(limit);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::UserGet, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserGet), -1, "user get")) {
	executed(code, &ctx);
	return;
      }
      auto userList = static_cast<const fbs::UserList *>(ack->data());
      Zfb::Load::all(userList->list(), [&out](unsigned, auto user) {
	out << *user << '\n';
      });
      executed(0, &ctx);
      return;
    });
  }
  void userAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      ZtRegex::Captures roles_;
      ZtREGEX(",").split(ctx->args->get("3"), roles_);
      Zfb::IOBuilder fbb;
      auto name = str(fbb, ctx->args->get("1"));
      auto roles = strVecIter(fbb, roles_.length(),
	[&roles_](unsigned i) { return roles_[i]; });
      uint8_t flags = 0;
      if (ctx->args->get("enabled")) flags |= UserFlags::Enabled();
      if (ctx->args->get("immutable")) flags |= UserFlags::Immutable();
      fbs::UserBuilder fbb_{fbb};
      fbb_.add_name(name);
      fbb_.add_roles(roles);
      fbb_.add_flags(flags);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::UserAdd, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserAdd), -1, "user add")) {
	executed(code, &ctx);
	return;
      }
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      out << *(userPass->user()) << '\n';
      out << "secret="
	<< ZtQuote::Base32{Zfb::Load::bytes(userPass->user()->secret())}
	<< '\n';
      out << "passwd=" << Zfb::Load::str(userPass->passwd()) << '\n';
      executed(0, &ctx);
    });
  }
  void resetPassCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::ResetPass,
	    fbs::CreateUserID(fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX)).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::ResetPass), -1, "reset password")) {
	executed(code, &ctx);
	return;
      }
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      out << *(userPass->user()) << '\n';
      out << "passwd=" << Zfb::Load::str(userPass->passwd()) << '\n';
      executed(0, &ctx);
    });
  }
  void userModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      Zfb::Offset<Zfb::String> name;
      Zfb::Offset<Zfb::Vector<Zfb::Offset<Zfb::String>>> roles;
      if (ctx->args->exists("name"))
	name = str(fbb, ctx->args->get("name"));
      if (ctx->args->exists("roles")) {
	ZtRegex::Captures roles_;
	ZtREGEX(",").split(ctx->args->get("roles"), roles_);
	roles = strVecIter(fbb, roles_.length(),
	  [&roles_](unsigned i) { return roles_[i]; });
      }
      uint8_t flags = 0;
      bool modFlags =
	ctx->args->exists("enabled") || ctx->args->exists("immutable");
      if (modFlags) {
	if (ctx->args->getBool("enabled", false))
	  flags |= UserFlags::Enabled();
	if (ctx->args->getBool("immutable", false))
	  flags |= UserFlags::Immutable();
      }
      fbs::UserBuilder fbb_{fbb};
      fbb_.add_id(ctx->args->getInt64<true>("1", 0, ~uint64_t(0)));
      if (!name.IsNull()) fbb_.add_name(name);
      if (!roles.IsNull()) fbb_.add_roles(roles);
      if (modFlags) fbb_.add_flags(flags);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::UserMod, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserMod), -1, "user modify")) {
	executed(code, &ctx);
	return;
      }
      auto user = static_cast<const fbs::User *>(ack->data());
      out << *user << '\n';
      executed(0, &ctx);
    });
  }
  void userDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::UserDel,
	    fbs::CreateUserID(fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX)).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserDel), -1, "user delete")) {
	executed(code, &ctx);
	return;
      }
      auto user = static_cast<const fbs::User *>(ack->data());
      out << *user << '\n';
      out << "user deleted\n";
      executed(0, &ctx);
    });
  }

  void rolesCmd(ZcmdContext *ctx) {
    unsigned argc;
    bool exclusive;
    unsigned limit;
    try {
      argc = ctx->args->getInt<true>("#", 1, 2);
      exclusive = ctx->args->getBool("exclusive", false);
      limit = ctx->args->getInt("limit", 1, Zum::MaxQueryLimit, 1);
    } catch (...) { throw ZcmdUsage{}; }
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      Zfb::Offset<Zfb::String> name;
      if (argc == 2) name = str(fbb, ctx->args->get("1"));
      fbs::RoleQueryBuilder fbb_(fbb);
      if (argc == 2) fbb_.add_roleKey(name);
      fbb_.add_inclusive(!exclusive);
      fbb_.add_limit(limit);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::RoleGet, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleGet), -1, "role get")) {
	executed(code, &ctx);
	return;
      }
      auto roleList = static_cast<const fbs::RoleList *>(ack->data());
      Zfb::Load::all(roleList->list(), [&out](unsigned, auto role) {
	out << *role << '\n';
      });
      executed(0, &ctx);
    });
  }
  void roleAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      ZtBitmap perms{ctx->args->get("2")};
      ZtBitmap apiperms{ctx->args->get("3")};
      uint8_t flags = 0;
      if (ctx->args->get("immutable")) flags |= RoleFlags::Immutable();
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::RoleAdd,
	    fbs::CreateRole(fbb,
	      str(fbb, ctx->args->get("1")),
	      bitmap(fbb, perms),
	      bitmap(fbb, apiperms),
	      flags).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleAdd), -1, "role add")) {
	executed(code, &ctx);
	return;
      }
      auto role = static_cast<const fbs::Role *>(ack->data());
      out << "added " << *role << '\n';
      executed(0, &ctx);
    });
  }
  void roleModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      Zfb::Offset<Zfb::String> name;
      Zfb::Offset<Zfb::Bitmap> perms, apiperms;
      name = str(fbb, ctx->args->get<true>("1"));
      if (ctx->args->exists("perms"))
	perms = bitmap(fbb, ZtBitmap{ctx->args->get("perms")});
      if (ctx->args->exists("apiperms"))
	apiperms = bitmap(fbb, ZtBitmap{ctx->args->get("apiperms")});
      uint8_t flags = 0;
      bool modFlags = ctx->args->exists("immutable");
      if (modFlags) {
	if (ctx->args->getBool("immutable", false))
	  flags |= RoleFlags::Immutable();
      }
      fbs::RoleBuilder fbb_{fbb};
      fbb_.add_name(name);
      if (!perms.IsNull()) fbb_.add_perms(perms);
      if (!apiperms.IsNull()) fbb_.add_apiperms(apiperms);
      if (modFlags) fbb_.add_flags(flags);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::RoleMod, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleMod), -1, "role modify")) {
	executed(code, &ctx);
	return;
      }
      auto role = static_cast<const fbs::Role *>(ack->data());
      out << "modified " << *role << '\n';
      executed(0, &ctx);
    });
  }
  void roleDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::RoleDel,
	    fbs::CreateRoleID(fbb, str(fbb, ctx->args->get("1"))).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleMod), -1, "role delete")) {
	executed(code, &ctx);
	return;
      }
      auto role = static_cast<const fbs::Role *>(ack->data());
      out << "deleted " << *role << '\n';
      executed(0, &ctx);
    });
  }

  void permsCmd(ZcmdContext *ctx) {
    ZuUnion<void, uint64_t, ZtString> key;
    bool exclusive;
    unsigned limit;
    try {
      ctx->args->getInt<true>("#", 1, 1);
      if (auto id = ctx->args->get("id"))
	*(key.new_<uint64_t>()) = ZuBox<uint64_t>{id};
      else if (auto name = ctx->args->get("name"))
	new (key.new_<ZtString>()) ZtString{name};
      exclusive = ctx->args->getBool("exclusive", false);
      limit = ctx->args->getInt("limit", 1, Zum::MaxQueryLimit, 1);
    } catch (...) { throw ZcmdUsage{}; }
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      Zfb::Offset<void> fbKey;
      if (key.is<uint64_t>())
	fbKey = fbs::CreatePermID(fbb, key.p<uint64_t>()).Union();
      else if (key.is<ZtString>())
	fbKey = fbs::CreatePermName(fbb, str(fbb, key.p<ZtString>())).Union();
      fbs::PermQueryBuilder fbb_(fbb);
      if (key.is<uint64_t>()) {
	fbb_.add_permKey_type(fbs::PermKey::ID);
	fbb_.add_permKey(fbKey);
      } else if (key.is<ZtString>()) {
	fbb_.add_permKey_type(fbs::PermKey::Name);
	fbb_.add_permKey(fbKey);
      }
      fbb_.add_inclusive(!exclusive);
      fbb_.add_limit(limit);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::PermGet, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, int(fbs::ReqAckData::PermGet), -1, "perm get")) {
	executed(code, &ctx);
	return;
      }
      auto permList = static_cast<const fbs::PermList *>(ack->data());
      Zfb::Load::all(permList->list(), [&out](unsigned, auto perm) {
	out << *perm << '\n';
      });
      executed(0, &ctx);
      return;
    });
  }
  void permAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      auto name = str(fbb, ctx->args->get("1"));
      fbs::PermBuilder fbb_{fbb};
      fbb_.add_name(name);
      fbb.Finish(fbs::CreateRequest(
	  fbb, ctx->seqNo, fbs::ReqData::PermAdd, fbb_.Finish().Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermAdd), -1, "permission add")) {
	executed(code, &ctx);
	return;
      }
      auto perm = static_cast<const fbs::Perm *>(ack->data());
      out << "added " << *perm << '\n';
      executed(0, &ctx);
    });
  }
  void permModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 3) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt<true>("1", 0, ~uint32_t(0));
      auto permName = ctx->args->get("2");
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::PermMod,
	    fbs::CreatePerm(fbb, permID, str(fbb, permName)).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermMod), -1, "permission modify")) {
	executed(code, &ctx);
	return;
      }
      auto perm = static_cast<const fbs::Perm *>(ack->data());
      out << "modified " << *perm << '\n';
      executed(0, &ctx);
    });
  }
  void permDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt<true>("1", 0, ~uint32_t(0));
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::PermDel,
	    fbs::CreatePermID(fbb, permID).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermDel), -1, "permission delete")) {
	executed(code, &ctx);
	return;
      }
      auto perm = static_cast<const fbs::Perm *>(ack->data());
      out << "deleted " << *perm << '\n';
      executed(0, &ctx);
    });
  }

  void keysCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      if (argc == 1)
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::OwnKeyGet,
	      fbs::CreateUserID(fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::KeyGet,
	      fbs::CreateUserID(fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyGet), int(fbs::ReqAckData::KeyGet),
	  "key get")) {
	executed(code, &ctx);
	return;
      }
      auto keyIDList = static_cast<const fbs::KeyIDList *>(ack->data());
      Zfb::Load::all(keyIDList->list(), [&out](unsigned, auto keyID) {
	out << ZtQuote::Base64{Zfb::Load::bytes(keyID->data())} << '\n';
      });
      executed(0, &ctx);
    });
  }

  void keyAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      if (argc == 1)
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::OwnKeyAdd,
	      fbs::CreateUserID(fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::KeyAdd,
	      fbs::CreateUserID(fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyAdd), int(fbs::ReqAckData::KeyAdd),
	  "key add")) {
	executed(code, &ctx);
	return;
      }
      auto key = static_cast<const fbs::Key *>(ack->data());
      out << "added " << *key << '\n';
      executed(0, &ctx);
    });
  }

  void keyClrCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      if (argc == 1)
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::OwnKeyClr,
	      fbs::CreateUserID(fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	      fbs::ReqData::KeyClr,
	      fbs::CreateUserID(fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyClr), int(fbs::ReqAckData::KeyClr),
	  "key clear")) {
	executed(code, &ctx);
	return;
      }
      out << "keys cleared\n";
      executed(0, &ctx);
    });
  }

  void keyDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    using namespace Zum;
    ZmRef<ZiIOBuf> buf;
    {
      using namespace Zfb::Save;
      auto keyID_ = ctx->args->get("1");
      ZtArray<uint8_t> keyID;
      keyID.length(ZuBase64::declen(keyID_.length()));
      ZuBase64::decode(keyID, keyID_);
      keyID.length(16);
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb, ctx->seqNo,
	    fbs::ReqData::KeyDel,
	    fbs::CreateKeyID(fbb, bytes(fbb, keyID)).Union()));
      buf = fbb.buf();
    }
    m_link->sendUserDB(ZuMv(buf), ctx->seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyDel), int(fbs::ReqAckData::KeyDel),
	  "key delete")) {
	executed(code, &ctx);
	return;
      }
      auto key = static_cast<const fbs::Key *>(ack->data());
      out << "deleted " << *key << '\n';
      executed(0, &ctx);
    });
  }

  void remoteCmd(ZcmdContext *) { } // unused

  void telcapCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    using namespace Ztel;
    unsigned interval = ctx->args->getInt("interval", 0, 1000000, 100);
    bool subscribe = !ctx->args->getBool("unsubscribe");
    if (!subscribe) {
      for (unsigned i = 0; i < TelDataN; i++) m_telcap[i] = TelCap{};
      if (argc > 1) throw ZcmdUsage{};
    } else {
      if (argc < 2) throw ZcmdUsage{};
    }
    ZtArray<ZmAtomic<unsigned>> ok;
    ZtArray<ZuString> filters;
    ZtArray<int> types;
    auto reqNames = fbs::EnumNamesReqType();
    if (argc <= 1 + subscribe) {
      ok.length(ReqTypeN);
      filters.length(ok.length());
      types.length(ok.length());
      for (unsigned i = 0; i < ReqTypeN; i++) {
	filters[i] = "*";
	types[i] = ReqType::MIN + i;
      }
    } else {
      ok.length(argc - (1 + subscribe));
      filters.length(ok.length());
      types.length(ok.length());
      for (unsigned i = 2; i < (unsigned)argc; i++) {
	auto j = i - 2;
	auto arg = ctx->args->get(ZuStringN<24>{} << i);
	ZuString type_;
	ZtRegex::Captures c;
	if (ZtREGEX(":").m(arg, c)) {
	  type_ = c[0];
	  filters[j] = c[2];
	} else {
	  type_ = arg;
	  filters[j] = "*";
	}
	types[j] = -1;
	for (unsigned k = ReqType::MIN; k <= ReqType::MAX; k++)
	  if (type_ == reqNames[k]) { types[j] = k; break; }
	if (types[j] < 0) throw ZcmdUsage{};
      }
    }
    auto &out = ctx->out;
    if (subscribe) {
      auto dir = ctx->args->get("1");
      ZiFile::age(dir, 10);
      {
	ZeError e;
	if (ZiFile::mkdir(dir, &e) != Zi::OK) {
	  out << dir << ": " << e << '\n';
	  executed(1, ctx);
	  return;
	}
      }
      for (unsigned i = 0, n = ok.length(); i < n; i++) {
	try {
	  switch (types[i]) {
	    case ReqType::Heap:
	      m_telcap[TelData::Heap - TelData::MIN] =
		TelCap::keyedFn<Heap>(
		    ZiFile::append(dir, "heap.csv"));
	      break;
	    case ReqType::HashTbl:
	      m_telcap[TelData::HashTbl - TelData::MIN] =
		TelCap::keyedFn<HashTbl>(
		    ZiFile::append(dir, "hash.csv"));
	      break;
	    case ReqType::Thread:
	      m_telcap[TelData::Thread - TelData::MIN] =
		TelCap::keyedFn<Thread>(
		    ZiFile::append(dir, "thread.csv"));
	      break;
	    case ReqType::Mx:
	      m_telcap[TelData::Mx - TelData::MIN] =
		TelCap::keyedFn<Mx>(
		    ZiFile::append(dir, "mx.csv"));
	      m_telcap[TelData::Socket - TelData::MIN] =
		TelCap::keyedFn<Socket>(
		    ZiFile::append(dir, "socket.csv"));
	      break;
	    case ReqType::Queue:
	      m_telcap[TelData::Queue - TelData::MIN] =
		TelCap::keyedFn<Queue>(
		    ZiFile::append(dir, "queue.csv"));
	      break;
	    case ReqType::Engine:
	      m_telcap[TelData::Engine - TelData::MIN] =
		TelCap::keyedFn<Ztel::Engine>(
		    ZiFile::append(dir, "engine.csv"));
	      m_telcap[TelData::Link - TelData::MIN] =
		TelCap::keyedFn<Ztel::Link>(
		    ZiFile::append(dir, "link.csv"));
	      break;
	    case ReqType::DB:
	      m_telcap[TelData::DB - TelData::MIN] =
		TelCap::singletonFn<DB>(
		    ZiFile::append(dir, "dbenv.csv"));
	      m_telcap[TelData::DBHost - TelData::MIN] =
		TelCap::keyedFn<DBHost>(
		    ZiFile::append(dir, "dbhost.csv"));
	      m_telcap[TelData::DBTable - TelData::MIN] =
		TelCap::keyedFn<DBTable>(
		    ZiFile::append(dir, "db.csv"));
	      break;
	    case ReqType::App:
	      m_telcap[TelData::App - TelData::MIN] =
		TelCap::singletonFn<Ztel::App>(
		    ZiFile::append(dir, "app.csv"));
	      break;
	    case ReqType::Alert:
	      m_telcap[TelData::Alert - TelData::MIN] =
		TelCap::alertFn<Alert>(
		    ZiFile::append(dir, "alert.csv"));
	      break;
	  }
	} catch (const ZvError &e) {
	  out << e << '\n';
	  executed(1, ctx);
	  return;
	}
      }
    }
    auto &sem = ZmTLS<ZmSemaphore, &ZCmd::telcapCmd>();
    for (unsigned i = 0, n = ok.length(); i < n; i++) {
      using namespace Zfb::Save;
      Zfb::IOBuilder fbb;
      fbb.Finish(fbs::CreateRequest(fbb,
	    ctx->seqNo, str(fbb, filters[i]), interval,
	    static_cast<fbs::ReqType>(types[i]), subscribe));
      m_link->sendTelReq(fbb.buf(), ctx->seqNo,
	  [ok = &ok[i], sem = &sem](const fbs::ReqAck *ack) {
	    ok->store_(ack->ok());
	    sem->post();
	  });
    }
    for (unsigned i = 0, n = ok.length(); i < n; i++) sem.wait();
    bool allOK = true;
    for (unsigned i = 0, n = ok.length(); i < n; i++)
      if (!ok[i].load_()) {
	out << "telemetry request "
	  << reqNames[types[i]] << ':' << filters[i] << " rejected\n";
	allOK = false;
      }
    if (!allOK) {
      executed(1, ctx);
      return;
    }
    if (subscribe) {
      if (!interval)
	out << "telemetry queried\n";
      else
	out << "telemetry subscribed\n";
    } else
      out << "telemetry unsubscribed\n";
    executed(0, ctx);
  }

private:
  bool			m_interactive = true;
  bool			m_solo = false;
  ZtString		m_soloMsg;

  ZmSemaphore		m_done;
  ZmSemaphore		m_executed;

  Zrl::Globber		m_globber;
  Zrl::History		m_history{100};
  Zrl::CLI		m_cli;

// FIXME
// - enable switching between multiple client connections
// - each client connection is composed of {link, ctx->seqNo, prompt, telcap}

// FIXME
// - telemetry should be distinguished by app ID + instanceID from
//   initial App telemetry msg

// FIXME
// reconcile tension between zdash telemetry aggregation and
// zcmd vs zdash fanout

  ZmRef<Link>		m_link;	
  ZvSeqNo		m_seqNo = 0;

  ZmPLock		m_promptLock;
    ZtArray<uint8_t>	  m_prompt;

  bool			m_exiting = false;

  TelCap		m_telcap[TelDataN];
};

template <typename Server>
inline Link::Link(ZCmd *app, Server &&server, uint16_t port) :
    Base{app, ZuFwd<Server>(server), port} { }

inline void Link::loggedIn()
{
  this->app()->loggedIn();
}
inline void Link::disconnected()
{
  this->app()->disconnected();
  Base::disconnected();
}
inline void Link::connectFailed(bool transient)
{
  this->app()->connectFailed();
}

inline int Link::processTelemetry(ZmRef<ZiIOBuf> buf)
{
  return this->app()->processTelemetry(ZuMv(buf));
}

ZmRef<ZCmd> client;

void sigint() { if (client) client->sigint(); }

int main(int argc, char **argv)
{
  if (argc < 2) usage();

  ZeLog::init("zcmd");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::lambdaSink([](ZeLogBuf &buf, const ZeEventInfo &) {
    buf << '\n';
    std::cerr << buf << std::flush;
  }));
  ZeLog::start();

  bool interactive = Zrl::interactive();
  ZuString keyID = ::getenv("ZCMD_KEY_ID");
  ZuString secret = ::getenv("ZCMD_KEY_SECRET");
  ZtString user, server;
  ZuBox<unsigned> port;

  try {
    {
      ZtRegex::Captures c;
      if (ZtREGEX("^([^@]+)@([^:]+):(\d+)$").m(argv[1], c) == 4) {
	user = c[2];
	server = c[3];
	port = c[4];
      }
    }
    if (!user) {
      ZtRegex::Captures c;
      if (ZtREGEX("^([^@]+)@(\d+)$").m(argv[1], c) == 3) {
	user = c[2];
	server = "localhost";
	port = c[3];
      }
    }
    if (!user) {
      ZtRegex::Captures c;
      if (ZtREGEX("^([^:]+):(\d+)$").m(argv[1], c) == 3) {
	server = c[2];
	port = c[3];
      }
    }
    if (!server) {
      ZtRegex::Captures c;
      if (ZtREGEX("^(\d+)$").m(argv[1], c) == 2) {
	server = "localhost";
	port = c[2];
      }
    }
  } catch (const ZtRegexError &) {
    usage();
  }
  if (!server || !*port || !port) usage();
  if (user)
    keyID = secret = {};
  else if (!keyID) {
    std::cerr << "set ZCMD_KEY_ID and ZCMD_KEY_SECRET "
      "to use without username\n" << std::flush;
    ::exit(1);
  }
  if (keyID) {
    if (!secret) {
      std::cerr << "set ZCMD_KEY_SECRET "
	"to use with ZCMD_KEY_ID\n" << std::flush;
      ::exit(1);
    }
  } else {
    if (!interactive || argc > 2) {
      std::cerr << "set ZCMD_KEY_ID and ZCMD_KEY_SECRET "
	"to use non-interactively\n" << std::flush;
      ::exit(1);
    }
  }

  ZiMultiplex *mx = new ZiMultiplex(
      ZiMxParams()
	.scheduler([](auto &s) {
	  s.nThreads(4)
	    .thread(1, [](auto &t) { t.isolated(1); })
	    .thread(2, [](auto &t) { t.isolated(1); })
	    .thread(3, [](auto &t) { t.isolated(1); }); })
	.rxThread(1).txThread(2));

  mx->start();

  client = new ZCmd();

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  {
    ZmRef<ZvCf> cf = new ZvCf();
    cf->set("timeout", "1");
    cf->set("thread", "3");
    if (auto caPath = ::getenv("ZCMD_CAPATH"))
      cf->set("caPath", caPath);
    else
      cf->set("caPath", "/etc/ssl/certs");
    try {
      client->init(mx, cf, interactive);
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

  if (argc > 2) {
    ZtString solo;
    for (int i = 2; i < argc; i++) {
      solo << argv[i];
      if (ZuLikely(i < argc - 1)) solo << ' ';
    }
    client->solo(ZuMv(solo));
  } else
    client->target(argv[1]);

  if (keyID)
    client->access(ZuMv(server), port, ZuMv(keyID), ZuMv(secret));
  else
    client->login(ZuMv(server), port, ZuMv(user));

  client->wait();

  if (client->interactive()) {
    std::cerr << std::flush;
    std::cout << std::flush;
  }

  client->exiting();
  client->disconnect();
  client->wait();

  mx->stop();

  ZeLog::stop();

  client->final();

  delete mx;

  ZmTrap::sigintFn(nullptr);

  return 0;
}
