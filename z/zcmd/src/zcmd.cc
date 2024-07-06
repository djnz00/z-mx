//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#include <zlib/ZuPolymorph.hh>
#include <zlib/ZuByteSwap.hh>

#include <zlib/ZmPlatform.hh>
#include <zlib/ZmTrap.hh>

#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiModule.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvCSV.hh>
#include <zlib/ZcmdClient.hh>
#include <zlib/ZcmdHost.hh>
#include <zlib/ZvUserDB.hh>

#include <zlib/ZuBase32.hh>
#include <zlib/ZuBase64.hh>
#include <zlib/ZtlsTOTP.hh>

#include <zlib/ZrlCLI.hh>
#include <zlib/ZrlGlobber.hh>
#include <zlib/ZrlHistory.hh>

#ifdef _MSC_VER
#pragma warning(disable:4800)
#endif

static void usage()
{
  static const char *usage =
    "usage: zcmd [USER@][HOST:]PORT [CMD [ARGS]]\n"
    "\tUSER\t- user (not needed if API key used)\n"
    "\tHOST\t- target host (default localhost)\n"
    "\tPORT\t- target port\n"
    "\tCMD\t- command to send to target\n"
    "\t\t  (reads commands from standard input if none specified)\n"
    "\tARGS\t- command arguments\n\n"
    "Environment Variables:\n"
    "\tZCMD_KEY_ID\tAPI key ID\n"
    "\tZCMD_KEY_SECRET\tAPI key secret\n"
    "\tZCMD_PLUGIN\tzcmd plugin module\n";
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

  int processTelemetry(const uint8_t *data, unsigned len);
};

class ZCmd :
    public ZmPolymorph,
    public ZcmdClient<ZCmd, Link>,
    public ZcmdHost {
public:
  using Base = ZcmdClient<ZCmd, Link>;
  using FBB = typename Base::FBB;

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
  void login(Server &&server, uint16_t port, Args &&... args) {
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
  void access(Server &&server, uint16_t port, Args &&... args) {
    m_link = new Link(this, ZuFwd<Server>(server), port);
    m_link->access(ZuFwd<Args>(args)...);
  }

  void disconnect() { if (m_link) m_link->disconnect(); }

  void wait() { m_done.wait(); }
  void done() { m_done.post(); }

  void exiting() { m_exiting = true; }

  // ZcmdHost virtual functions
  ZcmdDispatcher *dispatcher() { return this; }
  void send(void *link, ZmRef<ZiAnyIOBuf> buf) {
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
    ReqTypeN = ZvTelemetry::ReqType::N,
    TelDataN = ZvTelemetry::TelData::N
  };

  int processTelemetry(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace ZvTelemetry;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyTelemetryBuffer(verifier)) return -1;
    }
    auto msg = fbs::GetTelemetry(data);
    int i = int(msg->data_type());
    if (ZuUnlikely(i < TelData::MIN)) return 0;
    i -= TelData::MIN;
    if (ZuUnlikely(i >= TelDataN)) return 0;
    m_telcap[i](msg->data());
    return len;
  }

  void disconnected() {
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
      .app_ = this,
      .link_ = m_link,
      .interactive = m_interactive
    };
    {
      ZtRegex::Captures c;
      unsigned pos = 0, n = 0;
      if (n = ZtREGEX("\s*>>\s*").m(cmd, c, pos)) {
	ZtString path{c[2]};
	if (!(ctx.file = fopen(path, "a"))) {
	  ZeLOG(Error, ([path, e = ZeLastError](auto &s) {
	    s << path << ": " << e;
	  }));
	  return -1;
	}
	cmd = c[0];
      } else if (n = ZtREGEX("\s*>\s*").m(cmd, c, pos)) {
	ZtString path{c[2]};
	if (!(ctx.file = fopen(path, "w"))) {
	  ZeLOG(Error, ([path, e = ZeLastError](auto &s) {
	    s << path << ": " << e;
	  }));
	  return -1;
	}
	cmd = c[0];
      } else {
	ctx.file = stdout;
	cmd = ZuMv(cmd_);
      }
    }
    ZtArray<ZtString> args;
    ZvCf::parseCLI(cmd, args);
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
    auto seqNo = m_seqNo++;
    m_fbb.Finish(Zcmd::fbs::CreateRequest(m_fbb, seqNo,
	  Zfb::Save::strVecIter(m_fbb, args.length(),
	    [&args](unsigned i) { return args[i]; })));
    m_link->sendCmd(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	  const Zcmd::fbs::ReqAck *ack) mutable {
      using namespace Zfb::Load;
      ctx.out = str(ack->out());
      executed(ack->code(), &ctx);
    });
  }

  using ZcmdHost::executed;
  void executed(ZcmdContext *ctx) {
    if (const auto &out = ctx->out)
      fwrite(out.data(), 1, out.length(), ctx->file);
    fflush(stdout);
    if (ctx->file != stdout) { fclose(ctx->file); ctx->file = stdout; }
    m_executed.post();
  }

private:
  // built-in commands

  int filterAck(
      ZtString &out,
      const ZvUserDB::fbs::ReqAck *ack,
      int ackType1, int ackType2,
      const char *op) {
    using namespace ZvUserDB;
    if (ack->rejCode()) {
      out << '[' << ZuBox<unsigned>(ack->rejCode()) << "] "
	<< Zfb::Load::str(ack->rejText());
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
	"change passwd", "usage: passwd");

    addCmd("users", "",
	ZcmdFn::Member<&ZCmd::usersCmd>::fn(this),
	"list users", "usage: users");
    addCmd("useradd",
	"e enabled enabled { type flag } "
	"i immutable immutable { type flag }",
	ZcmdFn::Member<&ZCmd::userAddCmd>::fn(this),
	"add user",
	"usage: useradd ID NAME ROLE[,ROLE,...] [OPTIONS...]\n\n"
	"Options:\n"
	"  -e, --enabled\t\tset Enabled flag\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("resetpass", "",
	ZcmdFn::Member<&ZCmd::resetPassCmd>::fn(this),
	"reset password", "usage: resetpass USERID");
    addCmd("usermod",
	"e enabled enabled { type flag } "
	"i immutable immutable { type flag }",
	ZcmdFn::Member<&ZCmd::userModCmd>::fn(this),
	"modify user",
	"usage: usermod ID NAME ROLE[,ROLE,...] [OPTIONS...]\n\n"
	"Options:\n"
	"  -e, --enabled\t\tset Enabled flag\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("userdel", "",
	ZcmdFn::Member<&ZCmd::userDelCmd>::fn(this),
	"delete user", "usage: userdel ID");

    addCmd("roles", "",
	ZcmdFn::Member<&ZCmd::rolesCmd>::fn(this),
	"list roles", "usage: roles");
    addCmd("roleadd", "i immutable immutable { type flag }",
	ZcmdFn::Member<&ZCmd::roleAddCmd>::fn(this),
	"add role",
	"usage: roleadd NAME PERMS APIPERMS [OPTIONS...]\n\n"
	"Options:\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("rolemod", "i immutable immutable { type scalar }",
	ZcmdFn::Member<&ZCmd::roleModCmd>::fn(this),
	"modify role",
	"usage: rolemod NAME PERMS APIPERMS [OPTIONS...]\n\n"
	"Options:\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("roledel", "",
	ZcmdFn::Member<&ZCmd::roleDelCmd>::fn(this),
	"delete role",
	"usage: roledel NAME");

    addCmd("perms", "",
	ZcmdFn::Member<&ZCmd::permsCmd>::fn(this),
	"list permissions", "usage: perms");
    addCmd("permadd", "",
	ZcmdFn::Member<&ZCmd::permAddCmd>::fn(this),
	"add permission", "usage: permadd NAME");
    addCmd("permmod", "",
	ZcmdFn::Member<&ZCmd::permModCmd>::fn(this),
	"modify permission", "usage: permmod ID NAME");
    addCmd("permdel", "",
	ZcmdFn::Member<&ZCmd::permDelCmd>::fn(this),
	"delete permission", "usage: permdel ID");

    addCmd("keys", "",
	ZcmdFn::Member<&ZCmd::keysCmd>::fn(this),
	"list keys", "usage: keys [USERID]");
    addCmd("keyadd", "",
	ZcmdFn::Member<&ZCmd::keyAddCmd>::fn(this),
	"add key", "usage: keyadd [USERID]");
    addCmd("keydel", "",
	ZcmdFn::Member<&ZCmd::keyDelCmd>::fn(this),
	"delete key", "usage: keydel ID");
    addCmd("keyclr", "",
	ZcmdFn::Member<&ZCmd::keyClrCmd>::fn(this),
	"clear all keys", "usage: keyclr [USERID]");

    addCmd("remote", "",
	ZcmdFn::Member<&ZCmd::remoteCmd>::fn(this),
	"run command remotely", "usage: remote COMMAND...");

    addCmd("telcap",
	"i interval interval { type scalar } "
	"u unsubscribe unsubscribe { type flag }",
	ZcmdFn::Member<&ZCmd::telcapCmd>::fn(this),
	"telemetry capture",
	"usage: telcap [OPTIONS...] PATH [TYPE[:FILTER]]...\n\n"
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
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::ChPass,
	    fbs::CreateUserChPass(m_fbb,
	      str(m_fbb, oldpw),
	      str(m_fbb, newpw)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      if (int code = filterAck(
	  ctx.out, ack, int(fbs::ReqAckData::ChPass), -1, "password change")) {
	executed(code, &ctx);
	return;
      }
      auto &out = ctx.out;
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "password change rejected\n";
	executed(1, &ctx);
	return;
      }
      out << "password changed\n";
      executed(0, &ctx);
    });
  }

  static void printUser(ZtString &out, const ZvUserDB::fbs::User *user_) {
    using namespace ZvUserDB;
    using namespace Zfb::Load;
    auto hmac_ = bytes(user_->hmac());
    ZtString hmac;
    hmac.length(ZuBase64::enclen(hmac_.length()));
    ZuBase64::encode(hmac, hmac_);
    auto secret_ = bytes(user_->secret());
    ZtString secret;
    secret.length(ZuBase32::enclen(secret_.length()));
    ZuBase32::encode(secret, secret_);
    out << user_->id() << ' ' << str(user_->name()) << " roles=[";
    all(user_->roles(), [&out](unsigned i, auto role_) {
      if (i) out << ',';
      out << str(role_);
    });
    out << "] hmac=" << hmac << " secret=" << secret << " flags=";
    bool pipe = false;
    if (user_->flags() & User::Enabled) {
      out << "Enabled";
      pipe = true;
    }
    if (user_->flags() & User::Immutable) {
      if (pipe) out << '|';
      out << "Immutable";
      pipe = true;
    }
    if (user_->flags() & User::ChPass) {
      if (pipe) out << '|';
      out << "ChPass";
      // pipe = true;
    }
  }

  void usersCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      fbs::UserIDBuilder fbb_(m_fbb);
      if (argc == 2) fbb_.add_id(ctx->args->getInt64<true>("1", 0, LLONG_MAX));
      auto userID = fbb_.Finish();
      m_fbb.Finish(fbs::CreateRequest(
	  m_fbb, seqNo, fbs::ReqData::UserGet, userID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, int(fbs::ReqAckData::UserGet), -1, "user get")) {
	executed(code, &ctx);
	return;
      }
      auto userList = static_cast<const fbs::UserList *>(ack->data());
      using namespace Zfb::Load;
      all(userList->list(), [&out](unsigned, auto user_) {
	printUser(out, user_); out << '\n';
      });
      executed(0, &ctx);
      return;
    });
  }
  void userAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      uint8_t flags = 0;
      if (ctx->args->get("enabled")) flags |= User::Enabled;
      if (ctx->args->get("immutable")) flags |= User::Immutable;
      ZtRegex::Captures roles;
      ZtREGEX(",").split(ctx->args->get("3"), roles);
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::UserAdd,
	    fbs::CreateUser(m_fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX),
	      str(m_fbb, ctx->args->get("2")), 0, 0,
	      strVecIter(m_fbb, roles.length(),
		[&roles](unsigned i) { return roles[i]; }),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserAdd), -1, "user add")) {
	executed(code, &ctx);
	return;
      }
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      if (!userPass->ok()) {
	out << "user add rejected\n";
	executed(1, &ctx);
	return;
      }
      printUser(out, userPass->user()); out << '\n';
      using namespace Zfb::Load;
      out << "passwd=" << str(userPass->passwd()) << '\n';
      executed(0, &ctx);
    });
  }
  void resetPassCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::ResetPass,
	    fbs::CreateUserID(m_fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::ResetPass), -1, "reset password")) {
	executed(code, &ctx);
	return;
      }
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      if (!userPass->ok()) {
	out << "reset password rejected\n";
	executed(1, &ctx);
	return;
      }
      printUser(out, userPass->user()); out << '\n';
      using namespace Zfb::Load;
      out << "passwd=" << str(userPass->passwd()) << '\n';
      executed(0, &ctx);
    });
  }
  void userModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      uint8_t flags = 0;
      if (ctx->args->get("enabled")) flags |= User::Enabled;
      if (ctx->args->get("immutable")) flags |= User::Immutable;
      ZtRegex::Captures roles;
      ZtREGEX(",").split(ctx->args->get("3"), roles);
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::UserMod,
	    fbs::CreateUser(m_fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX),
	      str(m_fbb, ctx->args->get("2")), 0, 0,
	      strVecIter(m_fbb, roles.length(),
		[&roles](unsigned i) { return roles[i]; }),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserMod), -1, "user modify")) {
	executed(code, &ctx);
	return;
      }
      auto userUpdAck = static_cast<const fbs::UserUpdAck *>(ack->data());
      if (!userUpdAck->ok()) {
	out << "user modify rejected\n";
	executed(1, &ctx);
	return;
      }
      printUser(out, userUpdAck->user()); out << '\n';
      executed(0, &ctx);
    });
  }
  void userDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::UserDel,
	    fbs::CreateUserID(m_fbb,
	      ctx->args->getInt64<true>("1", 0, LLONG_MAX)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::UserDel), -1, "user delete")) {
	executed(code, &ctx);
	return;
      }
      auto userUpdAck = static_cast<const fbs::UserUpdAck *>(ack->data());
      if (!userUpdAck->ok()) {
	out << "user delete rejected\n";
	executed(1, &ctx);
	return;
      }
      printUser(out, userUpdAck->user()); out << '\n';
      out << "user deleted\n";
      executed(0, &ctx);
    });
  }

  static void printRole(ZtString &out, const ZvUserDB::fbs::Role *role_) {
    using namespace ZvUserDB;
    using namespace Zfb::Load;
    Bitmap perms, apiperms;
    all(role_->perms(), [&perms](unsigned i, uint64_t w) {
      if (ZuLikely(i < Bitmap::Words)) perms.data[i] = w;
    });
    all(role_->apiperms(), [&apiperms](unsigned i, uint64_t w) {
      if (ZuLikely(i < Bitmap::Words)) apiperms.data[i] = w;
    });
    out << str(role_->name())
      << " perms=[" << perms
      << "] apiperms=[" << apiperms
      << "] flags=";
    if (role_->flags() & Role::Immutable) out << "Immutable";
  }

  void rolesCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      Zfb::Offset<Zfb::String> name_;
      if (argc == 2) name_ = str(m_fbb, ctx->args->get("1"));
      fbs::RoleIDBuilder fbb_(m_fbb);
      if (argc == 2) fbb_.add_name(name_);
      auto roleID = fbb_.Finish();
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::RoleGet, roleID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleGet), -1, "role get")) {
	executed(code, &ctx);
	return;
      }
      auto roleList = static_cast<const fbs::RoleList *>(ack->data());
      using namespace Zfb::Load;
      all(roleList->list(), [&out](unsigned, auto role_) {
	printRole(out, role_); out << '\n';
      });
      executed(0, &ctx);
    });
  }
  void roleAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      Bitmap perms{ctx->args->get("2")};
      Bitmap apiperms{ctx->args->get("3")};
      uint8_t flags = 0;
      if (ctx->args->get("immutable")) flags |= Role::Immutable;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::RoleAdd,
	    fbs::CreateRole(m_fbb,
	      str(m_fbb, ctx->args->get("1")),
	      m_fbb.CreateVector(perms.data, Bitmap::Words),
	      m_fbb.CreateVector(apiperms.data, Bitmap::Words),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleAdd), -1, "role add")) {
	executed(code, &ctx);
	return;
      }
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role add rejected\n";
	executed(1, &ctx);
	return;
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      executed(0, &ctx);
    });
  }
  void roleModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      Bitmap perms{ctx->args->get("2")};
      Bitmap apiperms{ctx->args->get("3")};
      uint8_t flags = 0;
      if (ctx->args->get("immutable")) flags |= Role::Immutable;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::RoleMod,
	    fbs::CreateRole(m_fbb,
	      str(m_fbb, ctx->args->get("1")),
	      m_fbb.CreateVector(perms.data, Bitmap::Words),
	      m_fbb.CreateVector(apiperms.data, Bitmap::Words),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleMod), -1, "role modify")) {
	executed(code, &ctx);
	return;
      }
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role modify rejected\n";
	executed(1, &ctx);
	return;
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      executed(0, &ctx);
    });
  }
  void roleDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::RoleDel,
	    fbs::CreateRoleID(m_fbb, str(m_fbb, ctx->args->get("1"))).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::RoleMod), -1, "role delete")) {
	executed(code, &ctx);
	return;
      }
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role delete rejected\n";
	executed(1, &ctx);
	return;
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      out << "role deleted\n";
      executed(0, &ctx);
    });
  }

  void permsCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      fbs::PermIDBuilder fbb_(m_fbb);
      if (argc == 2) fbb_.add_id(ctx->args->getInt<true>("1", 0, Bitmap::Bits));
      auto permID = fbb_.Finish();
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::PermGet, permID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermGet), -1, "perm get")) {
	executed(code, &ctx);
	return;
      }
      auto permList = static_cast<const fbs::PermList *>(ack->data());
      using namespace Zfb::Load;
      all(permList->list(), [&out](unsigned, auto perm_) {
	out << ZuBoxed(perm_->id()).template fmt<ZuFmt::Right<3, ' '>>() <<
	  ' ' << str(perm_->name()) << '\n';
      });
      executed(0, &ctx);
    });
  }
  void permAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto name = ctx->args->get("1");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::PermAdd,
	    fbs::CreatePermAdd(m_fbb, str(m_fbb, name)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermAdd), -1, "permission add")) {
	executed(code, &ctx);
	return;
      }
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission add rejected\n";
	executed(1, &ctx);
	return;
      }
      auto perm = permUpdAck->perm();
      out << "added " << perm->id() << ' ' << str(perm->name()) << '\n';
      executed(0, &ctx);
    });
  }
  void permModCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 3) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt<true>("1", 0, Bitmap::Bits);
      auto permName = ctx->args->get("2");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::PermMod,
	    fbs::CreatePerm(m_fbb, permID, str(m_fbb, permName)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermMod), -1, "permission modify")) {
	executed(code, &ctx);
	return;
      }
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission modify rejected\n";
	executed(1, &ctx);
	return;
      }
      auto perm = permUpdAck->perm();
      out << "modified " << perm->id() << ' ' << str(perm->name()) << '\n';
      executed(0, &ctx);
    });
  }
  void permDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt<true>("1", 0, Bitmap::Bits);
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::PermDel,
	    fbs::CreatePermID(m_fbb, permID).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack, int(fbs::ReqAckData::PermDel), -1, "permission delete")) {
	executed(code, &ctx);
	return;
      }
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission delete rejected\n";
	executed(1, &ctx);
	return;
      }
      auto perm = permUpdAck->perm();
      out << "deleted " << perm->id() << ' ' << str(perm->name()) << '\n';
      executed(0, &ctx);
    });
  }

  void keysCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::OwnKeyGet,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::KeyGet,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
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
      using namespace Zfb::Load;
      all(keyIDList->list(), [&out](unsigned, auto key_) {
	out << str(key_) << '\n';
      });
      executed(0, &ctx);
    });
  }

  void keyAddCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::OwnKeyAdd,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::KeyAdd,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyAdd), int(fbs::ReqAckData::KeyAdd),
	  "key add")) {
	executed(code, &ctx);
	return;
      }
      using namespace Zfb::Load;
      auto keyUpdAck = static_cast<const fbs::KeyUpdAck *>(ack->data());
      if (!keyUpdAck->ok()) {
	out << "key add rejected\n";
	executed(1, &ctx);
	return;
      }
      auto secret_ = bytes(keyUpdAck->key()->secret());
      ZtString secret;
      secret.length(ZuBase64::enclen(secret_.length()));
      ZuBase64::encode(secret, secret_);
      out << "id: " << str(keyUpdAck->key()->id())
	<< "\nsecret: " << secret << '\n';
      executed(0, &ctx);
    });
  }

  void keyDelCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto keyID = ctx->args->get("1");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData::KeyDel,
	    fbs::CreateKeyID(m_fbb, str(m_fbb, keyID)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyDel), int(fbs::ReqAckData::KeyDel),
	  "key delete")) {
	executed(code, &ctx);
	return;
      }
      using namespace Zfb::Load;
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "key delete rejected\n";
	executed(1, &ctx);
	return;
      }
      out << "key deleted\n";
      executed(0, &ctx);
    });
  }

  void keyClrCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZcmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::OwnKeyClr,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData::KeyClr,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	  out, ack,
	  int(fbs::ReqAckData::OwnKeyClr), int(fbs::ReqAckData::KeyClr),
	  "key clear")) {
	executed(code, &ctx);
	return;
      }
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "key clear rejected\n";
	executed(1, &ctx);
	return;
      }
      out << "keys cleared\n";
      executed(0, &ctx);
    });
  }

  void remoteCmd(ZcmdContext *) { } // unused

  void telcapCmd(ZcmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    using namespace ZvTelemetry;
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
		TelCap::keyedFn<ZvTelemetry::Engine>(
		    ZiFile::append(dir, "engine.csv"));
	      m_telcap[TelData::Link - TelData::MIN] =
		TelCap::keyedFn<ZvTelemetry::Link>(
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
		TelCap::singletonFn<ZvTelemetry::App>(
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
      auto seqNo = m_seqNo++;
      m_fbb.Finish(fbs::CreateRequest(m_fbb,
	    seqNo, str(m_fbb, filters[i]), interval,
	    static_cast<fbs::ReqType>(types[i]), subscribe));
      m_link->sendTelReq(m_fbb, seqNo,
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
// - each client connection is composed of {link, seqNo, prompt, telcap}
//
// - m_fbb can be shared

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

  FBB			m_fbb;

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

inline int Link::processTelemetry(const uint8_t *data, unsigned len)
{
  return this->app()->processTelemetry(data, len);
}

ZmRef<ZCmd> client;

void sigint() { if (client) client->done(); }

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
