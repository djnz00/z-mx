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
#include <signal.h>

#include <zlib/ZuPolymorph.hpp>
#include <zlib/ZuByteSwap.hpp>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTrap.hpp>

#include <zlib/ZiMultiplex.hpp>
#include <zlib/ZiModule.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvCSV.hpp>
#include <zlib/ZvMultiplex.hpp>
#include <zlib/ZvCmdClient.hpp>
#include <zlib/ZvCmdHost.hpp>
#include <zlib/ZvUserDB.hpp>

#include <zlib/ZtlsBase32.hpp>
#include <zlib/ZtlsBase64.hpp>

#include <zlib/ZrlCLI.hpp>
#include <zlib/ZrlGlobber.hpp>
#include <zlib/ZrlHistory.hpp>

#ifdef _WIN32
#include <io.h>		// for _isatty
#ifndef isatty
#define isatty _isatty
#endif
#ifndef fileno
#define fileno _fileno
#endif
#else
#include <termios.h>
#include <unistd.h>	// for isatty
#include <fcntl.h>
#endif

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
  ZmPlatform::exit(1);
}

class TelCap {
public:
  using Fn = ZmFn<const void *>;

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
    using Data = ZvFB::Load<Data_>;
    using FBS = ZvFBS<Data>;
    using Key = decltype(ZvFieldKey(ZuDeclVal<const Data &>()));
    struct Accessor : public ZuAccessor<Data, ZuDecay<Key>> {
      static Key value(const Data &data) { return ZvFieldKey(data); }
    };
    using Tree_ =
      ZmRBTree<Data,
	ZmRBTreeIndex<Accessor,
	  ZmRBTreeUnique<true,
	    ZmRBTreeLock<ZmNoLock> > > >;
    struct Tree : public ZuObject, public Tree_ { };
    ZmRef<Tree> tree = new Tree{};
    return TelCap{[
	tree = ZuMv(tree),
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbs_) mutable {
      if (!fbs_) {
	l(nullptr);
	tree->clean();
	return;
      }
      auto fbs = static_cast<const FBS *>(fbs_);
      auto node = tree->find(ZvFB::key<Data>(fbs));
      if (!node)
	tree->add(node = new typename Tree::Node{fbs});
      else
	ZvFB::loadUpdate(node->key(), fbs);
      l(&(node->key()));
    }};
  }

  template <typename Data_>
  static TelCap singletonFn(ZtString path) {
    using Data = ZvFB::Load<Data_>;
    using FBS = ZvFBS<Data>;
    return TelCap{[
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbs_) mutable {
      if (!fbs_) {
	l(nullptr);
	return;
      }
      auto fbs = static_cast<const FBS *>(fbs_);
      static Data *data = nullptr;
      if (!data)
	data = new Data{fbs};
      else
	ZvFB::loadUpdate(*data, fbs);
      l(data);
    }};
  }

  template <typename Data_>
  static TelCap alertFn(ZtString path) {
    using Data = ZvFB::Load<Data_>;
    using FBS = ZvFBS<Data>;
    return TelCap{[
	l = ZvCSV<Data>{}.writeFile(path)](const void *fbs_) mutable {
      if (!fbs_) {
	l(nullptr);
	return;
      }
      auto fbs = static_cast<const FBS *>(fbs_);
      Data data{fbs};
      l(&data);
    }};
  }

  void operator ()(const void *p) { m_fn(p); }

private:
  Fn		m_fn;
};

class ZCmd;

class Link : public ZvCmdCliLink<ZCmd, Link> {
public:
  using Base = ZvCmdCliLink<ZCmd, Link>;
  template <typename Server>
  Link(ZCmd *app, Server &&server, uint16_t port);

  void loggedIn();
  void disconnected();
  void connectFailed(bool transient);

  int processTelemetry(const uint8_t *data, unsigned len);
};

class ZCmd :
    public ZmPolymorph,
    public ZvCmdClient<ZCmd, Link>,
    public ZvCmdHost {
public:
  using Base = ZvCmdClient<ZCmd, Link>;
  using FBB = typename Base::FBB;

friend Link;

  void init(ZiMultiplex *mx, const ZvCf *cf, bool interactive) {
    Base::init(mx, cf);
    m_interactive = interactive;
    ZvCmdHost::init();
    initCmds();
    if (m_interactive)
      m_cli.init(Zrl::App{
	.error = [this](ZuString s) { std::cerr << s << '\n'; post(); },
	.prompt = [this](ZtArray<uint8_t> &s) {
	  ZmGuard guard(m_promptLock);
	  if (m_prompt.owned()) s = ZuMv(m_prompt);
	},
	.enter = [this](ZuString s) -> bool {
	  exec(ZtString{s});
	  m_executed.wait();
	  return code();
	},
	.end = [this]() { post(); },
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
	.compNext = m_globber.nextFn(),
	.histSave = m_history.saveFn(),
	.histLoad = m_history.loadFn()
      });
  }
  void final() {
    m_cli.final();
    for (unsigned i = 0; i < TelDataN; i++) m_telcap[i] = TelCap{};
    m_link = nullptr;
    ZvCmdHost::final();
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
    ZtString passwd = m_cli.getpass("password: ", 100);
    if (!passwd) return;
    ZuBox<unsigned> totp = m_cli.getpass("totp: ", 6);
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
  void post() { m_done.post(); }

  void exiting() { m_exiting = true; }

  int code() const { return m_code; }

  // ZvCmdHost virtual functions
  ZvCmdDispatcher *dispatcher() { return this; }
  void send(void *link, ZmRef<ZiIOBuf<>> buf) {
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
      ZtString out;
      loadMod(plugin, out);
      if (out) std::cerr << out;
    }
    start();
  }

  void start() {
    if (m_solo) {
      exec(ZuMv(m_soloMsg));
      m_executed.wait();
      post();
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
	  exec(ZuMv(cmd));
	  m_executed.wait();
	  if (code()) break;
	  cmd.size(4096);
	}
	post();
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
    int i = msg->data_type();
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
    if (m_exiting) return;
    if (m_interactive) {
      m_cli.final();
      std::cerr << "server disconnected\n" << std::flush;
    }
    ZmPlatform::exit(1);
  }
  void connectFailed() {
    if (m_interactive) {
      m_cli.stop();
      m_cli.close();
      m_cli.final();
      std::cerr << "connect failed\n" << std::flush;
    }
    ZmPlatform::exit(1);
  }

  void exec(ZtString cmd) {
    if (!cmd) return;
    ZtString cmd_ = ZuMv(cmd);
    ZvCmdContext ctx{.app_ = this, .link_ = m_link};
    {
      ZtRegex::Captures c;
      unsigned pos = 0, n = 0;
      if (n = ZtREGEX("\s*>>\s*").m(cmd, c, pos)) {
	if (!(ctx.file = fopen(c[2], "a"))) {
	  logError(ZtString{c[2]}, ": ", ZeLastError);
	  return;
	}
	cmd = c[0];
      } else if (n = ZtREGEX("\s*>\s*").m(cmd, c, pos)) {
	if (!(ctx.file = fopen(c[2], "w"))) {
	  logError(ZtString{c[2]}, ": ", ZeLastError);
	  return;
	}
	cmd = c[0];
      } else {
	ctx.file = stdout;
	cmd = ZuMv(cmd_);
      }
    }
    ZtArray<ZtString> args;
    ZvCf::parseCLI(cmd, args);
    if (!args) return;
    auto &out = ctx.out;
    if (args[0] == "help") {
      if (args.length() == 1) {
	out << "Local ";
	processCmd(&ctx, args);
	out << "\nRemote ";
	fwrite(out.data(), 1, out.length(), ctx.file);
      } else if (args.length() == 2 && hasCmd(args[1])) {
	int code = processCmd(&ctx, args);
	if (code || ctx.out) executed(code, &ctx);
	return;
      }
    } else if (args[0] == "remote") {
      args.shift();
    } else if (hasCmd(args[0])) {
      int code = processCmd(&ctx, args);
      if (code || ctx.out) executed(code, &ctx);
      return;
    }
    send(&ctx, args);
  }

  // async invoke
  void send(ZvCmdContext *ctx, ZuArray<const ZtString> args) {
    auto seqNo = m_seqNo++;
    m_fbb.Finish(ZvCmd::fbs::CreateRequest(m_fbb, seqNo,
	  Zfb::Save::strVecIter(m_fbb, args.length(),
	    [&args](unsigned i) { return args[i]; })));
    m_link->sendCmd(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	  const ZvCmd::fbs::ReqAck *ack) mutable {
      using namespace Zfb::Load;
      ctx.out = str(ack->out());
      executed(ack->code(), &ctx);
    });
  }

  int executed(int code, ZvCmdContext *ctx) {
    ctx->code = code;
    return executed(ctx);
  }

  int executed(ZvCmdContext *ctx) {
    if (const auto &out = ctx->out)
      fwrite(out.data(), 1, out.length(), ctx->file);
    if (ctx->file != stdout) { fclose(ctx->file); ctx->file = stdout; }
    m_code = ctx->code;
    m_executed.post();
    return ctx->code;
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
	<< Zfb::Load::str(ack->rejText()) << '\n';
      return 1;
    }
    auto ackType = ack->data_type();
    if ((int)ackType != ackType1 &&
	ackType2 >= fbs::ReqAckData_MIN && (int)ackType != ackType2) {
      logError("mismatched ack from server: ",
	  fbs::EnumNameReqAckData(ackType));
      out << op << " failed\n";
      return 1;
    }
    return 0;
  }

  void initCmds() {
    addCmd("passwd", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->passwdCmd(ctx);
	  }},  "change passwd", "usage: passwd");

    addCmd("users", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->usersCmd(ctx);
	  }},  "list users", "usage: users");
    addCmd("useradd",
	"e enabled enabled { type flag } "
	"i immutable immutable { type flag }",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->userAddCmd(ctx);
	  }},  "add user",
	"usage: useradd ID NAME ROLE[,ROLE,...] [OPTIONS...]\n\n"
	"Options:\n"
	"  -e, --enabled\t\tset Enabled flag\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("resetpass", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->resetPassCmd(ctx);
	  }},  "reset password", "usage: resetpass USERID");
    addCmd("usermod",
	"e enabled enabled { type flag } "
	"i immutable immutable { type flag }",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->userModCmd(ctx);
	  }},  "modify user",
	"usage: usermod ID NAME ROLE[,ROLE,...] [OPTIONS...]\n\n"
	"Options:\n"
	"  -e, --enabled\t\tset Enabled flag\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("userdel", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->userDelCmd(ctx);
	  }},  "delete user", "usage: userdel ID");

    addCmd("roles", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->rolesCmd(ctx);
	  }},  "list roles", "usage: roles");
    addCmd("roleadd", "i immutable immutable { type flag }",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->roleAddCmd(ctx);
	  }},  "add role",
	"usage: roleadd NAME PERMS APIPERMS [OPTIONS...]\n\n"
	"Options:\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("rolemod", "i immutable immutable { type scalar }",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->roleModCmd(ctx);
	  }},  "modify role",
	"usage: rolemod NAME PERMS APIPERMS [OPTIONS...]\n\n"
	"Options:\n"
	"  -i, --immutable\tset Immutable flag\n");
    addCmd("roledel", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->roleDelCmd(ctx);
	  }},  "delete role",
	"usage: roledel NAME");

    addCmd("perms", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->permsCmd(ctx);
	  }},  "list permissions", "usage: perms");
    addCmd("permadd", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->permAddCmd(ctx);
	  }},  "add permission", "usage: permadd NAME");
    addCmd("permmod", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->permModCmd(ctx);
	  }},  "modify permission", "usage: permmod ID NAME");
    addCmd("permdel", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->permDelCmd(ctx);
	  }},  "delete permission", "usage: permdel ID");

    addCmd("keys", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->keysCmd(ctx);
	  }},  "list keys", "usage: keys [USERID]");
    addCmd("keyadd", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->keyAddCmd(ctx);
	  }},  "add key", "usage: keyadd [USERID]");
    addCmd("keydel", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->keyDelCmd(ctx);
	  }},  "delete key", "usage: keydel ID");
    addCmd("keyclr", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->keyClrCmd(ctx);
	  }},  "clear all keys", "usage: keyclr [USERID]");

    addCmd("remote", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->remoteCmd(ctx);
	  }},  "run command remotely", "usage: remote COMMAND...");

    addCmd("loadmod", "",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->loadModCmd(ctx);
	  }},  "load application-specific module", "usage: loadmod MODULE");

    addCmd("telcap",
	"i interval interval { type scalar } "
	"u unsubscribe unsubscribe { type flag }",
	ZvCmdFn{this,
	  [](ZCmd *app, ZvCmdContext *ctx) {
	    return app->telcapCmd(ctx);
	  }},  "telemetry capture",
	"usage: telcap [OPTIONS...] PATH [TYPE[:FILTER]]...\n\n"
	"  PATH\tdirectory for capture CSV files\n"
	"  TYPE\t[Heap|HashTbl|Thread|Mx|Queue|Engine|DbEnv|App|Alert]\n"
	"  FILTER\tfilter specification in type-specific format\n\n"
	"Options:\n"
	"  -i, --interval=N\tset scan interval in milliseconds "
	  "(100 <= N <= 1M)\n"
	"  -u, --unsubscribe\tunsubscribe (i.e. end capture)\n");
  }

  int passwdCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 1) throw ZvCmdUsage{};
    ZtString oldpw = m_cli.getpass("Current password: ", 100);
    ZtString newpw = m_cli.getpass("New password: ", 100);
    ZtString checkpw = m_cli.getpass("Re-type new password: ", 100);
    if (checkpw != newpw) {
      ctx->out << "passwords do not match\npassword unchanged!\n";
      return 1;
    }
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_ChPass,
	    fbs::CreateUserChPass(m_fbb,
	      str(m_fbb, oldpw),
	      str(m_fbb, newpw)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      if (int code = filterAck(
	    ctx.out, ack, fbs::ReqAckData_ChPass, -1, "password change"))
	return executed(code, &ctx);
      auto &out = ctx.out;
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "password change rejected\n";
	return executed(1, &ctx);
      }
      out << "password changed\n";
      return executed(0, &ctx);
    });
    return 0;
  }

  static void printUser(ZtString &out, const ZvUserDB::fbs::User *user_) {
    using namespace ZvUserDB;
    using namespace Zfb::Load;
    auto hmac_ = bytes(user_->hmac());
    ZtString hmac;
    hmac.length(Ztls::Base64::enclen(hmac_.length()));
    Ztls::Base64::encode(
	hmac.data(), hmac.length(), hmac_.data(), hmac_.length());
    auto secret_ = bytes(user_->secret());
    ZtString secret;
    secret.length(Ztls::Base32::enclen(secret_.length()));
    Ztls::Base32::encode(
	secret.data(), secret.length(), secret_.data(), secret_.length());
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

  int usersCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      fbs::UserIDBuilder fbb_(m_fbb);
      if (argc == 2) fbb_.add_id(ctx->args->getInt64("1", 0, LLONG_MAX, true));
      auto userID = fbb_.Finish();
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_UserGet, userID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_UserGet, -1, "user get"))
	return executed(code, &ctx);
      auto userList = static_cast<const fbs::UserList *>(ack->data());
      using namespace Zfb::Load;
      all(userList->list(), [&out](unsigned, auto user_) {
	printUser(out, user_); out << '\n';
      });
      return executed(0, &ctx);
    });
    return 0;
  }
  int userAddCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZvCmdUsage{};
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
	    fbs::ReqData_UserAdd,
	    fbs::CreateUser(m_fbb,
	      ctx->args->getInt64("1", 0, LLONG_MAX, true),
	      str(m_fbb, ctx->args->get("2")), 0, 0,
	      strVecIter(m_fbb, roles.length(),
		[&roles](unsigned i) { return roles[i]; }),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_UserAdd, -1, "user add"))
	return executed(code, &ctx);
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      if (!userPass->ok()) {
	out << "user add rejected\n";
	return executed(1, &ctx);
      }
      printUser(out, userPass->user()); out << '\n';
      using namespace Zfb::Load;
      out << "passwd=" << str(userPass->passwd()) << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int resetPassCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_ResetPass,
	    fbs::CreateUserID(m_fbb,
	      ctx->args->getInt64("1", 0, LLONG_MAX, true)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_ResetPass, -1, "reset password"))
	return executed(code, &ctx);
      auto userPass = static_cast<const fbs::UserPass *>(ack->data());
      if (!userPass->ok()) {
	out << "reset password rejected\n";
	return executed(1, &ctx);
      }
      printUser(out, userPass->user()); out << '\n';
      using namespace Zfb::Load;
      out << "passwd=" << str(userPass->passwd()) << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int userModCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZvCmdUsage{};
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
	    fbs::ReqData_UserMod,
	    fbs::CreateUser(m_fbb,
	      ctx->args->getInt64("1", 0, LLONG_MAX, true),
	      str(m_fbb, ctx->args->get("2")), 0, 0,
	      strVecIter(m_fbb, roles.length(),
		[&roles](unsigned i) { return roles[i]; }),
	      flags).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_UserMod, -1, "user modify"))
	return executed(code, &ctx);
      auto userUpdAck = static_cast<const fbs::UserUpdAck *>(ack->data());
      if (!userUpdAck->ok()) {
	out << "user modify rejected\n";
	return executed(1, &ctx);
      }
      printUser(out, userUpdAck->user()); out << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int userDelCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_UserDel,
	    fbs::CreateUserID(m_fbb,
	      ctx->args->getInt64("1", 0, LLONG_MAX, true)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_UserDel, -1, "user delete"))
	return executed(code, &ctx);
      auto userUpdAck = static_cast<const fbs::UserUpdAck *>(ack->data());
      if (!userUpdAck->ok()) {
	out << "user delete rejected\n";
	return executed(1, &ctx);
      }
      printUser(out, userUpdAck->user()); out << '\n';
      out << "user deleted\n";
      return executed(0, &ctx);
    });
    return 0;
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

  int rolesCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
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
	    fbs::ReqData_RoleGet, roleID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_RoleGet, -1, "role get"))
	return executed(code, &ctx);
      auto roleList = static_cast<const fbs::RoleList *>(ack->data());
      using namespace Zfb::Load;
      all(roleList->list(), [&out](unsigned, auto role_) {
	printRole(out, role_); out << '\n';
      });
      return executed(0, &ctx);
    });
    return 0;
  }
  int roleAddCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      Bitmap perms{ctx->args->get("2")};
      Bitmap apiperms{ctx->args->get("3")};
      uint8_t flags = 0;
      if (ctx->args->get("immutable")) flags |= Role::Immutable;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_RoleAdd,
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
	    out, ack, fbs::ReqAckData_RoleAdd, -1, "role add"))
	return executed(code, &ctx);
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role add rejected\n";
	return executed(1, &ctx);
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int roleModCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 4) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      Bitmap perms{ctx->args->get("2")};
      Bitmap apiperms{ctx->args->get("3")};
      uint8_t flags = 0;
      if (ctx->args->get("immutable")) flags |= Role::Immutable;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_RoleMod,
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
	    out, ack, fbs::ReqAckData_RoleMod, -1, "role modify"))
	return executed(code, &ctx);
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role modify rejected\n";
	return executed(1, &ctx);
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int roleDelCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_RoleDel,
	    fbs::CreateRoleID(m_fbb, str(m_fbb, ctx->args->get("1"))).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_RoleMod, -1, "role delete"))
	return executed(code, &ctx);
      auto roleUpdAck = static_cast<const fbs::RoleUpdAck *>(ack->data());
      if (!roleUpdAck->ok()) {
	out << "role delete rejected\n";
	return executed(1, &ctx);
      }
      printRole(out, roleUpdAck->role()); out << '\n';
      out << "role deleted\n";
      return executed(0, &ctx);
    });
    return 0;
  }

  int permsCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      fbs::PermIDBuilder fbb_(m_fbb);
      if (argc == 2) fbb_.add_id(ctx->args->getInt("1", 0, Bitmap::Bits, true));
      auto permID = fbb_.Finish();
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_PermGet, permID.Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_PermGet, -1, "perm get"))
	return executed(code, &ctx);
      auto permList = static_cast<const fbs::PermList *>(ack->data());
      using namespace Zfb::Load;
      all(permList->list(), [&out](unsigned, auto perm_) {
	out << ZuBoxed(perm_->id()).fmt(ZuFmt::Right<3, ' '>()) << ' '
	  << str(perm_->name()) << '\n';
      });
      return executed(0, &ctx);
    });
    return 0;
  }
  int permAddCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto name = ctx->args->get("1");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_PermAdd,
	    fbs::CreatePermAdd(m_fbb, str(m_fbb, name)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_PermAdd, -1, "permission add"))
	return executed(code, &ctx);
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission add rejected\n";
	return executed(1, &ctx);
      }
      auto perm = permUpdAck->perm();
      out << "added " << perm->id() << ' ' << str(perm->name()) << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int permModCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 3) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt("1", 0, Bitmap::Bits, true);
      auto permName = ctx->args->get("2");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_PermMod,
	    fbs::CreatePerm(m_fbb, permID, str(m_fbb, permName)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_PermMod, -1, "permission modify"))
	return executed(code, &ctx);
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission modify rejected\n";
	return executed(1, &ctx);
      }
      auto perm = permUpdAck->perm();
      out << "modified " << perm->id() << ' ' << str(perm->name()) << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }
  int permDelCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto permID = ctx->args->getInt("1", 0, Bitmap::Bits, true);
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_PermDel,
	    fbs::CreatePermID(m_fbb, permID).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_PermDel, -1, "permission delete"))
	return executed(code, &ctx);
      using namespace Zfb::Load;
      auto permUpdAck = static_cast<const fbs::PermUpdAck *>(ack->data());
      if (!permUpdAck->ok()) {
	out << "permission delete rejected\n";
	return executed(1, &ctx);
      }
      auto perm = permUpdAck->perm();
      out << "deleted " << perm->id() << ' ' << str(perm->name()) << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }

  int keysCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_OwnKeyGet,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_KeyGet,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_OwnKeyGet, fbs::ReqAckData_KeyGet,
	    "key get"))
	return executed(code, &ctx);
      auto keyIDList = static_cast<const fbs::KeyIDList *>(ack->data());
      using namespace Zfb::Load;
      all(keyIDList->list(), [&out](unsigned, auto key_) {
	out << str(key_) << '\n';
      });
      return executed(0, &ctx);
    });
    return 0;
  }

  int keyAddCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_OwnKeyAdd,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_KeyAdd,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_OwnKeyAdd, fbs::ReqAckData_KeyAdd,
	    "key add"))
	return executed(code, &ctx);
      using namespace Zfb::Load;
      auto keyUpdAck = static_cast<const fbs::KeyUpdAck *>(ack->data());
      if (!keyUpdAck->ok()) {
	out << "key add rejected\n";
	return executed(1, &ctx);
      }
      auto secret_ = bytes(keyUpdAck->key()->secret());
      ZtString secret;
      secret.length(Ztls::Base64::enclen(secret_.length()));
      Ztls::Base64::encode(
	  secret.data(), secret.length(), secret_.data(), secret_.length());
      out << "id: " << str(keyUpdAck->key()->id())
	<< "\nsecret: " << secret << '\n';
      return executed(0, &ctx);
    });
    return 0;
  }

  int keyDelCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      auto keyID = ctx->args->get("1");
      m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	    fbs::ReqData_KeyDel,
	    fbs::CreateKeyID(m_fbb, str(m_fbb, keyID)).Union()));
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_OwnKeyDel, fbs::ReqAckData_KeyDel,
	    "key delete"))
	return executed(code, &ctx);
      using namespace Zfb::Load;
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "key delete rejected\n";
	return executed(1, &ctx);
      }
      out << "key deleted\n";
      return executed(0, &ctx);
    });
    return 0;
  }

  int keyClrCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc < 1 || argc > 2) throw ZvCmdUsage{};
    auto seqNo = m_seqNo++;
    using namespace ZvUserDB;
    {
      using namespace Zfb::Save;
      if (argc == 1)
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_OwnKeyClr,
	      fbs::CreateUserID(m_fbb, m_link->userID()).Union()));
      else {
	auto userID = ZuBox<uint64_t>(ctx->args->get("1"));
	m_fbb.Finish(fbs::CreateRequest(m_fbb, seqNo,
	      fbs::ReqData_KeyClr,
	      fbs::CreateUserID(m_fbb, userID).Union()));
      }
    }
    m_link->sendUserDB(m_fbb, seqNo, [this, ctx = ZuMv(*ctx)](
	const fbs::ReqAck *ack) mutable {
      auto &out = ctx.out;
      if (int code = filterAck(
	    out, ack, fbs::ReqAckData_OwnKeyClr, fbs::ReqAckData_KeyClr,
	    "key clear"))
	return executed(code, &ctx);
      auto userAck = static_cast<const fbs::UserAck *>(ack->data());
      if (!userAck->ok()) {
	out << "key clear rejected\n";
	return executed(1, &ctx);
      }
      out << "keys cleared\n";
      return executed(0, &ctx);
    });
    return 0;
  }

  int remoteCmd(ZvCmdContext *) { return 0; } // unused

  int loadModCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    if (argc != 2) throw ZvCmdUsage{};
    return loadMod(ctx->args->get("1", true), ctx->out);
  }

  int telcapCmd(ZvCmdContext *ctx) {
    ZuBox<int> argc = ctx->args->get("#");
    using namespace ZvTelemetry;
    unsigned interval = ctx->args->getInt("interval", 100, 1000000, false, 0);
    bool subscribe = !ctx->args->getInt("unsubscribe", 0, 1, false, 0);
    if (!subscribe) {
      for (unsigned i = 0; i < TelDataN; i++) m_telcap[i] = TelCap{};
      if (argc > 1) throw ZvCmdUsage{};
    } else {
      if (argc < 2) throw ZvCmdUsage{};
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
	if (types[j] < 0) throw ZvCmdUsage{};
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
	  return 1;
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
	    case ReqType::DBEnv:
	      m_telcap[TelData::DBEnv - TelData::MIN] =
		TelCap::singletonFn<DBEnv>(
		    ZiFile::append(dir, "dbenv.csv"));
	      m_telcap[TelData::DBHost - TelData::MIN] =
		TelCap::keyedFn<DBHost>(
		    ZiFile::append(dir, "dbhost.csv"));
	      m_telcap[TelData::DB - TelData::MIN] =
		TelCap::keyedFn<DB>(
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
	  return 1;
	}
      }
    }
    thread_local ZmSemaphore sem;
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
    if (!allOK) return 1;
    if (subscribe) {
      if (!interval)
	out << "telemetry queried\n";
      else
	out << "telemetry subscribed\n";
    } else
      out << "telemetry unsubscribed\n";
    return 0;
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

  ZmPLock		m_promptLock;
    ZtArray<uint8_t>	  m_prompt;

  ZmRef<Link>		m_link;
  ZvSeqNo		m_seqNo = 0;

  FBB			m_fbb;

  int			m_code = 0;
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

int main(int argc, char **argv)
{
  if (argc < 2) usage();

  ZeLog::init("zcmd");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::lambdaSink(
	[](ZeEvent *e) { std::cerr << e->message() << '\n' << std::flush; }));
  ZeLog::start();

  bool interactive = isatty(fileno(stdin));
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

  ZmRef<ZCmd> client = new ZCmd();

  ZmTrap::sigintFn({client, [](ZCmd *client) { client->post(); }});
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

  if (client->interactive()) std::cout << std::flush;

  client->exiting();
  client->disconnect();

  mx->stop(true);

  ZeLog::stop();

  client->final();

  delete mx;

  ZmTrap::sigintFn(ZmFn<>{});

  return client->code();
}
