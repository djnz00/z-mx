//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// command line interface - line editor

#ifndef ZrlEditor_HH
#define ZrlEditor_HH

#ifndef ZrlLib_HH
#include <zlib/ZrlLib.hh>
#endif

#include <zlib/ZuPtr.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZtArray.hh>

#include <zlib/ZmLHash.hh>
#include <zlib/ZmRBTree.hh>

#include <zlib/ZtEnum.hh>
#include <zlib/ZtWindow.hh>

#include <zlib/ZvCf.hh>

#include <zlib/ZrlLine.hh>
#include <zlib/ZrlTerminal.hh>
#include <zlib/ZrlApp.hh>
#include <zlib/ZrlConfig.hh>

namespace Zrl {

namespace Op { // line editor operation codes
  ZtEnumValues(Zrl.Op, int8_t,
    Null,		// sentinel

    Nop,		// no-operation
    Syn,		// synthetic keystroke

    Mode,		// switch mode
    Push,		// push mode (and switch)
    Pop,		// pop mode

    // terminal driver events and control keys (from termios)
    Error,		// I/O error - causes stop
    EndOfFile,		// ^D EOF - causes stop

    SigInt,		// ^C
    SigQuit,		// quit (ctrl-backslash)
    SigSusp,		// ^Z (SIGTSTP)

    Enter,		// line entered

    // Erase,		// ^H backspace		// BackSpace
    // WErase,		// ^W word erase	// RevWord|Del|Unix
    // Kill,		// ^U			// Home|Del

    // LNext,		// ^V		// Glyph(0, '^'),Left|Mv,Push(1)

    // single glyph/row motions
    Up,
    Down,
    Left,
    Right,
    Home,
    End,

    // Delete,		// delete forward	// Right|Del

    // word motions - Unix flag implies "Unix" white-space delimited word
    FwdWord,
    RevWord,
    FwdWordEnd,
    RevWordEnd,

    MvMark,		// move to glyph mark
    ClrVis,		// clear highlight (can use Del and Copy flags)

    InsToggle,		// insert/overwrite toggle
    Insert,		// insert
    Over,		// overwrite

    Clear,		// clear screen and redraw line
    Redraw,		// redraw line

    Paste,		// pastes register 0 (i.e. most recent cut/copy)
    Yank,		// Emacs "yanks" the top of kill ring (per yank offset)
    Rotate,		// rotates kill ring (increments yank offset modulo 10)

    Glyph,		// insert/overwrite glyph (depending on toggle)
    InsGlyph,		// insert glyph
    OverGlyph,		// overwrite glyph
    BackSpace,		// back space, falling back to Left[Del]
    Edit,		// upcoming repeatable edit - set repeat count
    EditRep,		// repeat last edit as required

    ArgDigit,		// append digit to argument

    Register,		// specify register (0-9 a-z + *) for next cmd

    Undo,		// undo
    Redo,		// redo
    EmacsUndo,		// undo/redo, Emacs style
    EmacsAbort,		// abort undo, ''
    Repeat,		// repeat last edit

    TransGlyph,		// transpose glyphs
    TransWord,		// transpose words
    TransUnixWord,	// transpose white-space delimited words

    CapGlyph,		// capitalize glyph (toggles capitalization)
    LowerWord,		// lower-case word
    UpperWord,		// upper-case word
    CapWord,		// capitalize word (rotates through ucfirst, uc, lc)

    LowerVis,		// lower-case visual highlight
    UpperVis,		// upper-case ''
    CapVis,		// capitalize ''

    XchMark,		// swap cursor with glyph mark

    // glyph search
    FwdGlyphSrch,	// fwd glyph search
    RevGlyphSrch,	// rev ''

    // auto-completion
    Complete,		// attempt completion
    RevComplete,	// revert completion
    ListComplete,	// list possible completions

    // history
    Next,		// also triggered by Down from bottom row
    Prev,		// also triggered by Up from top row

    // immediate/incremental search
    ClrIncSrch,		// clear incremental search
    FwdIncSrch,		// append vkey to search term, fwd search
    RevIncSrch,		// '', rev search

    // prompted search
    PromptSrch,		// prompt for non-incremental search
    			//   (stash line, prompt is vkey)
    EnterSrchFwd,	// enter non-incremental search term, fwd search
    EnterSrchRev,	// '', rev search
    AbortSrch,		// abort search prompt, restore line

    // repeat search
    FwdSearch,		// fwd search
    RevSearch		// rev ''
  );

  // modifiers
  enum {
    Mask	= 0x007f,

    KeepArg	= 0x0080,	// retain register selection
    KeepReg	= 0x0100,	// retain argument

    // Left/Right/Home/End/{Fwd,Rev}*{Word,WordEnd,GlyphSrch}/MvMark
    Mv	 	= 0x0200,	// move cursor
    Del	 	= 0x0400,	// delete span (implies move)
    Copy	= 0x0800,	// copy span (cut is Del + Copy)
    Draw	= 0x1000,	// (re)draw span (normally, unless Vis set)
    Vis		= 0x2000,	// highlight (standout) (implies Draw set)

    // {Fwd,Rev}*{Word,WordEnd}
    Unix	= 0x4000,	// a "Unix" word is white-space delimited
    // {Fwd,Rev}*{WordEnd}
    Past	= 0x8000	// move past end
  };

  ZrlExtern void print_(uint32_t, ZuVStream &);
  struct Print {
    uint32_t op;
    template <typename S>
    void print(S &s_) const { ZuVStream s{s_}; print_(op, s); }
    void print(ZuVStream &s) const { print_(op, s); }
    friend ZuPrintFn ZuPrintType(Print *);
  };
  inline Print print(uint32_t op) { return {op}; }
}

// line editor command - combination of op code, argument and virtual key
class ZrlAPI Cmd {
  enum Internal_ { Internal }; // disambiguator

public:
  static constexpr int16_t nullArg() { return -0x8000; }
  static bool nullArg(int16_t arg) { return arg == nullArg(); }

  Cmd() = default;
  Cmd(const Cmd &) = default;
  Cmd &operator =(const Cmd &) = default;
  Cmd(Cmd &&) = default;
  Cmd &operator =(Cmd &&) = default;

  Cmd(uint64_t op, int16_t arg = nullArg(), int32_t vkey = -VKey::Null) :
      m_value{op |
	(static_cast<uint64_t>(static_cast<uint16_t>(arg))<<16) |
	(static_cast<uint64_t>(vkey)<<32)} { }

  auto op() const { return m_value & 0xffffU; }
  int16_t arg() const { return (m_value>>16) & 0xffffU; }
  int32_t vkey() const { return m_value>>32; }

  bool operator !() const { return !op(); }
  ZuOpBool

  Cmd negArg() const {
    auto arg = this->arg();
    arg = -arg;
    return Cmd{Internal,
      (m_value & (~0xffffU<<16)) | (static_cast<uint64_t>(arg)<<16)};
  }

  int parse(ZuString, int off);

  void print_(ZuVStream &) const;
  template <typename S> void print(S &s_) const { ZuVStream s{s_}; print_(s); }
  void print(ZuVStream &s) const { print_(s); }
  friend ZuPrintFn ZuPrintType(Cmd *);

private:
  Cmd(Internal_, uint64_t value) : m_value{value} { }

  uint64_t	m_value = 0;
};

using CmdSeq = ZtArray<Cmd>;

struct ZrlAPI Binding { // maps a vkey to a sequence of commands
  int32_t	vkey = -VKey::Null;
  CmdSeq	cmds;

  static auto KeyAxor(const Binding *b) { return b->vkey; }
  static auto ValAxor(const Binding *b) { return b; }

  void print_(ZuVStream &) const;
  template <typename S> void print(S &s_) const { ZuVStream s{s_}; print_(s); }
  void print(ZuVStream &s) const { print_(s); }
  friend ZuPrintFn ZuPrintType(Binding *);
};

using Bindings_ =
  ZmLHash<ZuPtr<Binding>,
    ZmLHashKeyVal<Binding::KeyAxor, Binding::ValAxor>>;

struct Bindings : public Bindings_ {
  Bindings() : Bindings_{ZmHashParams{}.bits(8).loadFactor(1.0)} { }
};

namespace ModeType {
  ZtEnumValues_(int8_t, Edit, Command, Base);
  ZtEnumNames_(ModeType, "edit", "command", "base");
}

// line editor mode
struct Mode {
  ZmRef<Bindings>	bindings;	// vkey -> command sequence bindings
  int			type = 0;	// ModeType
};

// key map
struct ZrlAPI Map_ {
  using ID = ZtString;

  ID		id;		// identifier for map
  ZtArray<Mode>	modes;		// modes

  static auto IDAxor(const Map_ &m) { return m.id; }

  int parse(ZuString s, int off);

  void addMode(unsigned mode, int type);
  void bind(unsigned mode, int32_t vkey, CmdSeq cmds);
  void reset(); // reset all modes and key bindings

  void print_(ZuVStream &) const;
  template <typename S> void print(S &s_) const { ZuVStream s{s_}; print_(s); }
  void print(ZuVStream &s) const { print_(s); }
  friend ZuPrintFn ZuPrintType(Map_ *);

private:
  int parseMode(ZuString s, int off);
};
struct Map_IDAccessor {
  static const Map_::ID &get(const Map_ &m) { return m.id; }
};

using Maps =
  ZmRBTree<Map_,
    ZmRBTreeNode<Map_,
      ZmRBTreeKey<Map_::IDAxor>>>;

using Map = Maps::Node;

// Note: this implementation of registers isn't strictly consistent
// with vi or vim/gvim (and they in turn are not mutually consistent)
//
// vi/vim both distinguish yanks from deletes, and distinguish small
// intra-line deletes from larger multi-line deletes
//
// in both traditional vi and vim:
//   - yanking (copying) stores into register "0
//   - deleting (cutting) shifts up registers "1-"9 and stores into "1
//   - small deletes within a line use the 'small delete' register "-
//     and do not update registers "1-"9
//   - all yanks/deletes update the shadow 'unnamed' register "", which
//     is used as a default for retrieval, to point to the last yank/delete
//     (i.e. "0, "1 or "-)
//
// in vim:
//   - storing explicitly to "" selects "0
//
// in traditional vi:
//   - "- and "" are internal and are not accessible as named registers
//
// none of this quirkiness embodies good product design or usability -
// most vi users are likely unaware of these nuances; vi mode emulators
// typically neglect register handling in its entirety; in this implementation
// all yanks and deletes shift up registers 0>9 and store into register "0,
// and the 'unnamed' register is always implicitly "0; "" and "- do not
// separately exist and are aliased to "0; these design choices retain
// the usability of multiple registers while adopting a logically consistent
// use of the numbered registers, and the implementation can be shared with
// Emacs emulation

using RegData = ZtArray<uint8_t>;
using Register = ZuPtr<RegData>;

class Registers { // maintains a unified Vi/Emacs register store
public:
  static int index(char c) {
    if (ZuLikely(c >= '0' && c <= '9')) return c - '0';	
    if (ZuLikely(c >= 'a' && c <= 'z')) return c - 'a' + 10;
    if (ZuLikely(c >= 'A' && c <= 'Z')) return c - 'A' + 10;
    if (ZuLikely(c == '/')) return 36;		// search string
    if (ZuLikely(c == '+')) return 37;		// clipboard
    if (ZuLikely(c == '*')) return 38;		// alt. clipboard
    if (ZuLikely(c == '"')) return 0;		// alias for "0
    if (ZuLikely(c == '-')) return 0;		// alias for "0
    return -1;
  }

  const RegData &get(unsigned i) {
    static const RegData null;
    if (RegData *ptr = m_array[i]) return *ptr;
    return null;
  }
  RegData &set(unsigned i) {
    if (!m_array[i]) m_array[i] = new RegData{};
    return *(m_array[i]);
  }

  // Vi yank / put
  RegData &vi_yank() {
    m_offset = 0;
    if (m_count < 10) ++m_count;
    for (unsigned i = 10; --i > 0; ) m_array[i] = ZuMv(m_array[i - 1]);
    m_array[0] = new RegData{};
    return *(m_array[0]);
  }
  const RegData &vi_put() { return get(0); }

  // Emacs yank / yank-pop
  const RegData &emacs_yank() { return get(m_offset); }
  void emacs_rotateFwd() {
    if (ZuUnlikely(++m_offset >= m_count)) m_offset = 0;
  }
  void emacs_rotateRev() {
    if (ZuUnlikely(!m_offset--)) m_offset = !m_count ? 0 : m_count - 1;
  }

private:
  Register	m_array[39];
  unsigned	m_offset = 0;	// mod10 offset
  unsigned	m_count = 0;
};

struct UndoOp {
  int			oldPos = -1;	// position prior to splice
  int			spliceOff = -1;	// offset of splice
  ZtArray<uint8_t>	oldData;
  ZtArray<uint8_t>	newData;
  bool			last = true;	// whether last in a sequence

  bool operator !() const { return oldPos < 0; }
  ZuOpBool
};

using Undo = ZtWindow<UndoOp>;

// command execution context
struct CmdContext {
  // cursor position and line UTF data are maintained by ZrlTerminal

  ZtArray<uint8_t>	prompt;		// current prompt
  unsigned		startPos = 0;	// start position (following prompt)
  unsigned		mode = 0;	// current mode
  ZtArray<unsigned>	stack;		// mode stack
  int32_t		synVKey = -VKey::Null; // pending synthetic keystroke
  Cmd			prevCmd;	// previous command
  int			horizPos = -1;	// vertical motion position
  int			markPos = -1;	// glyph mark / highlight begin pos.
  int			highPos = -1;	// highlight end position

  // numerical argument context
  int			arg = Cmd::nullArg(); // extant arg
  unsigned		accumArg_ = 0;	// accumulating argument

  // register context
  int			register_ = -1;	// register index
  Registers		registers;	// registers

  // undo buffer
  Undo			undo;		// undo buffer
  UndoOp		editOp;		// pending edit
  int			editArg = -1;	// repeat count for ''
  unsigned		undoNext = 0;	// undo index of next op
  int			undoIndex = -1;	// undo index of undo/redo
  int			undoPos = -1;	// saved position prior to first undo

  // history context
  int			histLoadOff = -1;	// history load offset
  unsigned		histSaveOff = 0;	// history save offset

  // history search context
  ZtArray<uint8_t>	srchTerm;		// search term
  ZuUTFSpan		srchPrmptSpan;		// search prompt span
  bool			srchFwd = true;		// search direction

  // insert/overwrite mode
  bool			overwrite = false;

  // Emacs' bizarre undo/redo navigation
  bool			emacsRedo = false;	// Emacs undo is redo

  void accumDigit(unsigned i) {
    accumArg_ = accumArg_ * 10 + i;
  }

  void accumArg() {
    if (accumArg_) {
      if (arg <= 0)
	arg = accumArg_;
      else
	arg *= accumArg_;
      accumArg_ = 0;
    }
  }

  unsigned evalArg(int cmdArg, unsigned defArg) {
    if (!Cmd::nullArg(cmdArg)) return cmdArg; // do not consume
    if (arg < 0) return defArg;
    return arg;
  }

  void clrArg() {
    arg = Cmd::nullArg();
    accumArg_ = 0;
  }

  void edit(int pos, int off) {
    // editArg should be preserved
    if (!editOp) editOp = { pos, off };
  }

  void appendEdit() {
    if (undoIndex >= 0) { // edit following undo, abandoning history
      undoNext = undoIndex;
      undoIndex = -1;
    }
  }

  void applyEdit() {
    if (editOp.newData) {
      appendEdit();
      undo.set(undoNext++, ZuMv(editOp));
      clrEdit();
    }
  }

  void clrEdit() {
    editOp = {};
    editArg = -1;
  }

  void clrUndo() {
    clrEdit();
    undoIndex = -1;
    undoNext = 0;
    undo.clear();
  }
  
  void reset() {
    startPos = 0;

    mode = 0;
    stack.clear();

    synVKey = -VKey::Null;
    prevCmd = {};

    horizPos = -1;
    markPos = -1;
    highPos = -1;

    clrArg();
    register_ = -1;

    clrUndo();

    histLoadOff = -1;

    srchTerm = {};
    srchPrmptSpan = {};
  }
};

// the line editor is a virtual machine that executes sequences of commands;
// each command sequence is bound to a virtual key; individual commands
// consist of an opcode, an optionally overridden argument and an
// optional overridden/re-mapped virtual key (UTF32 if positive)
class ZrlAPI Editor {
  Editor(const Editor &) = delete;
  Editor &operator =(const Editor &) = delete;
  Editor(Editor &&) = delete;
  Editor &operator =(Editor &&) = delete;

  using CmdFn = bool (Editor::*)(Cmd, int32_t);

public:
  Editor();

  // initialization/finalization
  void init(App app) { init(Config{}, ZuMv(app)); }
  void init(Config config, App app);
  void final();

  bool loadMap(ZuString file, bool select = false); // must call init() first
  const ZtString &loadError() const { return m_loadError; }

  // terminal open/close
  void open(ZmScheduler *sched, unsigned thread);
  void close();
  bool isOpen() const;

  // can be called before start(), or from within terminal thread once running
  ZtString getpass(ZuString prompt, unsigned passLen) {
    return m_tty.getpass(prompt, passLen);
  }

  // print using std cout/cerr
  template <typename L>
  void print(L l) {
    m_tty.invoke([this, l = ZuMv(l)]() {
      m_tty.opost_off();
      std::cout << "\r";
      l();
      m_tty.opost_on();
      m_tty.redraw();
    });
  }

  // start/stop
  using StartFn = ZmFn<void(Editor &)>;
  void start(StartFn = {});
  void stop();
  bool running() const;

  bool map(ZuString id);		// terminal thread

  // dump key bindings
  Terminal::DumpVKeys dumpVKeys() const { return {m_tty}; }
  void dumpMaps_(ZuVStream &) const;
  struct DumpMaps {
    const Editor &editor;
    template <typename S>
    void print(S &s_) const { ZuVStream s{s_}; editor.dumpMaps_(s); }
    void print(ZuVStream &s) const { editor.dumpMaps_(s); }
    friend ZuPrintFn ZuPrintType(DumpMaps *);
  };
  DumpMaps dumpMaps() const { return {*this}; }

private:
  void prompt_();

  bool process(int32_t vkey);
  bool process_(int32_t vkey);
  bool process__(const CmdSeq &cmds, int32_t vkey);

  bool cmdSyn(Cmd, int32_t);
  bool cmdNop(Cmd, int32_t);

  bool cmdMode(Cmd, int32_t);
  bool cmdPush(Cmd, int32_t);
  bool cmdPop(Cmd, int32_t);

  bool cmdError(Cmd, int32_t);
  bool cmdEndOfFile(Cmd, int32_t);

  bool cmdSigInt(Cmd, int32_t);
  bool cmdSigQuit(Cmd, int32_t);
  bool cmdSigSusp(Cmd, int32_t);

  bool cmdEnter(Cmd, int32_t);

  ZuArray<const uint8_t> substr(unsigned begin, unsigned end);

  // is current mode type Command
  bool commandMode();
  // align cursor within line if not in an edit mode
  unsigned align(unsigned pos);
  // splice data in line - clears histLoadOff since line is being modified
  void splice(
      unsigned off, ZuUTFSpan span,
      ZuArray<const uint8_t> replace, ZuUTFSpan rspan,
      bool append, bool last = true);
  void splice_(
      unsigned off, ZuUTFSpan span,
      ZuArray<const uint8_t> replace, ZuUTFSpan rspan,
      bool append);
  // perform copy/del/move in conjunction with a cursor motion
  void motion(unsigned op, unsigned off,
      unsigned begin, unsigned end,
      unsigned begPos, unsigned endPos);
  // maintains consistent horizontal position during vertical movement
  unsigned horizPos();

  bool cmdUp(Cmd, int32_t);
  bool cmdDown(Cmd, int32_t);
  bool cmdLeft(Cmd, int32_t);
  bool cmdRight(Cmd, int32_t);
  bool cmdHome(Cmd, int32_t);
  bool cmdEnd(Cmd, int32_t);
  bool cmdFwdWord(Cmd, int32_t);
  bool cmdRevWord(Cmd, int32_t);
  bool cmdFwdWordEnd(Cmd, int32_t);
  bool cmdRevWordEnd(Cmd, int32_t);
  bool cmdMvMark(Cmd, int32_t);

  bool cmdClrVis(Cmd, int32_t);

  bool cmdInsToggle(Cmd, int32_t);
  bool cmdInsert(Cmd, int32_t);
  bool cmdOver(Cmd, int32_t);

  bool cmdClear(Cmd, int32_t);
  bool cmdRedraw(Cmd, int32_t);

  bool cmdPaste(Cmd, int32_t);

  bool cmdYank(Cmd, int32_t);
  bool cmdRotate(Cmd, int32_t);

  // insert/overwrite glyphs
  void edit(ZuArray<const uint8_t> replace, ZuUTFSpan rspan, bool overwrite);
  bool glyph(Cmd, int32_t vkey, bool overwrite);

  bool cmdGlyph(Cmd cmd, int32_t);
  bool cmdInsGlyph(Cmd cmd, int32_t);
  bool cmdOverGlyph(Cmd cmd, int32_t);
  bool cmdBackSpace(Cmd cmd, int32_t);
  bool cmdEdit(Cmd cmd, int32_t);
  bool cmdEditRep(Cmd cmd, int32_t);

  bool cmdTransGlyph(Cmd, int32_t);
  bool cmdTransWord(Cmd, int32_t);
  bool cmdTransUnixWord(Cmd, int32_t);

public:
  typedef void (*TransformCharFn)(uint8_t, uint8_t &);
  typedef void (*TransformSpanFn)(TransformCharFn, ZuArray<uint8_t>);
private:
  void transformWord(TransformSpanFn, TransformCharFn);
  void transformVis(TransformSpanFn, TransformCharFn);

  bool cmdCapGlyph(Cmd, int32_t);
  bool cmdLowerWord(Cmd, int32_t);
  bool cmdUpperWord(Cmd, int32_t);
  bool cmdCapWord(Cmd, int32_t);

  bool cmdLowerVis(Cmd, int32_t);
  bool cmdUpperVis(Cmd, int32_t);
  bool cmdCapVis(Cmd, int32_t);

  bool cmdSetMark(Cmd, int32_t);
  bool cmdXchMark(Cmd, int32_t);

  bool cmdArgDigit(Cmd, int32_t);

  bool cmdRegister(Cmd, int32_t);

  bool cmdUndo(Cmd, int32_t);
  bool cmdRedo(Cmd, int32_t);
  bool cmdEmacsUndo(Cmd, int32_t);
  bool cmdEmacsAbort(Cmd, int32_t);
  bool cmdRepeat(Cmd, int32_t);

  bool cmdFwdGlyphSrch(Cmd, int32_t);
  bool cmdRevGlyphSrch(Cmd, int32_t);

  void initComplete();		// initialize completions context
  void finalComplete();		// finalize completions context
  void startComplete();		// (re-)start completions enumeration
  void complete(bool next);	// called by cmdComplete() / cmdRevComplete()
  void spliceCompletion(	// splice completion into line
    unsigned off,
    ZuUTFSpan span,
    ZuArray<const uint8_t> replace,
    ZuUTFSpan rspan);
  bool cmdComplete(Cmd, int32_t);
  bool cmdRevComplete(Cmd, int32_t);
  bool cmdListComplete(Cmd, int32_t);

  // loads data previously retrieved from app at history offset
  void histLoad(int offset, ZuArray<const uint8_t> data, bool save);

  bool cmdNext(Cmd, int32_t);
  bool cmdPrev(Cmd, int32_t);

  // adds a key to incremental search term
  bool addIncSrch(int32_t vkey);

  // simple/fast substring matcher - returns true if search term is in data
  bool match(ZuArray<const uint8_t> data);
  // searches forward skipping N-1 matches - returns true if found
  bool searchFwd(int arg);
  // searches backward skipping N-1 matches - returns true if found
  bool searchRev(int arg);

  bool cmdClrIncSrch(Cmd, int32_t);
  bool cmdFwdIncSrch(Cmd, int32_t);
  bool cmdRevIncSrch(Cmd, int32_t);

  bool cmdPromptSrch(Cmd cmd, int32_t vkey);

  // perform non-incremental search operation (abort, forward or reverse)
  struct SearchOp { enum { Abort = 0, Fwd, Rev }; };
  void srchEndPrompt(int op);

  bool cmdEnterSrchFwd(Cmd, int32_t);
  bool cmdEnterSrchRev(Cmd, int32_t);
  bool cmdAbortSrch(Cmd, int32_t);

  bool cmdFwdSearch(Cmd, int32_t);
  bool cmdRevSearch(Cmd, int32_t);

private:
  CmdFn		m_cmdFn[Op::N] = { nullptr };	// opcode jump table

  Config	m_config;		// configuration

  ZtString	m_loadError;		// key map file load error
  Maps		m_maps;			// key maps
  ZuPtr<Map>	m_defltMap;		// default map
  Map		*m_map = nullptr;	// current map

  App		m_app;			// application callbacks

  Terminal	m_tty;			// terminal

  CmdContext	m_context;		// command execution context

  CompSpliceFn	m_compSpliceFn;		// splice callback for completions
};

} // Zrl

#endif /* ZrlEditor_HH */
