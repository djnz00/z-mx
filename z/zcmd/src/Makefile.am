METASOURCES = AUTO
AM_CPPFLAGS = \
	-I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I$(top_srcdir)/zi/src \
	-I$(top_srcdir)/ztls/src -I$(top_srcdir)/zfb/src \
	-I$(top_srcdir)/zv/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
pkginclude_HEADERS = ZcmdLib.hh \
	ZcmdClient.hh ZcmdDispatcher.hh ZcmdHost.hh ZcmdNet.hh ZcmdServer.hh
lib_LTLIBRARIES = libZcmd.la
libZcmd_la_CPPFLAGS = $(AM_CPPFLAGS) -DZCMD_EXPORTS
libZcmd_la_LDFLAGS = @Z_SO_LDFLAGS@
libZcmd_la_SOURCES = ZcmdLib.cc \
	ZcmdDispatcher.cc ZcmdHost.cc
libZcmd_la_LIBADD = \
	$(top_builddir)/zfb/src/libZv.la \
	$(top_builddir)/zfb/src/libZfb.la $(top_builddir)/zfb/src/libZtls.la \
	$(top_builddir)/zi/src/libZi.la $(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@FBS_LIBS@ \
	@Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
bin_PROGRAMS = zcmd
zcmd_CPPFLAGS = $(AM_CPPFLAGS) -I$(top_srcdir)/zrl/src
zcmd_LDADD = \
	$(top_builddir)/zrl/src/libZrl.la \
	$(top_builddir)/ztls/src/libZtls.la $(top_builddir)/zfb/src/libZfb.la \
	$(top_builddir)/zv/src/libZv.la $(top_builddir)/zi/src/libZi.la \
	$(top_builddir)/ze/src/libZe.la $(top_builddir)/zt/src/libZt.la \
	$(top_builddir)/zm/src/libZm.la $(top_builddir)/zu/src/libZu.la \
	@Z_TERM_LIBS@ @Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
zcmd_SOURCES = zcmd.cc
