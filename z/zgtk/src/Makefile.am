METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I$(top_srcdir)/zi/src \
	-I$(top_srcdir)/ztls/src -I$(top_srcdir)/zfb/src \
	-I$(top_srcdir)/zv/src \
	@Z_CPPFLAGS@
AM_CFLAGS = @GTK_CFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@ @GTK_CFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @GTK_LDFLAGS@
FBS = request.fbs reqack.fbs link.fbs version.fbs
FBS_H = ${FBS:%.fbs=%_fbs.h}
${FBS_H}: %_fbs.h: fbs/%.fbs
	flatc -c --cpp-field-case-style lower -I fbs -I $(top_srcdir)/zv/src/fbs $<
	mv ${@:%_fbs.h=%_generated.h} $@
	perl -pi -e 's|^#include "(\w+)_generated\.h"|#include <zlib/$${1}_fbs.h>|; s|^#include "flatbuffers/flatbuffers.h"|#include <flatbuffers/flatbuffers.h>|;' $@
BUILT_SOURCES = ${FBS_H}
CLEANFILES = ${FBS_H}
pkginclude_HEADERS = ZGtkLib.hpp \
	ZGtkApp.hpp ZGtkCallback.hpp ZGtkValue.hpp \
	ZGtkTreeModel.hpp
#fbsdir = $(datarootdir)/${PACKAGE}/fbs
#dist_fbs_DATA = ${FBS:%=fbs/%}
lib_LTLIBRARIES = libZGtk.la
libZGtk_la_CPPFLAGS = $(AM_CPPFLAGS) -DZGTK_EXPORTS 
libZGtk_la_LDFLAGS = @Z_SO_LDFLAGS@
libZGtk_la_SOURCES = ZGtkLib.cpp ZGtkApp.cpp
libZGtk_la_LIBADD = $(top_builddir)/zv/src/libZv.la \
	$(top_builddir)/zfb/src/libZfb.la $(top_builddir)/ztls/src/libZtls.la \
	$(top_builddir)/zi/src/libZi.la $(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@GTK_LIBS@ @FBS_LIBS@ @MBEDTLS_LIBS@ \
	@Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
bin_PROGRAMS = zdash
zdash_CPPFLAGS = $(AM_CPPFLAGS) \
	-I$(top_srcdir)/zdf/src \
	-DDATADIR='"$(datadir)"'
zdash_CXXFLAGS = ${AM_CXXFLAGS} -ftemplate-backtrace-limit=0 #-fprofile-instr-generate -fcoverage-mapping
zdash_LDADD = libZGtk.la \
	$(top_builddir)/zdf/src/libZdf.la \
	$(libZGtk_la_LDADD)
if MINGW
zdash_LDFLAGS = ${AM_LDFLAGS} -mwindows
endif
zdash_SOURCES = zdash.cpp
