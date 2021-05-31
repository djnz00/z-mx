METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src \
	-I$(top_srcdir)/zi/src -I$(top_srcdir)/zfb/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
FBS = zfbtest.fbs
FBS_H = ${FBS:%.fbs=%_fbs.h}
${FBS_H}: %_fbs.h: fbs/%.fbs
	flatc -c --cpp-field-case lower -I fbs $<
	mv ${@:%_fbs.h=%_generated.h} $@
	perl -pi -e 's|^#include "(\w+)_generated\.h"|#include <zlib/$${1}_fbs.h>|; s|^#include "flatbuffers/flatbuffers.h"|#include <flatbuffers/flatbuffers.h>|;' $@
BUILT_SOURCES = ${FBS_H}
CLEANFILES = ${FBS_H}
noinst_HEADERS = ${FBS_H}
LDADD = $(top_builddir)/zfb/src/libZfb.la \
	$(top_builddir)/zi/src/libZi.la \
	$(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la \
	$(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@FBS_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
noinst_PROGRAMS = zfbtest
zfbtest_SOURCES = zfbtest.cpp zfbtest_generated.h
