METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I$(top_srcdir)/zi/src \
	-I$(top_srcdir)/ztls/src -I$(top_srcdir)/zfb/src \
	-I$(top_srcdir)/zv/src \
	-DZDF_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
FBS = dataframe.fbs
FBS_H = ${FBS:%.fbs=%_fbs.h}
${FBS_H}: %_fbs.h: fbs/%.fbs
	flatc -c --no-warnings -I fbs -I $(top_srcdir)/zfb/src/fbs $<
	mv ${@:%_fbs.h=%_generated.h} $@
	perl -pi -e 's|^#include "(\w+)_generated\.h"|#include <zlib/$${1}_fbs.h>|; s|^#include "flatbuffers/flatbuffers.h"|#include <flatbuffers/flatbuffers.h>|;' $@
BUILT_SOURCES = ${FBS_H}
CLEANFILES = ${FBS_H}
pkginclude_HEADERS = ZdfLib.hpp \
	ZdfCompress.hpp ZdfBuf.hpp ZdfMgr.hpp \
	ZdfSeries.hpp ZdfMem.hpp ZdfFile.hpp Zdf.hpp ZdfStats.hpp \
	${FBS_H} \
	stats_tree.hpp
#fbsdir = $(datarootdir)/${PACKAGE}/fbs
#dist_fbs_DATA = fbs/telemetry.fbs
lib_LTLIBRARIES = libZdf.la
libZdf_la_LDFLAGS = @Z_SO_LDFLAGS@
libZdf_la_SOURCES = ZdfLib.cpp \
	ZdfBuf.cpp ZdfMgr.cpp ZdfMem.cpp ZdfFile.cpp Zdf.cpp
libZdf_la_LIBADD = \
	$(top_builddir)/zv/src/libZv.la \
	$(top_builddir)/zfb/src/libZfb.la \
	$(top_builddir)/ztls/src/libZtls.la \
	$(top_builddir)/zi/src/libZi.la $(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@FBS_LIBS@ @MBEDTLS_LIBS@ @Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
