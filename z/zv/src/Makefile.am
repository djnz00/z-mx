METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I$(top_srcdir)/zi/src \
	-I$(top_srcdir)/ztls/src -I$(top_srcdir)/zfb/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
FBS = \
	key.fbs \
	perm.fbs \
	role.fbs \
	user.fbs \
	userdb.fbs \
	loginack.fbs \
	loginreq.fbs \
	userdbreq.fbs \
	userdback.fbs \
	zcmdreq.fbs \
	zcmdack.fbs \
	telreq.fbs \
	telack.fbs \
	telemetry.fbs
FBS_H = ${FBS:%.fbs=%_fbs.h}
${FBS_H}: %_fbs.h: fbs/%.fbs
	flatc -c --cpp-field-case-style lower -I fbs -I $(top_srcdir)/zfb/src/fbs $<
	mv ${@:%_fbs.h=%_generated.h} $@
	perl -pi -e 's|^#include "(\w+)_generated\.h"|#include <zlib/$${1}_fbs.h>|; s|^#include "flatbuffers/flatbuffers.h"|#include <flatbuffers/flatbuffers.h>|;' $@
BUILT_SOURCES = ${FBS_H}
CLEANFILES = ${FBS_H}
pkginclude_HEADERS = ZvLib.hpp \
	ZvCf.hpp ZvCSV.hpp ZvError.hpp ZvEnum.hpp ZvHashCSV.hpp \
	ZvHeapCSV.hpp ZvDaemon.hpp \
	ZvRingParams.hpp ZvThreadParams.hpp ZvUserDB.hpp \
	ZvCmdNet.hpp ZvCmdHost.hpp ZvCmdDispatcher.hpp \
	ZvCmdClient.hpp ZvCmdServer.hpp \
	ZvMxParams.hpp \
	ZvSeqNo.hpp ZvMsgID.hpp ZvIOQueue.hpp ZvEngine.hpp \
	ZvTelemetry.hpp ZvTelServer.hpp \
	${FBS_H}
#fbsdir = $(datarootdir)/${PACKAGE}/fbs
#dist_fbs_DATA = fbs/telemetry.fbs
lib_LTLIBRARIES = libZv.la
bin_PROGRAMS = zuserdb
libZv_la_CPPFLAGS = $(AM_CPPFLAGS) -DZV_EXPORTS 
libZv_la_LDFLAGS = @Z_SO_LDFLAGS@
libZv_la_SOURCES = ZvLib.cpp \
	ZvCf.cpp ZvCSV.cpp ZvDaemon.cpp ZvUserDB.cpp ZvEngine.cpp \
	ZvCmdHost.cpp ZvCmdDispatcher.cpp
libZv_la_LIBADD = $(top_builddir)/zfb/src/libZfb.la \
	$(top_builddir)/ztls/src/libZtls.la \
	$(top_builddir)/zi/src/libZi.la $(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@FBS_LIBS@ @MBEDTLS_LIBS@ \
	@Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
zuserdb_LDADD = libZv.la $(libZv_la_LDADD)
zuserdb_SOURCES = zuserdb.cpp
