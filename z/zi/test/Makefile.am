METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I$(top_srcdir)/zi/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zi/src/libZi.la $(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
noinst_PROGRAMS = \
	ZiFileTest ZiFileAgeTest ZiGlobTest \
	ZiRingTest ZiRingTest2 \
	ZiMxClient ZiMxServer ZiMxUDPClient ZiMxUDPServer
noinst_HEADERS = Global.hpp HttpHeader.hpp
if NETLINK
noinst_PROGRAMS += ZiNetlinkTest
endif
ZiFileTest_SOURCES = ZiFileTest.cpp
ZiFileAgeTest_SOURCES = ZiFileAgeTest.cpp
ZiGlobTest_SOURCES = ZiGlobTest.cpp
ZiRingTest_SOURCES = ZiRingTest.cpp
ZiRingTest2_SOURCES = ZiRingTest2.cpp
ZiMxClient_SOURCES = ZiMxClient.cpp
ZiMxServer_SOURCES = ZiMxServer.cpp
ZiMxUDPClient_SOURCES = ZiMxUDPClient.cpp
ZiMxUDPServer_SOURCES = ZiMxUDPServer.cpp
if NETLINK
ZiNetlinkTest_SOURCES = ZiNetlinkTest.cpp
endif
