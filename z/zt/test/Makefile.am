METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src -I$(top_srcdir)/zt/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zt/src/libZt.la $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@Z_ZT_LIBS@ @Z_MT_LIBS@
noinst_PROGRAMS = \
	ZtArrayTest ZtDateTest ZtDateFixTest ZtRegexTest \
	ZtStringHash ZtStringTest vsntest ZtIconvTest ZtBitWindowTest \
	ZtFieldTest
ZtArrayTest_SOURCES = ZtArrayTest.cpp
ZtDateTest_SOURCES = ZtDateTest.cpp
ZtDateFixTest_SOURCES = ZtDateFixTest.cpp
ZtRegexTest_SOURCES = ZtRegexTest.cpp
ZtStringHash_SOURCES = ZtStringHash.cpp
ZtStringTest_SOURCES = ZtStringTest.cpp
vsntest_SOURCES = vsntest.cpp
ZtIconvTest_SOURCES = ZtIconvTest.cpp
ZtBitWindowTest_SOURCES = ZtBitWindowTest.cpp
ZtFieldTest_SOURCES = ZtFieldTest.cpp
