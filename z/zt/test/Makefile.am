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
ZtArrayTest_SOURCES = ZtArrayTest.cc
ZtDateTest_SOURCES = ZtDateTest.cc
ZtDateFixTest_SOURCES = ZtDateFixTest.cc
ZtRegexTest_SOURCES = ZtRegexTest.cc
ZtStringHash_SOURCES = ZtStringHash.cc
ZtStringTest_SOURCES = ZtStringTest.cc
vsntest_SOURCES = vsntest.cc
ZtIconvTest_SOURCES = ZtIconvTest.cc
ZtBitWindowTest_SOURCES = ZtBitWindowTest.cc
ZtFieldTest_SOURCES = ZtFieldTest.cc
