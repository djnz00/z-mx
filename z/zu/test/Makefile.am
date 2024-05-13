METASOURCES = AUTO
.cc.s:
	$(CXXCOMPILE) -c -S $<
AM_CPPFLAGS = -I$(top_srcdir)/zu/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zu/src/libZu.la @Z_MT_LIBS@
noinst_PROGRAMS = \
	ZuInspectTest ZuHashTest ZuCmpTest ZuBoxTest \
	ZuArrayTest ZuPairTest ZuIDTest ZuUTFTest ZuBitmapTest \
	ZuDecimalTest ZuSearchTest ZuFieldTest ZuTLTest \
	ZuAxorTest ZuPtrTest ZuTimeTest #ZuDateTimeTest
ZuInspectTest_SOURCES = ZuInspectTest.cc
ZuHashTest_SOURCES = ZuHashTest.cc Analyze.hh
ZuCmpTest_SOURCES = ZuCmpTest.cc
#ZuCmpTest_CXXFLAGS = $(AM_CXXFLAGS) -fprofile-instr-generate -fcoverage-mapping
ZuSearchTest_SOURCES = ZuSearchTest.cc
ZuBoxTest_SOURCES = ZuBoxTest.cc
ZuArrayTest_SOURCES = ZuArrayTest.cc
ZuPairTest_SOURCES = ZuPairTest.cc
ZuIDTest_SOURCES = ZuIDTest.cc
ZuUTFTest_SOURCES = ZuUTFTest.cc
ZuBitmapTest_SOURCES = ZuBitmapTest.cc
ZuDecimalTest_SOURCES = ZuDecimalTest.cc
ZuFieldTest_SOURCES = ZuFieldTest.cc
ZuTLTest_SOURCES = ZuTLTest.cc
ZuAxorTest_SOURCES = ZuAxorTest.cc
ZuPtrTest_SOURCES = ZuPtrTest.cc
ZuTimeTest_SOURCES = ZuTimeTest.cc
#ZuDateTimeTest_SOURCES = ZuDateTimeTest.cc
