METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zu/src/libZu.la @Z_MT_LIBS@
noinst_PROGRAMS = \
	ZuConversionTest ZuHashTest ZuCmpTest ZuBoxTest \
	ZuArrayTest ZuPairTest ZuIDTest ZuUTFTest ZuBitmapTest \
	ZuDecimalTest ZuSearchTest ZuFieldTest ZuTLTest ZuDemangleTest \
	ZuAxorTest
ZuConversionTest_SOURCES = ZuConversionTest.cpp
ZuHashTest_SOURCES = ZuHashTest.cpp Analyze.hpp
ZuCmpTest_SOURCES = ZuCmpTest.cpp
#ZuCmpTest_CXXFLAGS = $(AM_CXXFLAGS) -fprofile-instr-generate -fcoverage-mapping
ZuSearchTest_SOURCES = ZuSearchTest.cpp
ZuBoxTest_SOURCES = ZuBoxTest.cpp
ZuArrayTest_SOURCES = ZuArrayTest.cpp
ZuPairTest_SOURCES = ZuPairTest.cpp
ZuIDTest_SOURCES = ZuIDTest.cpp
ZuUTFTest_SOURCES = ZuUTFTest.cpp
ZuBitmapTest_SOURCES = ZuBitmapTest.cpp
ZuDecimalTest_SOURCES = ZuDecimalTest.cpp
ZuFieldTest_SOURCES = ZuFieldTest.cpp
ZuTLTest_SOURCES = ZuTLTest.cpp
ZuDemangleTest_SOURCES = ZuDemangleTest.cpp
ZuAxorTest_SOURCES = ZuAxorTest.cpp
