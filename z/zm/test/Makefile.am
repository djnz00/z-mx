METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zm/src/libZm.la $(top_builddir)/zu/src/libZu.la \
	@Z_MT_LIBS@
lib_LTLIBRARIES = libZmBTTest.la libZmBTTest2.la
libZmBTTest_la_SOURCES = libZmBTTest.cc
libZmBTTest_la_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
libZmBTTest_la_LIBADD = libZmBTTest2.la
libZmBTTest2_la_SOURCES = libZmBTTest2.cc
libZmBTTest2_la_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
TESTPROGS = ZmFnTest ZmHeapTest ZmHeapTest2 ZmRBTest ZmRWTest \
	ZmSchedTest ZmStackTest ZmTest ZmHashTest ZmHashTest2 ZmTLockTest \
	ZmTTest ZmLHTest ZmHashCleanup ZmHashThread ZmPQueueTest \
	ZmPQueueTest2 ZmPQueueTest3 ZmRingTest ZmRingTest2 \
	ZmLockTest ZmTIDTest ZmAllocTest ZmCacheTest ZmDemangleTest \
	ZmTimeTest ZmAssertTest ZmPolyHashTest ZmPolyCacheTest ZmBench
if MINGW
bin_PROGRAMS = ZmBTTest
noinst_PROGRAMS = ${TESTPROGS}
else
noinst_PROGRAMS = ZmBTTest ${TESTPROGS}
endif
ZmBTTest_SOURCES = ZmBTTest.cc
ZmBTTest_LDADD = libZmBTTest.la ${LDADD}
ZmFnTest_SOURCES = ZmFnTest.cc
ZmHeapTest_SOURCES = ZmHeapTest.cc
ZmHeapTest2_SOURCES = ZmHeapTest2.cc
ZmRBTest_SOURCES = ZmRBTest.cc
ZmRWTest_SOURCES = ZmRWTest.cc
ZmSchedTest_SOURCES = ZmSchedTest.cc
ZmStackTest_SOURCES = ZmStackTest.cc
ZmTest_SOURCES = ZmTest.cc
ZmHashTest_SOURCES = ZmHashTest.cc
ZmHashTest2_SOURCES = ZmHashTest2.cc
ZmTLockTest_SOURCES = ZmTLockTest.cc
ZmTTest_SOURCES = ZmTTest.cc
ZmLHTest_SOURCES = ZmLHTest.cc
ZmHashCleanup_SOURCES = ZmHashCleanup.cc
ZmHashThread_SOURCES = ZmHashThread.cc
ZmPQueueTest_SOURCES = ZmPQueueTest.cc
ZmPQueueTest2_SOURCES = ZmPQueueTest2.cc
ZmPQueueTest3_SOURCES = ZmPQueueTest3.cc
ZmRingTest_SOURCES = ZmRingTest.cc
ZmRingTest2_SOURCES = ZmRingTest2.cc
ZmLockTest_SOURCES = ZmLockTest.cc
ZmTIDTest_SOURCES = ZmTIDTest.cc
ZmAllocTest_SOURCES = ZmAllocTest.cc
ZmCacheTest_SOURCES = ZmCacheTest.cc
ZmDemangleTest_SOURCES = ZmDemangleTest.cc
ZmTimeTest_SOURCES = ZmTimeTest.cc
ZmAssertTest_SOURCES = ZmAssertTest.cc
ZmPolyHashTest_SOURCES = ZmPolyHashTest.cc
ZmPolyCacheTest_SOURCES = ZmPolyCacheTest.cc
ZmBench_SOURCES = ZmBench.cc
