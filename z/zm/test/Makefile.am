METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zm/src/libZm.la $(top_builddir)/zu/src/libZu.la \
	@Z_MT_LIBS@
lib_LTLIBRARIES = libZmBTTest.la libZmBTTest2.la
libZmBTTest_la_SOURCES = libZmBTTest.cpp
libZmBTTest_la_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
libZmBTTest_la_LIBADD = libZmBTTest2.la
libZmBTTest2_la_SOURCES = libZmBTTest2.cpp
libZmBTTest2_la_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
TESTPROGS = ZmFnTest ZmHeapTest ZmRBTest ZmRWTest \
	ZmSchedTest ZmStackTest ZmTest ZmHashTest ZmHashTest2 ZmTLockTest \
	ZmTTest ZmLHTest ZmHashCleanup ZmHashThread ZmPQueueTest \
	ZmPQueueTest2 ZmPQueueTest3 ZmRingTest ZmRingTest2 ZmRingTest3 \
	ZmLockTest ZmTLSTest ZmAllocTest
if MINGW
bin_PROGRAMS = ZmBTTest
noinst_PROGRAMS = ${TESTPROGS}
else
noinst_PROGRAMS = ZmBTTest ${TESTPROGS}
endif
ZmBTTest_SOURCES = ZmBTTest.cpp
ZmBTTest_LDADD = libZmBTTest.la ${LDADD}
ZmFnTest_SOURCES = ZmFnTest.cpp
ZmHeapTest_SOURCES = ZmHeapTest.cpp
ZmRBTest_SOURCES = ZmRBTest.cpp
ZmRWTest_SOURCES = ZmRWTest.cpp
ZmSchedTest_SOURCES = ZmSchedTest.cpp
ZmStackTest_SOURCES = ZmStackTest.cpp
ZmTest_SOURCES = ZmTest.cpp
ZmHashTest_SOURCES = ZmHashTest.cpp
ZmHashTest2_SOURCES = ZmHashTest2.cpp
ZmTLockTest_SOURCES = ZmTLockTest.cpp
ZmTTest_SOURCES = ZmTTest.cpp
ZmLHTest_SOURCES = ZmLHTest.cpp
ZmHashCleanup_SOURCES = ZmHashCleanup.cpp
ZmHashThread_SOURCES = ZmHashThread.cpp
ZmPQueueTest_SOURCES = ZmPQueueTest.cpp
ZmPQueueTest2_SOURCES = ZmPQueueTest2.cpp
ZmPQueueTest3_SOURCES = ZmPQueueTest3.cpp
ZmRingTest_SOURCES = ZmRingTest.cpp
ZmRingTest2_SOURCES = ZmRingTest2.cpp
ZmRingTest3_SOURCES = ZmRingTest3.cpp
ZmLockTest_SOURCES = ZmLockTest.cpp
ZmTLSTest_SOURCES = ZmTLSTest.cpp
ZmAllocTest_SOURCES = ZmAllocTest.cpp
