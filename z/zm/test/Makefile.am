METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	@Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@
LDADD = $(top_builddir)/zm/src/libZm.la $(top_builddir)/zu/src/libZu.la \
	@Z_MT_LIBS@
noinst_PROGRAMS = \
	ZmBTTest ZmFnTest ZmHeapTest ZmQueueTest ZmRBTest ZmRWTest \
	ZmSchedTest ZmStackTest ZmTest ZmHashTest ZmHashTest2 ZmTLockTest \
	ZmTTest ZmLHTest ZmHashCleanup ZmHashThread ZmPQueueTest \
	ZmPQueueTest2 ZmPQueueTest3 ZmRingTest ZmBxRingTest ZmLockTest \
	ZmTLSTest
ZmBTTest_SOURCES = ZmBTTest.cpp
ZmFnTest_SOURCES = ZmFnTest.cpp
ZmHeapTest_SOURCES = ZmHeapTest.cpp
ZmQueueTest_SOURCES = ZmQueueTest.cpp
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
ZmBxRingTest_SOURCES = ZmBxRingTest.cpp
ZmLockTest_SOURCES = ZmLockTest.cpp
ZmTLSTest_SOURCES = ZmTLSTest.cpp
