METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src \
	-DZM_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZmAssert.hpp ZmAtomic.hpp \
	ZmBackTrace.hpp ZmBackTrace_.hpp ZmBackTrace_print.hpp \
	ZmBackTracer.hpp ZmBackoff.hpp ZmBitmap.hpp ZmBxRing.hpp ZmCleanup.hpp \
	ZmCondition.hpp ZmDRing.hpp ZmFn.hpp ZmFn_.hpp ZmFn_Lambda.hpp \
	ZmGlobal.hpp ZmGuard.hpp ZmHash.hpp ZmHashMgr.hpp ZmHeap.hpp \
	ZmINode.hpp ZmKVNode.hpp ZmLHash.hpp ZmLib.hpp ZmList.hpp ZmLock.hpp \
	ZmLockTraits.hpp ZmNoLock.hpp ZmObject_.hpp ZmObject.hpp \
	ZmPolymorph.hpp ZmPLock.hpp ZmPQueue.hpp ZmPlatform.hpp ZmQueue.hpp \
	ZmRBTree.hpp ZmRWLock.hpp ZmRandom.hpp ZmRef.hpp ZmRing.hpp \
	ZmScheduler.hpp ZmSemaphore.hpp ZmShard.hpp ZmSingleton.hpp \
	ZmSpecific.hpp ZmSpinLock.hpp ZmStack.hpp ZmStack_.hpp ZmStream.hpp \
	ZmTLock.hpp ZmThread.hpp ZmTime.hpp ZmTimeInterval.hpp ZmTimeout.hpp \
	ZmTopology.hpp ZmTrap.hpp ZmULock.hpp
lib_LTLIBRARIES = libZm.la
libZm_la_SOURCES = \
	ZmAssert.cpp ZmBackTrace.cpp ZmGlobal.cpp ZmHashMgr.cpp ZmHeap.cpp \
	ZmLib.cpp ZmLock.cpp ZmObject_.cpp ZmPlatform.cpp ZmRandom.cpp \
	ZmRing.cpp ZmScheduler.cpp ZmSingleton.cpp ZmSpecific.cpp \
	ZmTime.cpp ZmThread.cpp ZmTrap.cpp
libZm_la_LIBADD = $(top_builddir)/zu/src/libZu.la @Z_MT_LIBS@
if MINGW
libZm_la_LIBADD += -lbfd
endif
