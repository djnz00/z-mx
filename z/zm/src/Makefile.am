METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src \
	-DZM_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZmAssert.hpp ZmAtomic.hpp \
	ZmAlloc.hpp ZmAllocator.hpp ZmHeap.hpp ZmVHeap.hpp \
	ZmBackTrace.hpp ZmBackTrace_.hpp ZmBackTrace_print.hpp \
	ZmBackTracer.hpp ZmBackoff.hpp ZmBitmap.hpp ZmBlock.hpp \
	ZmCleanup.hpp ZmCondition.hpp ZmXRing.hpp \
	ZmFn.hpp ZmFn_.hpp ZmGlobal.hpp ZmGuard.hpp \
	ZmHash.hpp ZmHashMgr.hpp ZmNode.hpp ZmNodeFn.hpp ZmLHash.hpp \
	ZmLib.hpp ZmList.hpp ZmLock.hpp ZmLockTraits.hpp ZmNoLock.hpp \
	ZmObject.hpp ZmObjectDebug.hpp ZmPolymorph.hpp ZmPLock.hpp \
	ZmPQueue.hpp ZmPlatform.hpp ZmRBTree.hpp ZmRWLock.hpp ZmRandom.hpp \
	ZmRef.hpp ZmRing.hpp ZmRingFn.hpp \
	ZmScheduler.hpp ZmSemaphore.hpp ZmShard.hpp \
	ZmSingleton.hpp ZmSpecific.hpp ZmSpinLock.hpp ZmStack.hpp \
	ZmStream.hpp ZmTLock.hpp ZmThread.hpp ZmTime.hpp \
	ZmTimeInterval.hpp ZmTimeout.hpp ZmTopology.hpp ZmTrap.hpp \
	ZmEngine.hpp ZmCache.hpp ZmDemangle.hpp ZmCallBack.hpp
lib_LTLIBRARIES = libZm.la
libZm_la_SOURCES = \
	ZmAssert.cpp ZmBackTrace.cpp ZmGlobal.cpp ZmHashMgr.cpp ZmHeap.cpp \
	ZmLib.cpp ZmLock.cpp ZmObjectDebug.cpp ZmPlatform.cpp ZmRandom.cpp \
	ZmRing.cpp ZmScheduler.cpp \
	ZmSingleton.cpp ZmSpecific.cpp \
	ZmTime.cpp ZmThread.cpp ZmTrap.cpp
libZm_la_LIBADD = $(top_builddir)/zu/src/libZu.la @Z_MT_LIBS@
if MINGW
libZm_la_LIBADD += -L/mingw64/lib/binutils -lbfd
else
libZm_la_LIBADD += -L/usr/local/lib -lbfd
endif
