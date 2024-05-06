METASOURCES = AUTO
AM_CPPFLAGS = -DMXBASE_EXPORTS @MXBASE_CPPFLAGS@ @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @MXBASE_LDFLAGS@ @MXBASE_SO_LDFLAGS@ @Z_LDFLAGS@
pkginclude_HEADERS = \
	MxBase.hh MxBaseLib.hh MxBaseVersion.hh \
	MxCSV.hh MxMultiplex.hh MxScheduler.hh \
	MxValWindow.hh \
	MxTxDB.hh MxRxDB.hh
lib_LTLIBRARIES = libMxBase.la
libMxBase_la_SOURCES = MxBaseLib.cc MxBaseVersion.cc MxEngine.cc \
	MxTelemetry.cc
libMxBase_la_LIBADD = @Z_LIBS@ @MXBASE_XLIBS@
bin_PROGRAMS = mcap mcmerge mcreplay telcap
mcap_LDADD = libMxBase.la @Z_LIBS@ @MXBASE_XLIBS@
mcmerge_LDADD = libMxBase.la @Z_LIBS@ @MXBASE_XLIBS@
mcreplay_LDADD = libMxBase.la @Z_LIBS@ @MXBASE_XLIBS@
telcap_LDADD = libMxBase.la -lZdb @Z_LIBS@ @MXBASE_XLIBS@
mcap_SOURCES = mcap.cc
mcmerge_SOURCES = mcmerge.cc
mcreplay_SOURCES = mcreplay.cc
telcap_SOURCES = telcap.cc
