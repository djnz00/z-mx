METASOURCES = AUTO
AM_CPPFLAGS = -DMXMD_EXPORTS @MXMD_CPPFLAGS@ @MXBASE_CPPFLAGS@ @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @MXMD_LDFLAGS@ @MXMD_SO_LDFLAGS@ \
	@MXBASE_LDFLAGS@ @Z_LDFLAGS@
pkginclude_HEADERS = \
	MxMD.hh MxMDLib.hh MxMDVersion.hh MxMDTypes.hh \
	MxMDCore.hh MxMDCSV.hh MxMDStream.hh \
	MxMDChannel.hh \
	MxMDBroadcast.hh \
	MxMDRecord.hh MxMDReplay.hh \
	MxMDSubscriber.hh MxMDPublisher.hh \
	MxMDTelemetry.hh
lib_LTLIBRARIES = libMxMD.la
libMxMD_la_SOURCES = \
	MxMD.cc MxMDLib.cc MxMDVersion.cc MxMDCore.cc \
	MxMDBroadcast.cc \
	MxMDRecord.cc MxMDReplay.cc \
	MxMDPublisher.cc MxMDSubscriber.cc \
	MxMDTelemetry.cc
libMxMD_la_LIBADD = @MXBASE_LIBS@ @Z_LIBS@ @MXMD_XLIBS@
bin_PROGRAMS = recdump mdrecord
recdump_LDADD = libMxMD.la @MXBASE_LIBS@ @Z_LIBS@ @MXMD_XLIBS@
mdrecord_LDADD = libMxMD.la @MXBASE_LIBS@ @Z_LIBS@ @MXMD_XLIBS@
recdump_SOURCES = recdump.cc
mdrecord_SOURCES = mdrecord.cc
