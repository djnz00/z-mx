METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-DZT_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZtArray.hh ZtBitWindow.hh \
	ZtEnum.hh ZtIconv.hh ZtLib.hh ZtPlatform.hh \
	ZtRegex.hh ZtString.hh ZtHexDump.hh \
	ZtWindow.hh ZtBitmap.hh \
	ZtField.hh ZtScanBool.hh ZtJoin.hh ZtCase.hh ZtTimeZone.hh \
	ZtQuote.hh
lib_LTLIBRARIES = libZt.la
libZt_la_SOURCES = \
	ZtLib.cc ZtRegex.cc ZtString.cc ZtHexDump.cc ZtTimeZone.cc \
	ZtField.cc
libZt_la_LIBADD = $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@Z_ZT_LIBS@ @Z_MT_LIBS@
