METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-DZT_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZtArray.hpp ZtBitWindow.hpp ZtDate.hpp \
	ZtEnum.hpp ZtICmp.hpp ZtIconv.hpp ZtLib.hpp ZtPlatform.hpp \
	ZtRegex.hpp ZtString.hpp
lib_LTLIBRARIES = libZt.la
libZt_la_SOURCES = \
	ZtDate.cpp ZtLib.cpp ZtRegex.cpp ZtString.cpp
libZt_la_LIBADD = $(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@Z_ZT_LIBS@ @Z_MT_LIBS@
