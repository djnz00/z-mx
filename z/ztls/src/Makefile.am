METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/zu/src -I$(top_srcdir)/zm/src \
	-I$(top_srcdir)/zt/src -I$(top_srcdir)/ze/src -I${top_srcdir}/zi/src \
	-DZTLS_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = ZtlsLib.hpp Ztls.hpp \
	ZtlsBase32.hpp ZtlsBase64.hpp ZtlsHMAC.hpp ZtlsHex.hpp \
	ZtlsTOTP.hpp ZtlsRandom.hpp
lib_LTLIBRARIES = libZtls.la
libZtls_la_SOURCES = ZtlsLib.cpp ZtlsTOTP.cpp
libZtls_la_LIBADD = $(top_builddir)/zi/src/libZi.la \
	$(top_builddir)/ze/src/libZe.la \
	$(top_builddir)/zt/src/libZt.la \
	$(top_builddir)/zm/src/libZm.la \
	$(top_builddir)/zu/src/libZu.la \
	@MBEDTLS_LIBS@ @Z_IO_LIBS@ @Z_ZT_LIBS@ @Z_MT_LIBS@
bin_PROGRAMS = ztotp
ztotp_AM_CPPFLAGS = @Z_CPPFLAGS@
ztotp_LDADD = libZtls.la $(libZtls_la_LIBADD)
ztotp_SOURCES = ztotp.cpp
