METASOURCES = AUTO
AM_CPPFLAGS = -I$(top_srcdir)/src \
	@MXMD_CPPFLAGS@ @MXBASE_CPPFLAGS@ @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @MXMD_LDFLAGS@ @MXBASE_LDFLAGS@ @Z_LDFLAGS@
LDADD = $(top_builddir)/src/libMxMD.la @MXBASE_LIBS@ @Z_LIBS@ @MXMD_XLIBS@
noinst_PROGRAMS = \
	mdsample_standalone mdsample_symlist mdsample_interactive \
	mdsample_publisher mdsample_subscriber
mdsample_standalone_SOURCES = mdsample_standalone.cc
mdsample_symlist_SOURCES = mdsample_symlist.cc
mdsample_interactive_SOURCES = mdsample_interactive.cc
mdsample_publisher_SOURCES = mdsample_publisher.cc
mdsample_subscriber_SOURCES = mdsample_subscriber.cc
