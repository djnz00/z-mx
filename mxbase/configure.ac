m4_define([MXBASE_VERSION_], m4_esyscmd([grep MXBASE_VERNAME version.h | sed 's/[^0-9\.]//g;1q' | tr -d '\n']))
AC_INIT(mxbase, MXBASE_VERSION_)
MXBASE_VERSION=MXBASE_VERSION_

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS(config.h)

AM_INIT_AUTOMAKE

AC_LANG_CPLUSPLUS
AC_PROG_CXX
AC_LIBTOOL_WIN32_DLL
AC_PROG_LIBTOOL

AC_MSG_CHECKING([which threading environment to use])
case $build_os in
	linux-*) 
		AC_MSG_RESULT([-lpthread -lrt])
		MXBASE_LDFLAGS="-rdynamic"
		MXBASE_SO_LDFLAGS="-release $MXBASE_VERSION"
		MXBASE_XLIBS="-lhwloc -ldl -lpthread -lrt"
		;;
	mingw*)
		AC_MSG_RESULT([mingw])
		MXBASE_LDFLAGS="-Wl,--enable-auto-import"
		MXBASE_SO_LDFLAGS="-no-undefined"
		MXBASE_XLIBS="-lws2_32 -lwsock32 -liconv"
		;;
	*)
		AC_MSG_RESULT([default])
		MXBASE_LDFLAGS=""
		MXBASE_SO_LDFLAGS=""
		MXBASE_XLIBS=""
		;;
esac

AX_PATH_LIB_Z(8, 21, [-lZv -lZi -lZe -lZt -lZm -lZu],,
	      [AC_MSG_ERROR([ZLib >= 8.21 required])])

AC_SUBST(Z_CPPFLAGS)
AC_SUBST(Z_CXXFLAGS)
AC_SUBST(Z_LDFLAGS)
AC_SUBST(Z_LIBS)

MXBASE_CPPFLAGS=""

AC_SUBST(MXBASE_CPPFLAGS)
AC_SUBST(MXBASE_LDFLAGS)
AC_SUBST(MXBASE_SO_LDFLAGS)
AC_SUBST(MXBASE_XLIBS)

for i in src
do ( cd $i; test ! -e AC_PACKAGE_NAME && ln -s . AC_PACKAGE_NAME )
done

AC_OUTPUT(Makefile src/Makefile test/Makefile)
