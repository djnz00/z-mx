m4_define([MXMD_VERSION_], m4_esyscmd([grep MXMD_VERNAME version.h | sed 's/[^0-9\.]//g;1q' | tr -d '\n']))
AC_INIT(mxmd, MXMD_VERSION_)
MXMD_VERSION=MXMD_VERSION_

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
		MXMD_LDFLAGS="-rdynamic"
		MXMD_SO_LDFLAGS="-release $MXMD_VERSION"
		MXMD_XLIBS="-lhwloc -ldl -lpthread -lrt"
		;;
	mingw*)
		AC_MSG_RESULT([mingw])
		MXMD_LDFLAGS="-Wl,--enable-auto-import"
		MXMD_SO_LDFLAGS="-no-undefined"
		MXMD_XLIBS="-lws2_32 -lwsock32 -liconv"
		;;
	*)
		AC_MSG_RESULT([default])
		MXMD_LDFLAGS=""
		MXMD_SO_LDFLAGS=""
		MXMD_XLIBS=""
		;;
esac

AX_PATH_LIB_Z(8, 21, [-lZdb -lZv -lZi -lZe -lZt -lZm -lZu],,
	      [AC_MSG_ERROR([ZLib >= 8.21 required])])

AC_SUBST(Z_CPPFLAGS)
AC_SUBST(Z_CXXFLAGS)
AC_SUBST(Z_LDFLAGS)
AC_SUBST(Z_LIBS)

AX_PATH_LIB_MXBASE(4, 9, [-lMxBase],, [AC_MSG_ERROR([MxBase >= 4.9 required])])

AC_SUBST(MXBASE_CPPFLAGS)
AC_SUBST(MXBASE_LDFLAGS)
AC_SUBST(MXBASE_LIBS)

MXMD_CPPFLAGS=""

AC_SUBST(MXMD_CPPFLAGS)
AC_SUBST(MXMD_LDFLAGS)
AC_SUBST(MXMD_SO_LDFLAGS)
AC_SUBST(MXMD_XLIBS)

for i in src
do ( cd $i; test ! -e AC_PACKAGE_NAME && ln -s . AC_PACKAGE_NAME )
done

AC_OUTPUT(Makefile src/Makefile test/Makefile)
