m4_define([Z_VERSION_], m4_esyscmd([grep Z_VERNAME version.h | sed 's/[^0-9\.]//g;1q' | tr -d '\n']))
AC_INIT([zlib],[Z_VERSION_])
Z_VERSION=Z_VERSION_

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS(config.h)

AM_INIT_AUTOMAKE

AC_LANG([C++])
AC_PROG_CXX
LT_INIT([win32-dll shared disable-static])
LT_LANG([C++])

AX_PATH_LIB_CK(, [AC_MSG_ERROR([libck required])])
CPPFLAGS="$CK_CPPFLAGS $CPPFLAGS"
LDFLAGS="$CK_LDFLAGS $LDFLAGS"

AX_PATH_LIB_HWLOC(, [AC_MSG_ERROR([libhwloc required])])
CPPFLAGS="$HWLOC_CPPFLAGS $CPPFLAGS"
LDFLAGS="$HWLOC_LDFLAGS $LDFLAGS"

AX_PATH_LIB_PCRE(, [AC_MSG_ERROR([libpcre required])])
CPPFLAGS="$PCRE_CPPFLAGS $CPPFLAGS"
LDFLAGS="$PCRE_LDFLAGS $LDFLAGS"

AC_MSG_CHECKING([system multithreading libraries])
case $build_os in
  linux-*) 
    Z_CXXFLAGS=""
    Z_LDFLAGS="-rdynamic"
    Z_SO_LDFLAGS="-release $Z_VERSION"
    Z_MT_LIBS="$CK_LIBS $HWLOC_LIBS -lpthread -lrt -latomic"
    Z_ZT_LIBS="$PCRE_LIBS"
    Z_IO_LIBS="-ldl"
    Z_TERM_LIBS="-lncurses"
    ;;
  mingw*)
    mingw=yes
    case $build_cpu in
      i686) mingw32=yes;;
      x86_64) mingw64=yes;;
      *) ;;
    esac
    Z_CXXFLAGS=""
    Z_LDFLAGS="-Wl,--enable-auto-import"
    Z_SO_LDFLAGS="-no-undefined"
    Z_MT_LIBS="$CK_LIBS $HWLOC_LIBS -latomic"
    Z_ZT_LIBS="$PCRE_LIBS -liconv"
    Z_IO_LIBS="-lws2_32 -lwsock32"
    Z_TERM_LIBS=""
    ;;
  *)
    Z_CXXFLAGS=""
    Z_LDFLAGS=""
    Z_SO_LDFLAGS=""
    Z_MT_LIBS="$CK_LIBS $HWLOC_LIBS -lpthread"
    Z_ZT_LIBS="$PCRE_LIBS"
    Z_IO_LIBS="-ldl"
    Z_TERM_LIBS="-lterminfo"
    ;;
esac
AC_MSG_RESULT([${Z_MT_LIBS}])

AX_C_VA_LIST_IS_ARRAY

AX_PATH_JNI([jni="yes"], [dnl
  AC_MSG_NOTICE([JNI unavailable])
  jni="no"
])

AX_PATH_LIB_LZ4(, [AC_MSG_ERROR([liblz4 required])])
CPPFLAGS="$LZ4_CPPFLAGS $CPPFLAGS"
LDFLAGS="$LZ4_LDFLAGS $LDFLAGS"

AX_PATH_LIB_MBEDTLS(, [AC_MSG_ERROR([libmbedtls required])])
CPPFLAGS="$MBEDTLS_CPPFLAGS $CPPFLAGS"
LDFLAGS="$MBEDTLS_LDFLAGS $LDFLAGS"

AX_PATH_LIB_FBS(, [AC_MSG_ERROR([libflatbuffers required])])
CPPFLAGS="$FBS_CPPFLAGS $CPPFLAGS"
LDFLAGS="$FBS_LDFLAGS $LDFLAGS"

AX_PATH_LIB_CPPCODEC(, [AC_MSG_ERROR([libcppcodec required])])
CPPFLAGS="$CPPCODEC_CPPFLAGS $CPPFLAGS"

if test ".$mingw" != ".yes"; then
  AC_ARG_ENABLE([netlink],
    [  --enable-netlink        enable netlink],
    [case "${enableval}" in
      yes) netlink=true ;;
      no) netlink=false ;;
      *) AC_MSG_ERROR([bad value ${enableval} for --enable-netlink]) ;;
    esac], [netlink=false])
fi
AM_CONDITIONAL([NETLINK], [test x$netlink = xtrue])

AC_CHECK_PROG(PERL, "perl", "yes", "no")
if test ".$PERL" = ".no"; then
  AC_MSG_ERROR([perl required])
fi

PKG_CHECK_MODULES([PQ], [libpq >= 16], [dnl
  libpq="yes"
], [dnl
  AC_MSG_NOTICE([libpq unavailable])
  libpq="no"
])
AC_SUBST(PQ_LIBS)

PKG_CHECK_MODULES([GTK], [gtk+-3.0 >= 3.24.0], [dnl
  gtk="yes"
], [dnl
  AC_MSG_NOTICE([Gtk unavailable])
  gtk="no"
])
GTK_CFLAGS=`echo $GTK_CFLAGS | perl -pe 's/-pthread\s+//g; s/-mms-bitfields//g; s|/@<:@^/@:>@+/\.\./|/|g; s/-I(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$I{$1}++ ? "" : "-I${1}${2}"/ge'`
GTK_LDFLAGS=`echo $GTK_LDFLAGS | perl -pe 's/-pthread\s+//g; s|/[^/]+/\.\./|/|g; s/-L(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$L{$1}++ ? "" : "-L${1}${2}"/ge; s/(-Wl,-R,|-R)(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$R{$2}++ ? "" : "${1}${2}${3}"/ge'`
GTK_LIBS=`echo $GTK_LIBS | perl -pe 's/-pthread\s+//g; s|/@<:@^/@:>@+/\.\./|/|g; s/-l(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$l{$1}++ ? "" : "-l${1}${2}"/ge'`
AC_SUBST(GTK_CFLAGS)
AC_SUBST(GTK_LDFLAGS)
AC_SUBST(GTK_LIBS)

Z_CPPFLAGS="-D_REENTRANT -D_GNU_SOURCE"

AM_CONDITIONAL([GXX], [test ".$GXX" = ".yes"])
AM_CONDITIONAL([MINGW], [test ".$mingw" = ".yes"])
AM_CONDITIONAL([JNI], [test ".$jni" = ".yes"])
AM_CONDITIONAL([LIBPQ], [test ".$libpq" = ".yes"])
AM_CONDITIONAL([GTK], [test ".$gtk" = ".yes"])

if test ".$GXX" = ".yes"; then
  Z_CXXFLAGS="$Z_CXXFLAGS -Wall -Wno-parentheses -Wno-invalid-offsetof -Wno-misleading-indentation -fstrict-aliasing"
fi

if test ".$MX_CLANG" = ".yes"; then
  Z_CXXFLAGS="$Z_CXXFLAGS -Wno-unused-local-typedef -Wno-empty-body -Wno-c++11-extensions"
fi

CPPFLAGS=`echo $CPPFLAGS | perl -pe 's/-I(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$I{$1}++ ? "" : "-I${1}${2}"/ge;'`
LDFLAGS=`echo $LDFLAGS | perl -pe 's|/[^/]+/\.\./|/|g; s/-L(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$L{$1}++ ? "" : "-L${1}${2}"/ge; s/(-Wl,-R,|-R)(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$R{$2}++ ? "" : "${1}${2}${3}"/ge'`

AC_SUBST(Z_CPPFLAGS)
AC_SUBST(Z_CXXFLAGS)
AC_SUBST(Z_LDFLAGS)
AC_SUBST(Z_SO_LDFLAGS)
AC_SUBST(Z_MT_LIBS)
AC_SUBST(Z_ZT_LIBS)
AC_SUBST(Z_IO_LIBS)
AC_SUBST(Z_TERM_LIBS)

AC_SUBST(JNI_CPPFLAGS)
AC_SUBST(LZ4_LIBS)
AC_SUBST(MBEDTLS_LIBS)
AC_SUBST(FBS_LIBS)

for i in zu zm zt ze zi ztls zfb zv zdb zdb_pq zdf zrl zcmd zproxy zgtk; do (
  cd $i/src
  if test ! -e AC_PACKAGE_NAME; then
    if test ".$mingw" != ".yes"; then
      ln -s . AC_PACKAGE_NAME
    else
      env MSYS2_ARG_CONV_EXCL="*" cmd /c mklink /d AC_PACKAGE_NAME .
    fi
  fi
); done

AC_CONFIG_FILES([Makefile \
	zu/Makefile zu/src/Makefile zu/test/Makefile \
	zm/Makefile zm/src/Makefile zm/test/Makefile \
	zt/Makefile zt/src/Makefile zt/test/Makefile \
	ze/Makefile ze/src/Makefile ze/test/Makefile \
	zi/Makefile zi/src/Makefile zi/test/Makefile \
	ztls/Makefile ztls/src/Makefile ztls/test/Makefile \
	zfb/Makefile zfb/src/Makefile zfb/test/Makefile \
	zv/Makefile zv/src/Makefile zv/test/Makefile \
	zdb/Makefile zdb/src/Makefile zdb/test/Makefile \
	zdb_pq/Makefile zdb_pq/src/Makefile zdb_pq/test/Makefile \
	zdf/Makefile zdf/src/Makefile zdf/test/Makefile \
	zrl/Makefile zrl/src/Makefile zrl/test/Makefile \
	zcmd/Makefile zcmd/src/Makefile \
	zproxy/Makefile zproxy/src/Makefile \
	zgtk/Makefile zgtk/src/Makefile zgtk/test/Makefile])
AC_OUTPUT
