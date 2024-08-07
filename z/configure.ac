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

AX_C_VA_LIST_IS_ARRAY

# ensure perl is available
AC_CHECK_PROG(PERL, "perl", "yes", "no")
AS_IF([test ".$PERL" = ".no"], [
  AC_MSG_ERROR([perl required])
])

# concurrency kit
PKG_CHECK_MODULES([CK], [ck >= 0.6], [], [dnl
  AC_MSG_ERROR([libck required])
])
AC_SUBST(CK_LIBS)
CPPFLAGS="$CK_CFLAGS $CPPFLAGS"

# hwloc
PKG_CHECK_MODULES([HWLOC], [hwloc >= 2], [], [dnl
  AC_MSG_ERROR([libhwloc required])
])
AC_SUBST(HWLOC_LIBS)
CPPFLAGS="$HWLOC_CFLAGS $CPPFLAGS"

# pcre
AX_PATH_LIB_PCRE(, [AC_MSG_ERROR([libpcre required])])
CPPFLAGS="$PCRE_CPPFLAGS $CPPFLAGS"
LDFLAGS="$PCRE_LDFLAGS $LDFLAGS"

# system MT libraries
AC_MSG_CHECKING([system multithreading libraries])
Z_CPPFLAGS="-D_REENTRANT -D_GNU_SOURCE"
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
AC_SUBST(Z_CPPFLAGS)
AC_SUBST(Z_CXXFLAGS)
AC_SUBST(Z_LDFLAGS)
AC_SUBST(Z_SO_LDFLAGS)
AC_SUBST(Z_MT_LIBS)
AC_SUBST(Z_ZT_LIBS)
AC_SUBST(Z_IO_LIBS)
AC_SUBST(Z_TERM_LIBS)
AC_MSG_RESULT([${Z_MT_LIBS}])

# MinGW, gcc handling
AM_CONDITIONAL([MINGW], [test ".$mingw" = ".yes"])
AM_CONDITIONAL([GXX], [test ".$GXX" = ".yes"])

AS_IF([test ".$GXX" = ".yes"], [
  Z_CXXFLAGS="$Z_CXXFLAGS -Wall -Wno-parentheses -Wno-invalid-offsetof -Wno-misleading-indentation -fstrict-aliasing"
])

# lz4
AX_PATH_LIB_LZ4(, [AC_MSG_ERROR([liblz4 required])])
AC_SUBST(LZ4_LIBS)
CPPFLAGS="$LZ4_CPPFLAGS $CPPFLAGS"
LDFLAGS="$LZ4_LDFLAGS $LDFLAGS"

# mbedtls
AX_PATH_LIB_MBEDTLS(, [AC_MSG_ERROR([libmbedtls required])])
AC_SUBST(MBEDTLS_LIBS)
CPPFLAGS="$MBEDTLS_CPPFLAGS $CPPFLAGS"
LDFLAGS="$MBEDTLS_LDFLAGS $LDFLAGS"

# flatbuffers
PKG_CHECK_MODULES([FBS], [flatbuffers >= 24], [], [dnl
  AC_MSG_ERROR([libflatbuffers required])
])
AC_SUBST(FBS_LIBS)
CPPFLAGS="$FBS_CFLAGS $CPPFLAGS"

# cppcodec (header only)
PKG_CHECK_MODULES([CPPCODEC], [cppcodec-1 >= 0.1], [], [dnl
  AC_MSG_ERROR([libcppcodec required])
])
CPPFLAGS="$CPPCODEC_CFLAGS $CPPFLAGS"

# netlink (non-MinGW only) (optional)
AS_IF([test ".$mingw" != ".yes"], [
  AC_ARG_WITH([netlink], AS_HELP_STRING([--with-netlink], [Build with NetLink]))
])
AM_CONDITIONAL([NETLINK], [test ".$with_netlink" = ".yes"])

# postgres libpq (optional)
AC_ARG_WITH([pq], AS_HELP_STRING([--with-pq], [Build with PostgreSQL]))
AS_IF([test ".$with_pq" = ".yes"], [
  PKG_CHECK_MODULES([PQ], [libpq >= 16], [], [dnl
    AC_MSG_ERROR([libpq unavailable])
  ])
  AC_SUBST(PQ_CFLAGS)
  AC_SUBST(PQ_LIBS)
])
AM_CONDITIONAL([LIBPQ], [test ".$with_pq" = ".yes"])

# Gtk (optional)
AC_ARG_WITH([gtk], AS_HELP_STRING([--with-gtk], [Build with Gtk]))
AS_IF([test ".$with_gtk" = ".yes"], [
  PKG_CHECK_MODULES([GTK], [gtk+-3.0 >= 3.24.0], [], [dnl
    AC_MSG_NOTICE([Gtk unavailable])
  ])
  # cleanup and de-duplicate Gtk CFLAGS, LIBS
  GTK_CFLAGS=`echo $GTK_CFLAGS | perl -pe 's/-pthread\s+//g; s/-mms-bitfields//g; s|/@<:@^/@:>@+/\.\./|/|g; s/-I(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$I{$1}++ ? "" : "-I${1}${2}"/ge'`
  GTK_LIBS=`echo $GTK_LIBS | perl -pe 's/-pthread\s+//g; s|/@<:@^/@:>@+/\.\./|/|g; s/-L(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$L{$1}++ ? "" : "-L${1}${2}"/ge; s/-l(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$l{$1}++ ? "" : "-l${1}${2}"/ge; s/(-Wl,-R,|-R)(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$R{$2}++ ? "" : "${1}${2}${3}"/ge;'`
  AC_SUBST(GTK_CFLAGS)
  AC_SUBST(GTK_LIBS)
])
AM_CONDITIONAL([GTK], [test ".$with_gtk" = ".yes"])

# cleanup and de-duplicate CPPFLAGS and LDFLAGS
CPPFLAGS=`echo $CPPFLAGS | perl -pe 's/-I(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$I{$1}++ ? "" : "-I${1}${2}"/ge;'`
LDFLAGS=`echo $LDFLAGS | perl -pe 's|/[^/]+/\.\./|/|g; s/-L(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$L{$1}++ ? "" : "-L${1}${2}"/ge; s/(-Wl,-R,|-R)(@<:@^ \n@:>@+)(@<:@ \n@:>@)/$R{$2}++ ? "" : "${1}${2}${3}"/ge'`

for i in zu zm zt ze zi ztls zfb zv zdb zdb_pq zum zdf zrl zcmd zproxy zgtk; do {
  for j in src ext; do {
    if test -d $i/$j -a ! -e $i/$j/AC_PACKAGE_NAME; then (
      cd $i/$j;
      if test ".$mingw" != ".yes"; then
	ln -s . AC_PACKAGE_NAME
      else
	env MSYS2_ARG_CONV_EXCL="*" cmd /c mklink /d AC_PACKAGE_NAME .
      fi
    ); fi
  }; done
}; done

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
	zum/Makefile zum/src/Makefile zum/test/Makefile \
	zdf/Makefile zdf/src/Makefile zdf/test/Makefile \
	zrl/Makefile zrl/src/Makefile zrl/test/Makefile \
	zcmd/Makefile zcmd/src/Makefile zcmd/test/Makefile \
	zproxy/Makefile zproxy/src/Makefile \
	zgtk/Makefile zgtk/src/Makefile zgtk/test/Makefile])
AC_OUTPUT
