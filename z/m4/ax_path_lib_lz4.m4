dnl @synopsis AX_PATH_LIB_LZ4 [(A/NA)]
dnl
dnl check for lz4 and set LZ4_LIBS, LZ4_LDFLAGS, LZ4_CPPFLAGS
dnl
dnl also provide --with-lz4 option that may point to the $prefix of
dnl the lz4 installation - the macro will check $lz4/include and
dnl $lz4/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the LDFLAGS/CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_LZ4], [
  AC_MSG_CHECKING([lz4])
  AC_ARG_WITH(lz4, [AS_HELP_STRING([--with-lz4=prefix],
    [override location of lz4 library])],, with_lz4="yes")
  if test ".$with_lz4" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    OLDCPPFLAGS="$CPPFLAGS"
    OLDLDFLAGS="$LDFLAGS"
    OLDLIBS="$LIBS"
    if test ".$with_lz4" != "." -a ".$with_lz4" != ".yes"; then
      CPPFLAGS="-I$with_lz4/include $CPPFLAGS"
      LZ4_LDFLAGS_="-L$with_lz4/lib -L$with_lz4/lib64"
      case "$build_os" in
	mingw-*) ;;
	*) LZ4_LDFLAGS_="$LZ4_LDFLAGS_ -Wl,-R,$with_lz4/lib -Wl,-R,$with_lz4/lib64" ;;
      esac
      LDFLAGS="$LZ4_LDFLAGS_ $LDFLAGS"
    fi
    LIBS="-llz4"
    AC_CACHE_CHECK([whether lz4 library is installed],
      ax_cv_lib_lz4_ok, [AC_RUN_IFELSE([AC_LANG_SOURCE([
#include <stdio.h>
#include <lz4.h>
int main() {
  return LZ4_versionNumber() < 10000;
}])],
      [ax_cv_lib_lz4_ok=yes],
      [ax_cv_lib_lz4_ok=no],
      [ax_cv_lib_lz4_ok=no])])
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    LIBS="$OLDLIBS"
    if test "$ax_cv_lib_lz4_ok" = "yes"; then
      LZ4_LIBS="-llz4"
      LZ4_LDFLAGS=""
      LZ4_CPPFLAGS=""
      if test ".$with_lz4" != "." -a ".$with_lz4" != ".yes"; then
	AC_MSG_NOTICE([LZ4_LDFLAGS=$LZ4_LDFLAGS_])
	LZ4_LDFLAGS="$LZ4_LDFLAGS_"
	if test -d "$with_lz4/include"; then
	  AC_MSG_NOTICE([LZ4_CPPFLAGS=-I$with_lz4/include])
	  LZ4_CPPFLAGS="-I$with_lz4/include"
	fi
      fi
      AC_MSG_CHECKING([lib lz4])
      AC_MSG_RESULT([$LZ4_LIBS])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib lz4])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
  fi
])
