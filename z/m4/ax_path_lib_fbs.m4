dnl @synopsis AX_PATH_LIB_FBS [(A/NA)]
dnl
dnl check for flatbuffers and set
dnl FBS_LIBS, FBS_LDFLAGS, FBS_CPPFLAGS
dnl
dnl also provide --with-flatbuffers option that may point to the $prefix of
dnl the flatbuffers installation - the macro will check $flatbuffers/include
dnl and $flatbuffers/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the LDFLAGS/CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_FBS], [
  AC_MSG_CHECKING([flatbuffers])
  AC_ARG_WITH(flatbuffers, [AS_HELP_STRING([--with-flatbuffers=prefix],
    [override location of flatbuffers library])],,
    with_flatbuffers="yes")
  if test ".$with_flatbuffers" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    AC_LANG_PUSH([C++])
    OLDCPPFLAGS="$CPPFLAGS"
    OLDLDFLAGS="$LDFLAGS"
    OLDLIBS="$LIBS"
    if test ".$with_flatbuffers" != "." -a ".$with_flatbuffers" != ".yes"; then
      CPPFLAGS="-I$with_flatbuffers/include $CPPFLAGS"
      FBS_LDFLAGS_="-L$with_flatbuffers/lib -L$with_flatbuffers/lib64"
      case "$build_os" in
	mingw-*) ;;
	*) FBS_LDFLAGS_="$FBS_LDFLAGS_ -Wl,-R,$with_flatbuffers/lib -Wl,-R,$with_flatbuffers/lib64" ;;
      esac
      LDFLAGS="$FBS_LDFLAGS_ $LDFLAGS"
    fi
    LIBS="-lflatbuffers"
    AC_CACHE_CHECK([whether flatbuffers library is installed],
      ax_cv_lib_flatbuffers_ok, [AC_RUN_IFELSE([AC_LANG_SOURCE([
#include <stdio.h>
#include <flatbuffers/flatbuffers.h>
int main() {
  using namespace flatbuffers;
  FlatBufferBuilder builder;
  return 0;
}])],
      [ax_cv_lib_flatbuffers_ok=yes],
      [ax_cv_lib_flatbuffers_ok=no],
      [ax_cv_lib_flatbuffers_ok=no])])
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    LIBS="$OLDLIBS"
    if test ".$ax_cv_lib_flatbuffers_ok" = ".yes"; then
      FBS_LIBS="-lflatbuffers"
      FBS_LDFLAGS=""
      FBS_CPPFLAGS=""
      if test ".$with_flatbuffers" != "." -a ".$with_flatbuffers" != ".yes"; then
	AC_MSG_NOTICE([FBS_LDFLAGS=$FBS_LDFLAGS_])
	FBS_LDFLAGS="$FBS_LDFLAGS_"
	if test -d "$with_flatbuffers/include"; then
	  AC_MSG_NOTICE([FBS_CPPFLAGS=-I$with_flatbuffers/include])
	  FBS_CPPFLAGS="-I$with_flatbuffers/include"
	fi
      fi
      AC_MSG_CHECKING([lib flatbuffers])
      AC_MSG_RESULT([$FBS_LIBS])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib flatbuffers])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
    AC_LANG_POP([C++])
  fi
])
