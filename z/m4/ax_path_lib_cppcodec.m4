dnl @synopsis AX_PATH_LIB_CPPCODEC [(A/NA)]
dnl
dnl check for cppcodec and set
dnl CPPCODEC_CPPFLAGS
dnl
dnl also provide --with-cppcodec option that may point to the $prefix of
dnl the cppcodec installation - the macro will check $cppcodec/include
dnl and $cppcodec/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_CPPCODEC], [
  AC_MSG_CHECKING([cppcodec])
  AC_ARG_WITH(cppcodec, [AS_HELP_STRING([--with-cppcodec=prefix],
    [override location of cppcodec library])],,
    with_cppcodec="yes")
  if test ".$with_cppcodec" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    AC_LANG_PUSH([C++])
    OLDCPPFLAGS="$CPPFLAGS"
    if test ".$with_cppcodec" != "." -a ".$with_cppcodec" != ".yes"; then
      CPPFLAGS="-I$with_cppcodec/include $CPPFLAGS"
    fi
    AC_CACHE_CHECK([whether cppcodec library is installed],
      ax_cv_lib_cppcodec_ok, [AC_RUN_IFELSE([AC_LANG_SOURCE([
#include <cppcodec/base32_rfc4648.hpp>
#include <string.h>
#include <string>
int main()
{
  using base32 = cppcodec::base32_rfc4648;
  std::string s = base32::encode("Hello World!", 12);
  return strcmp(s.data(), "JBSWY3DPEBLW64TMMQQQ====");
}])],
      [ax_cv_lib_cppcodec_ok=yes],
      [ax_cv_lib_cppcodec_ok=no],
      [ax_cv_lib_cppcodec_ok=no])])
    CPPFLAGS="$OLDCPPFLAGS"
    if test ".$ax_cv_lib_cppcodec_ok" = ".yes"; then
      CPPCODEC_CPPFLAGS=""
      if test ".$with_cppcodec" != "." -a ".$with_cppcodec" != ".yes"; then
	if test -d "$with_cppcodec/include"; then
	  AC_MSG_NOTICE([CPPCODEC_CPPFLAGS=-I$with_cppcodec/include])
	  CPPCODEC_CPPFLAGS="-I$with_cppcodec/include"
	fi
      fi
      AC_MSG_CHECKING([lib cppcodec])
      AC_MSG_RESULT([yes])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib cppcodec])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
    AC_LANG_POP([C++])
  fi
])
