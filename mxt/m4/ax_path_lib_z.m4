AC_DEFUN([AX_PATH_LIB_Z], [dnl
  AC_MSG_CHECKING([ZLib])
  AC_ARG_WITH(Z,
    [AS_HELP_STRING([--with-Z=prefix@<:@:prefix:...@:>@], [specify location(s) of Z library and dependent libraries])],,
    with_Z="yes")
  if test ".$with_Z" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($5,$5)
  else
    AC_MSG_RESULT([(testing)])
    OLDCPPFLAGS="$CPPFLAGS";
    OLDLDFLAGS="$LDFLAGS";
    OLDLIBS="$LIBS";
    Z_INCDIRS="";
    Z_LIBDIRS="";
    if test ".$with_Z" != "." -a ".$with_Z" != ".yes"; then
      for i in `echo $with_Z | sed -e 's/:/ /g'`; do
	Z_INCDIRS="$Z_INCDIRS -I$i/include"
	Z_LIBDIRS="$Z_LIBDIRS -L$i/lib -Wl,-R,$i/lib"
      done
    fi
    CPPFLAGS="$Z_INCDIRS $CPPFLAGS -D_REENTRANT -D_GNU_SOURCE"
    LDFLAGS="$Z_LIBDIRS $LDFLAGS"
    LIBS="-lZu $LIBS"
    AC_RUN_IFELSE([
      AC_LANG_SOURCE([[
#include <stdlib.h>
#include <stdio.h>
#include <zlib/ZuVersion.hpp>
int main(int argc, char **argv)
{
  if (argc <= 1) return 0;
  if (argc != 3) return 1;
  int major = atoi(argv[1]);
  int minor = atoi(argv[2]);
  puts(ZuVersion() >= ZuVULong(major, minor, 0) ? "yes" : "no");
  return 0;
}
      ]])
    ], [
      ax_path_lib_z_version_ok=`./conftest$ac_exeext $1 $2`
    ],[],[])
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    LIBS="$OLDLIBS"
    if test ".$ax_path_lib_z_version_ok" = ".yes"; then
      Z_CPPFLAGS="-D_REENTRANT -D_GNU_SOURCE $Z_INCDIRS"
      Z_LDFLAGS="$Z_LIBDIRS"
      Z_LIBS="$3"
      Z_CXXFLAGS=""
      if test ".$GXX" = ".yes"; then
	Z_CXXFLAGS="$Z_CXXFLAGS -Wall -Wno-parentheses -Wno-invalid-offsetof -Wno-misleading-indentation -fstrict-aliasing"
      fi
      AC_MSG_NOTICE([Z_CPPFLAGS=$Z_CPPFLAGS])
      AC_MSG_NOTICE([Z_CXXFLAGS=$Z_CXXFLAGS])
      AC_MSG_NOTICE([Z_LDFLAGS=$Z_LDFLAGS])
      AC_MSG_NOTICE([Z_LIBS=$Z_LIBS])
      AC_MSG_CHECKING([ZLib])
      AC_MSG_RESULT([yes])
      m4_ifval($4,$4)
    else
      AC_MSG_CHECKING([ZLib])
      AC_MSG_RESULT([no])
      m4_ifval($5,$5)
    fi
  fi
])
