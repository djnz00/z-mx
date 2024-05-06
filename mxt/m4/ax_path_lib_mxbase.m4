AC_DEFUN([AX_PATH_LIB_MXBASE], [dnl
AC_MSG_CHECKING([MxBase])
AC_ARG_WITH(mxbase,
  [AS_HELP_STRING([--with-mxbase=prefix],
  [specify location of MxBase library])],, with_mxbase="yes")
if test ".$with_mxbase" = ".no" ; then
  AC_MSG_RESULT([disabled])
  m4_ifval($5,$5)
else
  AC_MSG_RESULT([(testing)])
  OLDCPPFLAGS="$CPPFLAGS"
  OLDLDFLAGS="$LDFLAGS"
  OLDLIBS="$LIBS"
  MXBASE_INCDIRS=""
  MXBASE_LIBDIRS=""
  if test ".$with_mxbase" != "." -a ".$with_mxbase" != ".yes"; then
    MXBASE_INCDIRS="-I$with_mxbase/include"
    MXBASE_LIBDIRS="-L$with_mxbase/lib -Wl,-R,$with_mxbase/lib"
  fi
  CPPFLAGS="-D_REENTRANT -D_GNU_SOURCE $MXBASE_INCDIRS $CPPFLAGS"
  LDFLAGS="$MXBASE_LIBDIRS $LDFLAGS"
  LIBS="-lMxBase -lZt -lZm -lZu $LIBS"
  AC_RUN_IFELSE([
    AC_LANG_SOURCE([[
#include <stdlib.h>
#include <stdio.h>
#include <mxbase/MxBaseVersion.hpp>
int main(int argc, char **argv)
{
  if (argc <= 1) return 0;
  if (argc != 3) return 1;
  int major = atoi(argv[1]);
  int minor = atoi(argv[2]);
  puts(MxBaseVersion() >= MxBaseVULong(major, minor, 0) ? "yes" : "no");
  return 0;
}
    ]])
  ], [
    ax_path_lib_mxbase_version_ok=`./conftest$ac_exeext $1 $2`
  ],[],[])
  CPPFLAGS="$OLDCPPFLAGS"
  LDFLAGS="$OLDLDFLAGS"
  LIBS="$OLDLIBS"
  if test ".$ax_path_lib_mxbase_version_ok" = ".yes"; then
    MXBASE_CPPFLAGS="$MXBASE_INCDIRS"
    MXBASE_LDFLAGS="$MXBASE_LIBDIRS"
    MXBASE_LIBS="$3"
    AC_MSG_NOTICE([MXBASE_CPPFLAGS=$MXBASE_CPPFLAGS])
    AC_MSG_NOTICE([MXBASE_LDFLAGS=$MXBASE_LDFLAGS])
    AC_MSG_NOTICE([MXBASE_LIBS=$MXBASE_LIBS])
    AC_MSG_CHECKING([MxBase])
    AC_MSG_RESULT([yes])
    m4_ifval($4,$4)
  else
    AC_MSG_CHECKING([MxBase])
    AC_MSG_RESULT([no])
    m4_ifval($5,$5)
  fi
fi
])
