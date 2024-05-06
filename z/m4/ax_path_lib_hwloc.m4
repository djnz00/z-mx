dnl @synopsis AX_PATH_LIB_HWLOC [(A/NA)]
dnl
dnl check for hwloc and set HWLOC_LIBS, HWLOC_LDFLAGS, HWLOC_CPPFLAGS
dnl
dnl also provide --with-hwloc option that may point to the $prefix of
dnl the hwloc installation - the macro will check $hwloc/include and
dnl $hwloc/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the LDFLAGS/CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_HWLOC], [
  AC_MSG_CHECKING([hwloc])
  AC_ARG_WITH(hwloc, [AS_HELP_STRING([--with-hwloc=prefix],
				   [override location of hwloc library])],,
	      with_hwloc="yes")
  if test ".$with_hwloc" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    OLDCPPFLAGS="$CPPFLAGS"
    OLDLDFLAGS="$LDFLAGS"
    OLDLIBS="$LIBS"
    if test ".$with_hwloc" != "." -a ".$with_hwloc" != ".yes"; then
      CPPFLAGS="-I$with_hwloc/include $CPPFLAGS"
      HWLOC_LDFLAGS_="-L$with_hwloc/lib -L$with_hwloc/lib64"
      case "$build_os" in
	mingw-*) ;;
	*) HWLOC_LDFLAGS_="$HWLOC_LDFLAGS_ -Wl,-R,$with_hwloc/lib -Wl,-R,$with_hwloc/lib64" ;;
      esac
      LDFLAGS="$HWLOC_LDFLAGS_ $LDFLAGS"
    fi
    AC_CHECK_LIB(hwloc, hwloc_get_api_version)
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    LIBS="$OLDLIBS"
    if test "$ac_cv_lib_hwloc_hwloc_get_api_version" = "yes"; then
      HWLOC_LIBS="-lhwloc"
      HWLOC_LDFLAGS=""
      HWLOC_CPPFLAGS=""
      if test ".$with_hwloc" != "." -a ".$with_hwloc" != ".yes"; then
	AC_MSG_NOTICE([HWLOC_LDFLAGS=$HWLOC_LDFLAGS_])
	HWLOC_LDFLAGS="$HWLOC_LDFLAGS_"
	if test -d "$with_hwloc/include"; then
	  AC_MSG_NOTICE([HWLOC_CPPFLAGS=-I$with_hwloc/include])
	  HWLOC_CPPFLAGS="-I$with_hwloc/include"
	fi
      fi
      AC_MSG_CHECKING([lib hwloc])
      AC_MSG_RESULT([$HWLOC_LIBS])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib hwloc])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
  fi
])
