dnl @synopsis AX_PATH_LIB_CK [(A/NA)]
dnl
dnl check for ck and set CK_LIBS, CK_LDFLAGS, CK_CPPFLAGS
dnl
dnl also provide --with-ck option that may point to the $prefix of
dnl the ck installation - the macro will check $ck/include and
dnl $ck/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the LDFLAGS/CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_CK], [
  AC_MSG_CHECKING([ck])
  AC_ARG_WITH(ck, [AS_HELP_STRING([--with-ck=prefix],
				   [override location of ck library])],,
	      with_ck="yes")
  if test ".$with_ck" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    OLDCPPFLAGS="$CPPFLAGS"
    OLDLDFLAGS="$LDFLAGS"
    OLDLIBS="$LIBS"
    if test ".$with_ck" != "." -a ".$with_ck" != ".yes"; then
      CPPFLAGS="-I$with_ck/include $CPPFLAGS"
      CK_LDFLAGS_="-L$with_ck/lib -L$with_ck/lib64"
      case "$build_os" in
	mingw-*) ;;
	*) CK_LDFLAGS_="$CK_LDFLAGS_ -Wl,-R,$with_ck/lib -Wl,-R,$with_ck/lib64" ;;
      esac
      LDFLAGS="$CK_LDFLAGS_ $LDFLAGS"
    fi
    AC_CHECK_LIB(ck, ck_array_init)
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    LIBS="$OLDLIBS"
    if test "$ac_cv_lib_ck_ck_array_init" = "yes"; then
      CK_LIBS="-lck"
      CK_LDFLAGS=""
      CK_CPPFLAGS=""
      if test ".$with_ck" != "." -a ".$with_ck" != ".yes"; then
	AC_MSG_NOTICE([CK_LDFLAGS=$CK_LDFLAGS_])
	CK_LDFLAGS="$CK_LDFLAGS_"
	if test -d "$with_ck/include"; then
	  AC_MSG_NOTICE([CK_CPPFLAGS=-I$with_ck/include])
	  CK_CPPFLAGS="-I$with_ck/include"
	fi
      fi
      AC_MSG_CHECKING([lib ck])
      AC_MSG_RESULT([$CK_LIBS])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib ck])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
  fi
])
