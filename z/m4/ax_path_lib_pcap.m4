dnl @synopsis AX_PATH_LIB_PCAP [(A/NA)]
dnl
dnl check for pcap and set PCAP_LIBS, PCAP_LDFLAGS, PCAP_CPPFLAGS
dnl
dnl also provide --with-ck option that may point to the $prefix of
dnl the ck installation - the macro will check $ck/include and
dnl $ck/lib to contain the necessary files.
dnl
dnl the usual two ACTION-IF-FOUND / ACTION-IF-NOT-FOUND are supported
dnl and they can take advantage of the LDFLAGS/CPPFLAGS additions.

AC_DEFUN([AX_PATH_LIB_PCAP], [
  AC_MSG_CHECKING([lib pcap])
  AC_ARG_WITH(pcap, [AS_HELP_STRING([--with-pcap=prefix],
				    [override location of pcap library])],,
	      with_pcap="yes")
  if test ".$with_pcap" = ".no" ; then
    AC_MSG_RESULT([disabled])
    m4_ifval($2,$2)
  else
    AC_MSG_RESULT([(testing)])
    OLDLIBS="$LIBS"
    OLDCPPFLAGS="$CPPFLAGS"
    OLDLDFLAGS="$LDFLAGS"
    if test ".$with_pcap" != "." -a ".$with_pcap" != ".yes"; then
      CPPFLAGS="-I$with_pcap/include $CPPFLAGS"
      PCAP_LDFLAGS_="-L$with_pcap/lib -L$with_pcap/lib64"
      case "$build_os" in
	mingw-*) ;;
	*) PCAP_LDFLAGS_="$PCAP_LDFLAGS_ -Wl,-R,$with_pcap/lib -Wl,-R,$with_pcap/lib64" ;;
      esac
      LDFLAGS="$PCAP_LDFLAGS_ $LDFLAGS"
    fi
    AC_CHECK_LIB(pcap, pcap_strerror)
    if test ".$ac_cv_lib_pcap_pcap_strerror" != ".yes"; then
      AC_CHECK_LIB(wpcap, pcap_strerror)
    fi
    LIBS="$OLDLIBS"
    CPPFLAGS="$OLDCPPFLAGS"
    LDFLAGS="$OLDLDFLAGS"
    PCAP_LIBS=""
    if test ".$ac_cv_lib_pcap_pcap_strerror" = ".yes"; then
      PCAP_LIBS="-lpcap"
    fi
    if test ".$ac_cv_lib_wpcap_pcap_strerror" = ".yes"; then
      PCAP_LIBS="-lwpcap"
    fi
    if test ".$PCAP_LIBS" != "."; then
      PCAP_LDFLAGS=""
      PCAP_CPPFLAGS=""
      if test ".$with_pcap" != "." -a ".$with_pcap" != ".yes"; then
	AC_MSG_NOTICE([PCAP_LDFLAGS=$PCAP_LDFLAGS_])
	PCAP_LDFLAGS="$PCAP_LDFLAGS_"
	if test -d "$with_pcap/include"; then
	  AC_MSG_NOTICE([PCAP_CPPFLAGS=-I$with_pcap/include])
	  PCAP_CPPFLAGS="-I$with_pcap/include"
	fi
      fi
      AC_MSG_CHECKING([lib pcap])
      AC_MSG_RESULT([$PCAP_LIBS])
      m4_ifval($1,$1)
    else
      AC_MSG_CHECKING([lib pcap])
      AC_MSG_RESULT([no])
      m4_ifval($2,$2)
    fi
  fi
])
