#!/bin/bash
BFDDIR="$MINGW_PREFIX"/lib/binutils
[ ! -d "$BFDDIR" ] && pacman -S mingw-w64-x86_64-binutils
cp mingw_libbfd.def "$BFDDIR"/libbfd.def
cd "$BFDDIR"
[ ! -f "${BFDDIR}/libbfd.dll" ] && {
  dlltool --export-all-symbols -e libbfd.o -l libbfd.dll.a -D libbfd.dll libbfd.a
  gcc -shared -o libbfd.dll libbfd.o libbfd.a -L . -liberty -lintl -lz
  rm libbfd.o
  echo "built/installed $BFDDIR/libbfd.dll"
  exit 0
}
mv libbfd.dll.a libbfd.dll.a.orig
dlltool -d libbfd.def -l libbfd.dll.a -D libbfd.dll
