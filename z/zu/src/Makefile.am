METASOURCES = AUTO
AM_CPPFLAGS = -DZU_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZuArrayFn.hpp ZuArray.hpp ZuArrayN.hpp ZuAssert.hpp ZuBox.hpp \
	ZuBitmap.hpp ZuByteSwap.hpp ZuCmp.hpp ZuConversion.hpp \
	ZuFnName.hpp ZuFmt.hpp ZuFP.hpp ZuHash.hpp ZuID.hpp \
	ZuInt.hpp ZuFunctorTraits.hpp ZuLargest.hpp \
	ZuLib.hpp ZuMixin.hpp ZuMvArray.hpp ZuNull.hpp \
	ZuObject_.hpp ZuObject.hpp ZuPolymorph.hpp ZuPOD.hpp ZuPP.hpp \
	ZuPair.hpp ZuPrint.hpp ZuRef.hpp ZuStdString.hpp ZuString.hpp \
	ZuStringFn.hpp ZuStringN.hpp ZuTraits.hpp ZuTuple.hpp ZuUnion.hpp \
	ZuVersion.hpp Zu_aton.hpp Zu_ntoa.hpp ZuDecimalFn.hpp ZuUTF.hpp \
	ZuDecimal.hpp ZuSwitch.hpp ZuSort.hpp ZuPtr.hpp ZuDemangle.hpp \
	ZuEquivChar.hpp ZuField.hpp ZuGrow.hpp ZuIOResult.hpp
lib_LTLIBRARIES = libZu.la
libZu_la_SOURCES = ZuLib.cpp ZuVersion.cpp ZuUTF.cpp
libZu_la_LIBADD = @Z_MT_LIBS@
