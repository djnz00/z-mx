METASOURCES = AUTO
AM_CPPFLAGS = -DZU_EXPORTS @Z_CPPFLAGS@
AM_CXXFLAGS = @Z_CXXFLAGS@
AM_LDFLAGS = @Z_LDFLAGS@ @Z_SO_LDFLAGS@
pkginclude_HEADERS = \
	ZuArrayFn.hh ZuSpan.hh ZuArray.hh ZuAssert.hh ZuBox.hh \
	ZuBitmap.hh ZuBitfield.hh ZuByteSwap.hh ZuCmp.hh ZuICmp.hh \
	ZuInspect.hh ZuFnName.hh ZuFmt.hh ZuFP.hh ZuHash.hh ZuID.hh \
	ZuInt.hh ZuLargest.hh ZuDemangle.hh ZuTime.hh ZuDateTime.hh \
	ZuLib.hh ZuMvArray.hh ZuLambdaTraits.hh \
	ZuObjectTraits.hh ZuObject.hh ZuPolymorph.hh ZuPP.hh \
	ZuPrint.hh ZuRef.hh ZuStdString.hh ZuCSpan.hh \
	ZuStringFn.hh ZuCArray.hh ZuTraits.hh ZuTuple.hh ZuUnion.hh \
	Zu_aton.hh Zu_ntoa.hh ZuDecimalFn.hh ZuUTF.hh \
	ZuDecimal.hh ZuSwitch.hh ZuSort.hh ZuPtr.hh \
	ZuNormChar.hh ZuEquiv.hh ZuField.hh ZuIOResult.hh ZuUnroll.hh \
	ZuBitStream.hh ZuStream.hh ZuBytes.hh ZuMostAligned.hh \
	ZuVStream.hh ZuVArray.hh ZuIterator.hh ZuJoin.hh \
	zu_lib.h zu_decimal.h zu_time.h zu_bitmap.h
lib_LTLIBRARIES = libZu.la
libZu_la_SOURCES = \
	ZuLib.cc ZuUTF.cc ZuTime.cc ZuDateTime.cc \
	zu_decimal.cc zu_time.cc zu_bitmap.cc
