// MxBase Version

#ifndef VERSION_H
#define VERSION_H

#define MXBASE_VULONG(major, minor, build) \
	(((major)<<24) | ((minor)<<16) | (build))

#define MXBASE_VERSION MXBASE_VULONG(4,9,0)	// <-- manually maintained
#define MXBASE_VERNAME "4.9.0"		// <-- ''
#define MXBASE_VERMSRC 4,9,0,0		// <-- ''

#define MXBASE_VMAJOR(n) (((unsigned long)n)>>24)
#define MXBASE_VMINOR(n) ((((unsigned long)n)>>16) & 0xff)
#define MXBASE_VPATCH(n) (((unsigned long)n) & 0xffff)

#endif /* VERSION_H */
