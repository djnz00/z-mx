// MxMD Version

#ifndef VERSION_H
#define VERSION_H

#define MXMD_VULONG(major, minor, build) \
	((((unsigned long)(major))<<24) | \
	 (((unsigned long)(minor))<<16) | \
	 ((unsigned long)(build)))

#define MXMD_VERSION MXMD_VULONG(8,14,0) // <-- manually maintained
#define MXMD_VERNAME "8.14.0"		// <-- ''
#define MXMD_VERMSRC 8,14,0,0		// <-- ''

#define MXMD_VMAJOR(n) (((unsigned long)n)>>24)
#define MXMD_VMINOR(n) ((((unsigned long)n)>>16) & 0xff)
#define MXMD_VPATCH(n) (((unsigned long)n) & 0xffff)

#endif /* VERSION_H */
