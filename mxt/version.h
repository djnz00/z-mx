// MxT Version

#ifndef VERSION_H
#define VERSION_H

#define MXT_VULONG(major, minor, build) \
	((((unsigned long)(major))<<24) | \
	 (((unsigned long)(minor))<<16) | \
	 ((unsigned long)(build)))

#define MXT_VERSION MXT_VULONG(2,6,0)	// <-- manually maintained
#define MXT_VERNAME "2.6.0"		// <-- ''
#define MXT_VERMSRC 2,6,0,0		// <-- ''

#define MXT_VMAJOR(n) (((unsigned long)n)>>24)
#define MXT_VMINOR(n) ((((unsigned long)n)>>16) & 0xff)
#define MXT_VPATCH(n) (((unsigned long)n) & 0xffff)

#endif /* VERSION_H */
