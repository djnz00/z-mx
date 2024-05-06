// ZLib version

#ifndef VERSION_H
#define VERSION_H

#define Z_VULONG(major, minor, build) (((major)<<24) | ((minor)<<16) | (build))

#define Z_VERSION Z_VULONG(9,0,0)	// <-- manually maintained
#define Z_VERNAME "9.0.0"		// <-- ''
#define Z_VERMSRC 9,0,0,0		// <-- ''

#define Z_VMAJOR(n) (((unsigned long)n)>>24)
#define Z_VMINOR(n) ((((unsigned long)n)>>16) & 0xff)
#define Z_VPATCH(n) (((unsigned long)n) & 0xffff)

#endif /* VERSION_H */
