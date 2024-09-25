// ZLib version

// Zlib uses semantic versioning 2.0, see https://semver.org
// - major.minor.patch

#ifndef VERSION_H
#define VERSION_H

#include <stdint.h>

#define Z_VULONG(major, minor, patch) (((major)<<24) | ((minor)<<16) | (patch))

#define Z_VERSION Z_VULONG(9,0,0)	// <-- manually maintained
#define Z_VERNAME "9.0.0"		// <-- ''
#define Z_VERMSRC 9,0,0,0		// <-- ''

#define Z_VMAJOR(n) (uint64_t(n)>>24)
#define Z_VMINOR(n) ((uint64_t(n)>>16) & 0xff)
#define Z_VPATCH(n) (uint64_t(n) & 0xffff)

#endif /* VERSION_H */
