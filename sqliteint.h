#pragma once

/* SQLite internal definitions */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sqlite3.h>

typedef uint32_t u32;
typedef uint64_t u64;
typedef int64_t i64;
typedef uint8_t u8;

u8 sqlite3GetVarint(const unsigned char *p, u64 *v);
u8 sqlite3GetVarint32(const unsigned char *p, u32 *v);
sqlite3_int64 sessionGetI64(u8 *aRec);

/*
** The common case is for a varint to be a single byte.  They following
** macros handle the common case without a procedure call, but then call
** the procedure for larger varints.
*/
#define getVarint32(A,B)  \
	(u8)((*(A)<(u8)0x80)?((B)=(u32)*(A)),1:sqlite3GetVarint32((A),(u32 *)&(B)))

#ifdef __cplusplus
}
#endif