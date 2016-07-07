#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
struct sqlite3;

/* Database B must be attached as 'aux' */
int sqlitediff_diff_prepared(
  sqlite3 *db,
  const char* zTab, /* name of table to diff, or NULL for all tables */
  int primarykey,   /* whether to use primary key instead of rowid */
  FILE* out         /* Output stream */
);

int sqlitediff_diff(
  const char* zDb1,
  const char* zDb2,
  const char* zTab,
  int primarykey,
  FILE* out
);

int sqlitediff_diff_file(
  const char* zDb1,
  const char* zDb2,
  const char* zTab,
  int primarykey,
  const char* out
);

#ifdef __cplusplus
} // end extern "C"
#endif
