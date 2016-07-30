#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include "sqlite3.h"

struct TableInfo {
  const char* tableName;
  uint8_t nCol;
  int* PKs;
};

struct Instruction {
  struct TableInfo* table;
  uint8_t iType;
  sqlite3_value** values;
};

typedef int (*InstrCallback)(const struct Instruction* instr, void* context);
typedef int (*TableCallback)(const struct TableInfo* table, void* context);

int slitediff_diff_prepared_callback(
  sqlite3 *db,
  const char* zTab,
  TableCallback* table_callback,
  InstrCallback* instr_callback,
  void* context
);

/* Database B must be attached as 'aux' */
int sqlitediff_diff_prepared(
  sqlite3 *db,
  const char* zTab, /* name of table to diff, or NULL for all tables */
  FILE* out         /* Output stream */
);

int sqlitediff_diff(
  const char* zDb1,
  const char* zDb2,
  const char* zTab,
  FILE* out
);

int sqlitediff_diff_file(
  const char* zDb1,
  const char* zDb2,
  const char* zTab,
  const char* out
);

#ifdef __cplusplus
} // end extern "C"
#endif
