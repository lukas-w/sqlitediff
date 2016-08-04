#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include "sqlite3.h"

struct sqlite_value
{
	int16_t type;
	union {
		int64_t iVal;
		double dVal;
	} data1;
	char* data2; //< Used for BLOB and TEXT
};

struct TableInfo {
  const char* tableName;
  uint8_t nCol;
  int* PKs;
  const char** columnNames;
};

struct Instruction {
  struct TableInfo* table;
  uint8_t iType;
  struct sqlite_value* values;
};

typedef int (*InstrCallback)(const struct Instruction* instr, void* context);
typedef int (*TableCallback)(const struct TableInfo* table, void* context);

int sqlitediff_write_table(const struct TableInfo* table, void* context);
int sqlitediff_write_instruction(const struct Instruction* instr, void* context);

int slitediff_diff_prepared_callback(
  sqlite3 *db,
  const char* zTab,
  TableCallback table_callback,
  InstrCallback instr_callback,
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

static void putsVarint(FILE *out, sqlite3_uint64 v);

#ifdef __cplusplus
} // end extern "C"
#endif
