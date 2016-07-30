#pragma once

#include "diff.h"
#include "sqlite3.h"

int applyInstruction(sqlite3* db, struct Instruction* instr);

int readChangeset(
		const char* buf,
		size_t size,
		InstrCallback instr_callback,
		void* context);
int readChangeset(
		const char* filename,
		InstrCallback instr_callback,
		void* context);
int applyChangeset(sqlite3* db, const char* filename);
