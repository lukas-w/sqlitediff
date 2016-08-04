#pragma once

#include "diff.h"
#include "sqlite3.h"

int applyInstruction(const Instruction* instr, sqlite3* db);

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
