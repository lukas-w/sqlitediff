#pragma once

#include "sqlite3.h"

int applyChangeset(sqlite3* db, const char* filename);