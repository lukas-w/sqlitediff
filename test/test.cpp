#include <iostream>

#include <diff.h>
#include <patch.h>

#include <cstdio>
#include <cerrno>
#include <cstring>

#define F(X) do {															\
	rc = X; 																\
	if (rc != 0) {															\
		std::cerr << "Error: " << #X << " returned " << rc << std::endl;	\
		std::cerr << "SQL:" << sqlite3_errstr(rc) << std::endl;	\
		return rc;															\
	}																		\
} while(0)

#define T(X) F(!(X))


void trace_callback( void* udp, const char* sql ) { printf("{SQL} [%s]\n", sql); }

int main(int argc, char const *argv[])
{
	int rc;

	const char* aF = "a.sqlite";
	const char* bF = "b.sqlite";

	rc = remove(aF);
	if (rc) {
		T(errno == ENOENT);
	}
	rc = remove(bF);
	if (rc) {
		T(errno == ENOENT);
	}

	sqlite3 *db;
	F(sqlite3_open_v2(aF, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	F(sqlite3_exec(db, "ATTACH 'b.sqlite' AS 'aux'", nullptr, nullptr, nullptr));

	F(sqlite3_exec(db, 
		"CREATE TABLE main.Entries (ID PRIMARY KEY, Name, Farbe)",
		nullptr, nullptr, nullptr));
	F(sqlite3_exec(db, 
		"CREATE TABLE aux.Entries (ID PRIMARY KEY, Name, Farbe)",
		nullptr, nullptr, nullptr));

	F(sqlite3_exec(db, 
		"INSERT INTO main.Entries VALUES (0, 'Apfel', 'Grün')",
		nullptr, nullptr, nullptr));
	F(sqlite3_exec(db, 
		"INSERT INTO main.Entries VALUES (1, 'Banane', 'Gälb')",
		nullptr, nullptr, nullptr));


	F(sqlite3_exec(db,
		"INSERT INTO aux.Entries VALUES (0, 'Apfel', 'Grün')",
		nullptr, nullptr, nullptr));
	F(sqlite3_exec(db,
		"INSERT INTO aux.Entries VALUES (1, 'Banane', 'Gelb')",
		nullptr, nullptr, nullptr));
	F(sqlite3_exec(db,
		"INSERT INTO aux.Entries VALUES (2, 'Clementine', 'Orange')",
		nullptr, nullptr, nullptr));

	auto checkB = [&](std::string dbName) {
		sqlite3_stmt* stmt;
		F(sqlite3_prepare(db, ("SELECT ID, Name, Farbe FROM "+dbName+".Entries;").data(), -1, &stmt, nullptr));

		T(sqlite3_step(stmt) == SQLITE_ROW);
		T(sqlite3_column_int(stmt, 0) == 0);
		F(strcmp((const char*) sqlite3_column_text(stmt, 1), "Apfel"));
		F(strcmp((const char*) sqlite3_column_text(stmt, 2), "Grün"));

		T(sqlite3_step(stmt) == SQLITE_ROW);
		T(sqlite3_column_int(stmt, 0) == 1);
		F(strcmp((const char*) sqlite3_column_text(stmt, 1), "Banane"));
		F(strcmp((const char*) sqlite3_column_text(stmt, 2), "Gelb"));

		T(sqlite3_step(stmt) == SQLITE_ROW);
		T(sqlite3_column_int(stmt, 0) == 2);
		F(strcmp((const char*) sqlite3_column_text(stmt, 1), "Clementine"));
		F(strcmp((const char*) sqlite3_column_text(stmt, 2), "Orange"));

		F(sqlite3_finalize(stmt));
	};

	F(checkB("aux"));

	// Diff
	FILE* out = fopen("out.diff", "w");
	F(sqlitediff_diff_prepared(
		db,
		nullptr,
		false,   /* whether to use primary key instead of rowid */
		out      /* Output stream */
	));
	fclose(out);


	sqlite3_trace(db, trace_callback, NULL);

	F(applyChangeset(db, "out.diff"));

	F(checkB("main"));

	F(sqlite3_close(db));

	return 0;
}
