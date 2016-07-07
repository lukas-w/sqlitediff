#include "diff.h"
#include "sqlite3.h"

#include <iostream>

using namespace std;

void trace_callback(void* udp, const char* sql) {
	std::cerr << "{SQL} " << sql << std::endl;
}

int main(int argc, char const *argv[])
{
	if (argc != 3) {
		cerr << "Wrong number of arguments" << endl;
		return 1;
	}

	const char* db1File = argv[1];
	const char* db2File = argv[2];

	int rc;
	sqlite3* db;
	rc = sqlite3_open_v2(db1File, &db, SQLITE_OPEN_READWRITE, nullptr);

	sqlite3_trace(db, trace_callback, NULL);

	if (rc != SQLITE_OK) {
		cerr << "Could not open sqlite DB " << db1File << endl;
		return 2;
	}

	//TODO: Attach
	//TODO: Call diff

	if (rc != SQLITE_OK) {
		cerr << "Could not create changeset." << endl;
		sqlite3_close_v2(db);
		return 2;	
	}

	sqlite3_close_v2(db);

	return 0;
}
