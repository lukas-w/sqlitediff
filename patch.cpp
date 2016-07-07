#include "diff.h"

#include "sqliteint.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>

#define CHANGESET_CORRUPT 5

/**

FORMAT of binary changeset (pseudo-grammar):

->: 
	<TableInstructions>[1+]

TableInstructions:
	'T'
	varint	=> nCols
	byte[nCols] (PK flag)
	<string>	name of table
	<Instruction>[1+]

string:
	varInt	length
	…		data

Instruction:
	<InstrInsert> | <InstrDelete> | <InstrUpdate>

InstrInsert:
	SQLITE_INSERT
	0x0
	<Value>[nCols]

InstrDelete:
	SQLITE_DELETE
	0x0
	<Value>[nCols]

InstrUpdate:
	SQLITE_DELETE
	0x0
	<ZeroOrValue>[nCols]		# Old Values (zero if not changed)
	<ZeroOrValue>[nCols]		# New Values (zero if not changed)

ZeroOrValue:
	0x0 | <Value>

Value:
	byte iDType data type, (if NULL that's it)
	…data

*/


struct sqlite_value
{
	u8 type;
	void* data;
};

size_t readValue(const char* buf, sqlite_value* val)
{
	val->type = buf[0];
	buf++;
	val->data = (void*)(buf);

	switch(val->type)
	{
	case SQLITE_INTEGER:
	case SQLITE_FLOAT:
		return 1 + 8;
	case SQLITE_TEXT:
	case SQLITE_BLOB: {
		u32 dataLen;
		u8 varIntLen = getVarint32((u8*)buf, dataLen);
		return 1 + dataLen + varIntLen;
	}
	case 0:
	case SQLITE_NULL:
		return 1;
	default:
		return 0;
	}
}

int64_t bindValue(sqlite3_stmt* stmt, const sqlite_value& val, int col)
{
	size_t size = 0; int rc;

	switch (val.type) {
	case SQLITE_INTEGER: {
		//int64_t iVal = ((int64_t*)(val.data))[0];
		int64_t iVal = sessionGetI64((u8*)val.data);
		size = 8;
		rc = sqlite3_bind_int64(stmt, col, iVal);
		break;
	}
	case SQLITE_FLOAT: {
		//double fVal = ((double*)val.data)[0];
		int64_t iVal = sessionGetI64((u8*)val.data);
		size = 8;
		rc = sqlite3_bind_double(stmt, col, *(double*)(&iVal));
		break;
	}
	case SQLITE_TEXT: {
		u32 textLen;
		u8 varIntLen = getVarint32((u8*)val.data, textLen);

		rc = sqlite3_bind_text(stmt, col, (char*)val.data + varIntLen, textLen, nullptr);

		size = textLen + varIntLen;
		break;
	}
	case SQLITE_BLOB: {
		u32 blobLen;
		u8 varIntLen = getVarint32((u8*)val.data, blobLen);

		rc = sqlite3_bind_text(stmt, col, (char*)val.data + varIntLen, blobLen, nullptr);

		size = blobLen + varIntLen;
		break;
	}
	case SQLITE_NULL: {
		rc = sqlite3_bind_null(stmt, col);
		break;
	}
	}

	if (rc != SQLITE_OK) {
		return -1;
	}

	return size;
}

size_t bindValue(sqlite3_stmt* stmt, const char* buf, int col)
{
	size_t nRead = 0; int rc;

	sqlite_value val;
	nRead = readValue(buf, &val);

	if (nRead == 0) {
		return 0;
	}

	buf = (char*) val.data;

	int64_t dataLen = bindValue(stmt, val, col);

	if (dataLen < 0) {
		return 0;
	} else {
		return nRead;
	}
}

size_t applyInsert(sqlite3* db, const char* tableName, u32 nCol, const char* buf)
{
	int rc; size_t nRead = 0;

	std::string sql = std::string() + "INSERT INTO " + tableName + " VALUES (";

	for (int i=0; i < nCol; i++) {
		sql += "?";
		if (i < nCol - 1) {
			sql += ", ";
		}
	}
	sql += ");";
	
	sqlite3_stmt* stmt;
	rc = sqlite3_prepare_v2(db, sql.data(), sql.size(), &stmt, nullptr);

	if (rc != SQLITE_OK) {
		return 0;
	}

	for (int i=1; i <= nCol; i++) {
		size_t read = bindValue(stmt, buf, i);
		nRead += read; buf += read;
		if (read == 0) {
			return 0;
		}
	}

	rc = sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		std::cerr << "Error applying insert: " << sqlite3_errmsg(db) << std::endl;
		return 0;
	}

	return nRead;
}

std::vector<std::string> getColumnNames(sqlite3* db, const char* tableName)
{
	std::vector<std::string> result; int rc;

	sqlite3_stmt* stmt;
	rc = sqlite3_prepare_v2(db, (std::string() + "pragma table_info ('" + tableName +"')").data(), -1, &stmt, nullptr);
	if (rc!=SQLITE_OK) {
		return result;
	}

	//rc = sqlite3_bind_text(stmt, 1, tableName, -1, nullptr);

	while(rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
	{
		result.push_back((const char*)sqlite3_column_text(stmt, 1));
	}

	result.shrink_to_fit();

	sqlite3_finalize(stmt);
	return result;
}

size_t applyDelete(sqlite3* db, const char* tableName, u32 nCol, const char* buf)
{
	int rc; size_t nRead = 0;

	auto columnNames = getColumnNames(db, tableName);
	std::string sql = std::string() + "DELETE FROM " + tableName + " WHERE";

	for (int i=0; i < nCol; i++) {
		if (i > 0) {
			sql += " AND";
		}
		sql = sql + " " + columnNames.at(i) + " = ?";
	}

	sqlite3_stmt* stmt;
	rc = sqlite3_prepare_v2(db, sql.data(), sql.size(), &stmt, nullptr);

	if (rc != SQLITE_OK) {
		return 0;
	}

	for (int i=0; i < nCol; i++) {
		size_t read = bindValue(stmt, buf, i+1);
		if (read == 0) {
			return 0;
		}
		nRead += read; buf += read;
	}

	rc = sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		return 0;
	}

	return nRead;
}

size_t applyUpdate(sqlite3* db, const char* tableName, u32 nCol, const char* buf)
{
	size_t nRead = 0;

	std::vector<sqlite_value> valsBefore(nCol);
	std::vector<sqlite_value> valsAfter(nCol);
	std::vector<std::string> columnNames = getColumnNames(db, tableName);

	for (int i=0; i < nCol; i++) {
		size_t valLen = readValue(buf, &valsBefore.at(i));
		if (valLen == 0) {
			return 0;
		}
		buf += valLen;
		nRead += valLen;
	}
	for (int i=0; i < nCol; i++) {
		size_t valLen = readValue(buf, &valsAfter.at(i));
		if (valLen == 0) {
			return 0;
		}
		buf += valLen;
		nRead += valLen;
	}

	std::string sql;
	sql = sql + "UPDATE " + tableName + " SET";

	// sets
	for (int n=0, i=0; i < nCol; i++) {
		const auto& name = columnNames.at(i);
		const auto& val = valsAfter.at(i);
		if (val.type) {
			if (n > 0) {
				sql += ", ";
			}
			sql = sql + " " + name + " = " + "?";
			n++;
		}
	}

	//wheres
	sql += " WHERE ";
	for (int n=0, i=0; i < nCol; i++) {
		const auto& name = columnNames.at(i);
		const auto& val = valsBefore.at(i);
		if (val.type) {
			if (n > 0) {
				sql += " AND";
			}
			sql = sql + " " + name + " = " + "?";
			n++;
		}
	}

	sqlite3_stmt* stmt; int rc;
	rc = sqlite3_prepare_v2(db, sql.data(), sql.size(), &stmt, nullptr);

	int n = 1;
	for (int i=0; i < nCol; i++) {
		const auto& val = valsAfter.at(i);
		if (val.type) {
			if (!bindValue(stmt, val, n)) {
				return 0;
			}
			n++;
		}
	}

	for (int i=0; i < nCol; i++) {
		const auto& val = valsBefore.at(i);
		if (val.type) {
			if (!bindValue(stmt, val, n)) {
				return 0;
			}
			n++;
		}
	}

	rc = sqlite3_step(stmt);

	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		return 0;
	} else {
		return nRead;
	}
}

size_t applyInstruction(sqlite3* db, const char* tableName, u32 nCol, const char* buf)
{
	size_t nRead = 0;

	// Read instruction type
	u8 iType = *buf;
	buf++; nRead++;
	buf++; nRead++;

	switch(iType){
	case SQLITE_INSERT:
		nRead += applyInsert(db, tableName, nCol, buf);
		break;
	case SQLITE_DELETE: {
		nRead += applyDelete(db, tableName, nCol, buf);
		break;
	}
	case SQLITE_UPDATE: {
		nRead += applyUpdate(db, tableName, nCol, buf);
		break;
	}
	default: {
		// INVALID
		return 0;
	}
	}

	return nRead;
}

int applyChangeset(sqlite3* db, const char* buf, size_t size)
{
	int rc;
	const char* bufStart = buf;

	rc = sqlite3_exec(db, "SAVEPOINT changeset_apply", 0, 0, 0);
	if( rc==SQLITE_OK ){
		rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 1", 0, 0, 0);
	}

	size_t off = 0;

	while( rc==SQLITE_OK && (buf - bufStart) < size ) {
		char op = buf[0];
		buf++;

		// Read OP
		if (op != 'T') {
			return CHANGESET_CORRUPT;
		}

		// Read number of columns
		u32 nCol;
		u8 varintLen = getVarint32((u8*)buf, nCol);
		buf += varintLen;

		// Read Primary Key flags
		bool PKs[nCol];
		for (int i=0; i < nCol; i++) {
			PKs[i] = (bool) buf[i];
		}
		buf += nCol;

		// Read table name
		const char* tableName = buf;
		size_t tableNameLen = std::strlen(tableName);
		buf += tableNameLen + 1;

		size_t instrRead = 0;

		while((instrRead = applyInstruction(db, tableName, nCol, buf))) {
			buf += instrRead;
		}
	}

	rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 0", 0, 0, 0);
	rc = sqlite3_exec(db, "RELEASE changeset_apply", 0, 0, 0);

	return rc;
}

int applyChangeset(sqlite3* db, const char* filename)
{
	std::ifstream file(filename, std::ios::binary | std::ios::ate);
	const auto size = file.tellg();

	if (!size) {
		return 1;
	}

	file.seekg(0);
	std::vector<char> buffer(size);
	if (! file.read(buffer.data(), size)) {
		return 1;
	}

	return applyChangeset(db, buffer.data(), size);
}
