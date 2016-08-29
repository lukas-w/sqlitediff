#include "diff.h"

#include "sqliteint.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>

#include <chrono>

#include "patch.h"

#define CHANGESET_CORRUPT 1
#define CHANGESET_INSTRUCTION_CORRUPT 3
#define CHANGESET_CALLBACK_ERROR 4

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

size_t readValue(const char* buf, sqlite_value* val)
{
	u8 type = buf[0];
	val->type = type;
	buf++;
	void* data = (void*)(buf);

	size_t read = 1;

	switch(type)
	{
	case SQLITE_INTEGER: {
		val->data1.iVal = sessionGetI64((u8*)data);
		read += 8;
		break;
	}
	case SQLITE_FLOAT: {
		int64_t iVal = sessionGetI64((u8*)data);
		val->data1.dVal = *(double*)(&iVal);
		read += 8;
		break;
	}

	case SQLITE_TEXT: {
		u32 textLen;
		u8 varIntLen = getVarint32((u8*)data, textLen);

		val->data1.iVal = textLen;
		val->data2 = (char*)data + varIntLen;

		read += textLen + varIntLen;
		break;
	}
	case SQLITE_BLOB: {
		u32 blobLen;
		u8 varIntLen = getVarint32((u8*)data, blobLen);

		val->data1.iVal = blobLen;
		val->data2 = (char*)data + varIntLen;

		read += blobLen + varIntLen;
		break;
	}
	case SQLITE_NULL:
		break;
	case 0:
		val->type = 0;
		break;
	default:
		val->type = -1;
		return 0;
	}

	return read;
}

int bindValue(sqlite3_stmt* stmt, int col, const sqlite_value* val) {
	switch(val->type) {
	case SQLITE_INTEGER:
		return sqlite3_bind_int64(stmt, col, val->data1.iVal);
	case SQLITE_FLOAT:
		return sqlite3_bind_double(stmt, col, val->data1.dVal);
	case SQLITE_TEXT:
		return sqlite3_bind_text(stmt, col, val->data2, val->data1.iVal, nullptr);
	case SQLITE_BLOB:
		return sqlite3_bind_blob64(stmt, col, val->data2, val->data1.iVal, nullptr);
	case SQLITE_NULL:
		return sqlite3_bind_null(stmt, col);
	default:
		return SQLITE_INTERNAL;
	}
}

int bindValues(sqlite3_stmt* stmt, sqlite_value* values, uint8_t nCol) {
	for (int i=0; i < nCol; i++) {
		int rc = bindValue(stmt, i+1, &values[i]);
		if (rc != SQLITE_OK) {
			return rc;
		}
	}
	return SQLITE_OK;
}

int applyInsert(sqlite3* db, const Instruction* instr)
{
	int rc;

	int nCol = instr->table->nCol;
	const char* tableName = instr->table->tableName;

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
		return rc;
	}


	rc = bindValues(stmt, instr->values, nCol);
	if (rc != SQLITE_OK) {
		return rc;
	}

	rc = sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		std::cerr << "Error applying insert: " << sqlite3_errmsg(db) << std::endl;
		return rc;
	}

	return SQLITE_OK;
}

std::vector<std::string> getColumnNames(sqlite3* db, const char* tableName)
{
	std::vector<std::string> result; int rc;

	sqlite3_stmt* stmt;
	std::string sql = std::string() + "pragma table_info(" + tableName +");";
	rc = sqlite3_prepare_v2(db, sql.data(), -1, &stmt, nullptr);
	if (rc!=SQLITE_OK) {
		return result;
	}

	while(rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW)
	{
		result.push_back((const char*)sqlite3_column_text(stmt, 1));
	}

	result.shrink_to_fit();

	sqlite3_finalize(stmt);
	return result;
}

int applyDelete(sqlite3* db, const Instruction* instr)
{
	int rc;

	auto columnNames = getColumnNames(db, instr->table->tableName);
	std::string sql = std::string() + "DELETE FROM " + instr->table->tableName + " WHERE ";

	uint8_t nCol = instr->table->nCol;

	std::vector<std::string> wheres;
	std::vector<sqlite_value> whereValues;
	for (int i=0; i < nCol; i++)
	{
		if (instr->values[i].type) {
			wheres.push_back(columnNames.at(i) + " = ?");
			whereValues.push_back(instr->values[i]);
		}
	}
	sql += std::accumulate(wheres.cbegin()+1, wheres.cend(), wheres.at(0), [&columnNames](const std::string&a, const std::string& b) {
		return a + " AND " + b;
	});

	sqlite3_stmt* stmt;
	rc = sqlite3_prepare_v2(db, sql.data(), sql.size(), &stmt, nullptr);

	if (rc != SQLITE_OK) {
		std::cerr << "Failed preparing DELETE statement " << sql << std::endl;
		return rc;
	}

	rc = bindValues(stmt, whereValues.data(), whereValues.size());
	if (rc != SQLITE_OK) {
		std::cerr << "Failed binding to DELETE statement " << sql << std::endl;
		return rc;
	}

	rc = sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		return rc;
	}

	return SQLITE_OK;
}

int applyUpdate(sqlite3* db, const Instruction* instr)
{
	int nCol = instr->table->nCol;

	sqlite_value* valsBefore = instr->values;
	sqlite_value* valsAfter = instr->values + nCol;

	std::vector<std::string> columnNames = getColumnNames(db, instr->table->tableName);

	std::string sql;
	sql = sql + "UPDATE " + instr->table->tableName + " SET";

	// sets
	for (int n=0, i=0; i < nCol; i++) {
		const auto& name = columnNames.at(i);
		const auto val = valsAfter[i];
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
		const auto val = valsBefore[i];
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

	if (rc != SQLITE_OK) {
		return 1;
	}

	int n = 1;
	for (int i=0; i < nCol; i++) {
		sqlite_value* val = &valsAfter[i];
		if (val->type) {
			if (bindValue(stmt, n, val)) {
				return 1;
			}
			n++;
		}
	}

	for (int i=0; i < nCol; i++) {
		sqlite_value* val = &valsBefore[i];
		if (val->type) {
			if (bindValue(stmt, n, val)) {
				return 1;
			}
			n++;
		}
	}

	rc = sqlite3_step(stmt);

	sqlite3_finalize(stmt);

	if (rc != SQLITE_DONE) {
		return rc;
	}
	return SQLITE_OK;
}


int applyInstruction(const Instruction* instr, sqlite3* db)
{
	switch(instr->iType) {
	case SQLITE_INSERT:
		return applyInsert(db, instr);
	case SQLITE_UPDATE:
		return applyUpdate(db, instr);
	case SQLITE_DELETE:
		return applyDelete(db, instr);
	default:
		return CHANGESET_CORRUPT;
	}
}


int applyInstructionCallback(const Instruction* instr, void* context)
{
	return applyInstruction(instr, (sqlite3*)context);
}


size_t readInstructionFromBuffer(const char* buf, Instruction* instr)
{
	size_t nRead = 0;

	instr->iType = *buf;
	buf += 2; nRead += 2;

	int nCol = instr->table->nCol;
	if (instr->iType == SQLITE_UPDATE) {
		nCol *= 2;
	}

	for (int i=0; i < nCol; i++) {
		sqlite_value* val_p = instr->values + i;
		size_t read = readValue(buf, val_p);
		if (read == 0) {
			return 0;
		}
		buf += read;
		nRead += read;
	}

	return nRead;
}


int applyChangeset(sqlite3* db, const char* buf, size_t size)
{
	int rc;

	rc = sqlite3_exec(db, "SAVEPOINT changeset_apply", 0, 0, 0);
	if( rc==SQLITE_OK ){
		rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 1", 0, 0, 0);
	}

	rc = readChangeset(buf, size, applyInstructionCallback, db);

	if (rc) {
		std::cerr << "Error occured." << std::endl;
		rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 0", 0, 0, 0);
		rc = sqlite3_exec(db, "ROLLBACK TO SAVEPOINT changeset_apply", 0, 0, 0);
	} else {
		rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 0", 0, 0, 0);
		rc = sqlite3_exec(db, "RELEASE changeset_apply", 0, 0, 0);
	}

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


int readChangeset(const char* buf, size_t size, InstrCallback instr_callback, void* context)
{
	const char* const bufStart = buf;
	const char* const bufEnd = buf + size;

	double lastPos = .0;
	auto t1 = std::chrono::high_resolution_clock::now();

	while(buf < bufEnd) {
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
		std::vector<int> PKs(nCol);
		for (u32 i=0; i < nCol; i++) {
			PKs[i] = (bool) buf[i];
		}
		buf += nCol;

		// Read table name
		const char* tableName = buf;
		size_t tableNameLen = std::strlen(tableName);
		buf += tableNameLen + 1;

		size_t instrRead = 0;

		TableInfo table;
		table.PKs = PKs.data();
		table.nCol = nCol;
		table.tableName = tableName;

		Instruction instr;
		instr.table = &table;
		instr.values = new sqlite_value[nCol*2];


		while (buf < bufEnd && buf[0] != 'T') {
			instrRead = readInstructionFromBuffer(buf, &instr);
			if (instrRead == 0) {
				std::cerr << "Error reading instruction from buffer." << std::endl;
				delete[] instr.values;
				return CHANGESET_INSTRUCTION_CORRUPT;
			}

			int rc;
			if (instr_callback && (rc = instr_callback(&instr, context))) {
				std::cerr << "Error applying instruction. Callback returned " << rc << std::endl;
				delete[] instr.values;
				return CHANGESET_CALLBACK_ERROR;
			}

			buf += instrRead;

			double pos = (double)(buf - bufStart) / (bufEnd - bufStart) * 100;
			if ((pos - lastPos) > 0.1) {
				auto t2 = std::chrono::high_resolution_clock::now();
				auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
				std::cerr << pos << "%, " << (time_span.count() / pos) * 100 << std::endl;
				lastPos = pos;
			}
		}

		delete[] instr.values;
	}

	return 0;
}

int readChangeset(const char* filename, InstrCallback instr_callback, void* context)
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

	return readChangeset(buffer.data(), size, instr_callback, context);
}
