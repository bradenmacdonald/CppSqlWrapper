////////////////////////////////////////////////////////////////////////////////
// CppSqlWrapper - A lightweight C++ wrapper for SQLite3.
//
// Copyright (c) 2011 Braden MacDonald.
//
// Based on:
// CppSQLite3 - A C++ wrapper around the SQLite3 embedded database library.
//
// Copyright (c) 2004 Rob Groves. All Rights Reserved. rob.groves@btinternet.com
// 
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement, is hereby granted, provided that the above copyright notice, 
// this paragraph and the following two paragraphs appear in all copies, 
// modifications, and distributions.
//
// IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
// INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
// PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE. THE SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF
// ANY, PROVIDED HEREUNDER IS PROVIDED "AS IS". THE AUTHOR HAS NO OBLIGATION
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//
////////////////////////////////////////////////////////////////////////////////
#include "CppSqlWrapper.h"

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <exception>
#include <sstream>

#include "sqlite3.h"

////////////////////////////////////////////////////////////////////////////////

#define assert(a) if (!(a)) { throw SqlDatabaseException("Assertion failed: " #a); }
#define require(b) assert(b != NULL)
void ThrowStatusCodeException(int statusCode, sqlite3* db) {
	if (statusCode == SQLITE_BUSY)
		throw SqlDatabaseBusyException();
	else if (statusCode == SQLITE_ERROR)
		throw SqlDatabaseException(sqlite3_errmsg(db));
	else {
		const static char* code_descr[] = {"SQLITE_OK", "SQLITE_ERROR", "SQLITE_INTERNAL", "SQLITE_PERM", 
			"SQLITE_ABORT", "SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_NOMEM", "SQLITE_READONLY", "SQLITE_INTERRUPT", 
			"SQLITE_IOERR", "SQLITE_CORRUPT", "SQLITE_NOTFOUND", "SQLITE_FULL", "SQLITE_CANTOPEN", 
			"SQLITE_PROTOCOL", "SQLITE_EMPTY", "SQLITE_SCHEMA", "SQLITE_TOOBIG", "SQLITE_CONSTRAINT",
			"SQLITE_MISMATCH", "SQLITE_MISUSE", "SQLITE_NOLFS", "SQLITE_AUTH", "SQLITE_FORMAT", "SQLITE_RANGE",
			"SQLITE_NOTADB"};
		std::ostringstream msg;
		msg << "Result code " << statusCode;
		if (statusCode < int(sizeof(code_descr) / sizeof(code_descr[0])))
			msg << " (" << code_descr[statusCode] << ")";
		throw SqlDatabaseException(msg.str());
	}
}
inline void ThrowStatusCodeException(int statusCode, sqlite3_stmt* vm) { ThrowStatusCodeException(statusCode, sqlite3_db_handle(vm)); }

////////////////////////////////////////////////////////////////////////////////
#ifdef _MSC_VER // Disable "warning C4355: 'this' : used in base member initializer list".
#pragma warning(push)
#pragma warning(disable:4355)
#endif
SqlStatement::SqlStatement()
	: mpVM(0), mBindNext(1), mResult(this), mEndOfRows(true), mColsInResult(0)
{}
SqlStatement::SqlStatement(sqlite3_stmt* pVM)
	: mpVM(pVM), mBindNext(1), mResult(this), mEndOfRows(true), mColsInResult(0)
{}

SqlStatement::SqlStatement(const SqlStatement& rStatement)
	:
    mpVM(rStatement.mpVM),
	mBindNext(rStatement.mBindNext),
	mEndOfRows(rStatement.mEndOfRows),
	mResult(this),
	mColsInResult(rStatement.mColsInResult)
{
	// Important: the new object will now own the VM, so we must ensure
	// rStatement doesn't finalize() the VM when it gets destroyed:
	const_cast<SqlStatement&>(rStatement).mpVM = 0;
}

SqlStatement& SqlStatement::operator=(const SqlStatement& rStatement) {
	destroy();
    mpVM = rStatement.mpVM;
	mBindNext = rStatement.mBindNext;
	mEndOfRows = rStatement.mEndOfRows;
	mResult = ResultRow(this);
	mColsInResult = rStatement.mColsInResult;
	// Important: the new object will now own the VM, so we must ensure
	// rStatement doesn't finalize() the VM when it gets destroyed:
	const_cast<SqlStatement&>(rStatement).mpVM = 0;
	return *this;
}
#ifdef _MSC_VER
#pragma warning(pop)
#endif

void SqlStatement::destroy() {
	mEndOfRows = true;
	if (mpVM) {
		sqlite3_finalize(mpVM);
		mpVM = 0;
	}
}

inline void SqlStatement::onBind() {
	// Internal method with the processing that must be done every time we bind() a value
	require(mpVM);
	if (mBindNext == 1) {
		// If we are binding the first parameter (index 1, not 0), we need to reset the VM
		// and clear any previous result info associated with this object
		sqlite3_reset(mpVM);
		mEndOfRows = true;
		mColsInResult = 0;
	}
}

SqlStatement &SqlStatement::bind(const char* szValue) {
	onBind();
	if (sqlite3_bind_text(mpVM, mBindNext++, szValue, -1, SQLITE_TRANSIENT) != SQLITE_OK)
		throw SqlDatabaseException("Error binding string param.");
	return *this;
}

SqlStatement &SqlStatement::bind(const int nValue) {
	onBind();
	if (sqlite3_bind_int(mpVM, mBindNext++, nValue) != SQLITE_OK)
		throw SqlDatabaseException("Error binding int param");
	return *this;
}

SqlStatement &SqlStatement::bind(const int64_t nValue) {
	onBind();
	if (sqlite3_bind_int64(mpVM, mBindNext++, nValue) != SQLITE_OK)
		throw SqlDatabaseException("Error binding int param");
	return *this;
}


SqlStatement &SqlStatement::bind(const double dValue) {
	onBind();
	if (sqlite3_bind_double(mpVM, mBindNext++, dValue) != SQLITE_OK)
		throw SqlDatabaseException("Error binding double param");
	return *this;
}


SqlStatement &SqlStatement::bind(const unsigned char* blobValue, int nLen) {
	onBind();
	if (sqlite3_bind_blob(mpVM, mBindNext++, (const void*)blobValue, nLen, SQLITE_TRANSIENT) != SQLITE_OK)
		throw SqlDatabaseException("Error binding blob param");
	return *this;
}


SqlStatement &SqlStatement::bindNull() {
	onBind();
	if (sqlite3_bind_null(mpVM, mBindNext++) != SQLITE_OK)
		throw SqlDatabaseException("Error binding NULL param");
	return *this;
}

SqlStatement &SqlStatement::bindSame() {
	onBind();
	mBindNext++;
	return *this;
}

SqlStatement &SqlStatement::execute() {
	require(mpVM);
	mBindNext = 1; // Next call to bind() should replace the first parameter (it's not zero-indexed)
	if (!mEndOfRows)
		sqlite3_reset(mpVM); // User wants to reset the query even though we haven't finished going through all rows...

	const int result = sqlite3_step(mpVM);
	if (result == SQLITE_DONE) {
		mEndOfRows = true; // No rows were returned
		mColsInResult = 0;
		return *this;
	} else if (result == SQLITE_ROW) {
		mEndOfRows = false; // At least one row was returned
		mColsInResult = sqlite3_column_count(mpVM);
		return *this;
	} else {
		ThrowStatusCodeException(result, mpVM);
		return *this;
	}
}

const SqlStatement::ResultRow& SqlStatement::currentRow() const {
	require(mpVM);
	if (mEndOfRows){ throw SqlDatabaseException("called currentRow() after reaching end of rows"); }
	return mResult;
}

bool SqlStatement::nextRow() {
	require(mpVM);
	if (mEndOfRows)
		return false;// Already at the last row.
	
	const int result = sqlite3_step(mpVM);
	if (result == SQLITE_DONE) {
		mEndOfRows = true; // No rows were returned
	} else if (result == SQLITE_ROW) {
		mEndOfRows = false; // At least one row was returned
	} else {
		ThrowStatusCodeException(result, mpVM);
		mEndOfRows = true;
	}
	return !mEndOfRows; // Returns false if there are no more rows, otherwise true
}


////////////////////////////////////////////////////////////////////////////////

int SqlStatement::ResultRow::numFields() const {
	require(mpParent->mpVM);
	return mpParent->mColsInResult;
}

inline void SqlStatement::ResultRow::checkIndex(int nField) const {
	if (nField < 0 || nField >= mpParent->mColsInResult)
		throw SqlDatabaseException("Invalid column index.");
}


int SqlStatement::ResultRow::getIntField(int nField, int nNullValue/*=0*/) const {
	if (fieldDataType(nField) == SQLITE_NULL)
		return nNullValue;
	else
		return sqlite3_column_int(mpParent->mpVM, nField);
}


int SqlStatement::ResultRow::getIntField(const char* szField, int nNullValue/*=0*/) const {
	return getIntField(fieldIndex(szField), nNullValue);
}

int64_t SqlStatement::ResultRow::getInt64Field(int nField, int nNullValue/*=0*/) const {
	if (fieldDataType(nField) == SQLITE_NULL)
		return nNullValue;
	else
		return sqlite3_column_int64(mpParent->mpVM, nField);
}


int64_t SqlStatement::ResultRow::getInt64Field(const char* szField, int nNullValue/*=0*/) const {
	return getInt64Field(fieldIndex(szField), nNullValue);
}


double SqlStatement::ResultRow::getFloatField(int nField, double fNullValue/*=0.0*/) const {
	if (fieldDataType(nField) == SQLITE_NULL)
		return fNullValue;
	else
		return sqlite3_column_double(mpParent->mpVM, nField);
}


double SqlStatement::ResultRow::getFloatField(const char* szField, double fNullValue/*=0.0*/) const {
	return getFloatField(fieldIndex(szField), fNullValue);
}


const char* SqlStatement::ResultRow::getStringField(int nField, const char* szNullValue/*=""*/) const {
	if (fieldDataType(nField) == SQLITE_NULL)
		return szNullValue;
	else
		return (const char*)sqlite3_column_text(mpParent->mpVM, nField);
}


const char* SqlStatement::ResultRow::getStringField(const char* szField, const char* szNullValue/*=""*/) const {
	return getStringField(fieldIndex(szField), szNullValue);
}


const unsigned char* SqlStatement::ResultRow::getBlobField(int nField, int& nLen) const {
	require(mpParent->mpVM);
	checkIndex(nField);

	nLen = sqlite3_column_bytes(mpParent->mpVM, nField);
	return (const unsigned char*)sqlite3_column_blob(mpParent->mpVM, nField);
}


const unsigned char* SqlStatement::ResultRow::getBlobField(const char* szField, int& nLen) const {
	return getBlobField(fieldIndex(szField), nLen);
}

int SqlStatement::ResultRow::fieldIndex(const char* szField) const {
	require(mpParent->mpVM);
	assert(szField != 0);

	for (int nField = 0; nField < mpParent->mColsInResult; nField++)	{
		if (strcmp(szField, sqlite3_column_name(mpParent->mpVM, nField)) == 0) {
			return nField;
		}
	}
	throw SqlDatabaseException("Invalid field name requested");
}


const char* SqlStatement::ResultRow::fieldName(int nField) const {
	require(mpParent->mpVM);
	checkIndex(nField);
	return sqlite3_column_name(mpParent->mpVM, nField);
}


const char* SqlStatement::ResultRow::fieldDeclType(int nField) const {
	require(mpParent->mpVM);
	checkIndex(nField);
	return sqlite3_column_decltype(mpParent->mpVM, nField);
}


SqlType SqlStatement::ResultRow::fieldDataType(int nField) const {
	require(mpParent->mpVM);
	checkIndex(nField);
	return (SqlType)sqlite3_column_type(mpParent->mpVM, nField);
}

////////////////////////////////////////////////////////////////////////////////

SqlDatabase::SqlDatabase(const char* szFile, bool useExclusiveWAL /* = true */) {
	mpDB = 0;
	mnBusyTimeoutMs = 60000; // 60 seconds
	assert(sqlite3_libversion_number()==SQLITE_VERSION_NUMBER);

	if (sqlite3_open(szFile, &mpDB) != SQLITE_OK)
		throw SqlDatabaseException("Unable to open/create database file.");
	setBusyTimeout(mnBusyTimeoutMs);
	if (useExclusiveWAL) {
		// Set the database to use Write-Ahead Logging and the EXCLUSIVE locking mode
		// for performance improvements:
		sqlExecute("PRAGMA locking_mode = EXCLUSIVE; PRAGMA journal_mode=WAL;");
	}
}


SqlDatabase::SqlDatabase(const SqlDatabase& db) {
	mpDB = db.mpDB;
	mnBusyTimeoutMs = 60000; // 60 seconds
}


SqlDatabase::~SqlDatabase() {
	try {
		close();
	} catch (...) {} // Destructors must not propagate exceptions
}


SqlDatabase& SqlDatabase::operator=(const SqlDatabase& db) {
	mpDB = db.mpDB;
	mnBusyTimeoutMs = 60000; // 60 seconds
	return *this;
}


void SqlDatabase::close() {
	if (mpDB) {
		// ensure that we have destroyed all compiled statements:
		if (sqlite3_next_stmt(mpDB, 0) != 0)
			throw SqlDatabaseException("Tried to close a database before deleting or calling destroy() on all statement objects.");
		int result = sqlite3_close(mpDB);
		if (result != SQLITE_OK)
			ThrowStatusCodeException(result, mpDB);
		mpDB = 0;
	}
}


SqlStatement SqlDatabase::sqlCompile(const char* szSQL) {
	require(mpDB);

	const char* szTail=0;
	sqlite3_stmt* pVM;

	const int result = sqlite3_prepare_v2(mpDB, szSQL, -1, &pVM, &szTail);
	assert(szTail != 0);
	const bool extra_statements = (szTail[0] != '\0'); // was (szTail && szTail[0] != '\0')
	if (result != SQLITE_OK)
		ThrowStatusCodeException(result, mpDB);
	if (extra_statements)
		throw SqlDatabaseException("sqlCompile() only compiles the first statement; other statements have been ignored.");
	return SqlStatement(pVM);
}


bool SqlDatabase::tableExists(const char* szTable) {
	char szSQL[128];
	sprintf(szSQL,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'",
			szTable);
	int nRet = getScalar(szSQL);
	return (nRet > 0);
}


int SqlDatabase::getScalar(const char* szSQL, int errorValue) {
	SqlStatement q = sqlQuery(szSQL);
	if (!q.hasRow() || q.currentRow().numFields() < 1)
		return errorValue;
	return q.currentRow().getIntField(0);
}


/////////////////////// Start new DB access methods:

// Execute the given SQL code, and return TRUE on success or FALSE on failure
void SqlDatabase::sqlExecute(const char* szSQL) {
	require(mpDB);
	int result = sqlite3_exec(mpDB, szSQL, 0, 0, 0);
	if (result != SQLITE_OK)
		ThrowStatusCodeException(result, mpDB);
}

// Format SQL with given arguments, then execute it.
// Supports "%q", "%Q", and "%z" formatting options, which should always be preferred to %s
// See http://www.sqlite.org/c3ref/mprintf.html for details on using these formatting options
void SqlDatabase::sqlExec(const char* szSQL, ...) {
	require(mpDB);
	va_list va;
	va_start(va, szSQL);
	char* szSqlFormatted = sqlite3_vmprintf(szSQL, va);
	va_end(va);
	if (!szSqlFormatted)
		throw SqlDatabaseException("Unable to apply format to SQL string");
	const int result = sqlite3_exec(mpDB, szSqlFormatted, 0, 0, 0);
	sqlite3_free(szSqlFormatted);
	if (result != SQLITE_OK)
		ThrowStatusCodeException(result, mpDB);
}
// Same but accepts a va_list
void SqlDatabase::sqlExecVar(const char* szSQL, va_list args) {
	require(mpDB);
	char* szSqlFormatted = sqlite3_vmprintf(szSQL, args);
	if (!szSqlFormatted)
		throw SqlDatabaseException("Unable to apply format to SQL string");

	const int result = sqlite3_exec(mpDB, szSqlFormatted, 0, 0, 0);
	sqlite3_free(szSqlFormatted);
	if (result != SQLITE_OK)
		ThrowStatusCodeException(result, mpDB);
}

int SqlDatabase::numberOfRowsChanged() const {
	return sqlite3_changes(mpDB);
}


SqlStatement SqlDatabase::sqlQuery(const char* szSQL, ...) {
	va_list va;
	va_start(va, szSQL);
	SqlStatement result = sqlQueryVar(szSQL, va);
	va_end(va);
	return result;
}
SqlStatement SqlDatabase::sqlQueryVar(const char* szSQL, va_list args) {
	require(mpDB);

	char* szSqlFormatted = sqlite3_vmprintf(szSQL, args);
	if (!szSqlFormatted)
		throw SqlDatabaseException("Unable to apply format to SQL string");
	const char* szTail=0;
	sqlite3_stmt* pVM;
	
	const int result = sqlite3_prepare_v2(mpDB, szSqlFormatted, -1, &pVM, &szTail);
	assert(szTail != 0);
	const bool extra_statements = (szTail[0] != '\0'); // was (szTail && szTail[0] != '\0')
	sqlite3_free(szSqlFormatted);
	if (result != SQLITE_OK)
		ThrowStatusCodeException(result, mpDB);
	if (extra_statements)
		throw SqlDatabaseException("sqlQuery() only compiles the first statement; other statements have been ignored.");
	
	return SqlStatement(pVM).execute();
}

std::string SqlDatabase::sqlFormat(char formatType, const char* str) {
	const char fmtStr[3] = {'%', formatType, '\0'};
	const char* pStr = sqlite3_mprintf(fmtStr, str);
	std::string result(pStr);
	sqlite3_free((void*)pStr);
	return result;
}
std::string SqlDatabase::sqlFormat(const char* formatString, ...) {
	va_list va;
	va_start(va, formatString);
	const char* pStr = sqlite3_vmprintf(formatString, va);
	va_end(va);
	std::string result(pStr);
	sqlite3_free((void*)pStr);
	return result;
}


int64_t SqlDatabase::lastRowId() {
	return sqlite3_last_insert_rowid(mpDB);
}


void SqlDatabase::setBusyTimeout(int nMillisecs) {
	mnBusyTimeoutMs = nMillisecs;
	sqlite3_busy_timeout(mpDB, mnBusyTimeoutMs);
}

void SqlDatabase::interrupt() { sqlite3_interrupt(mpDB); }

const char* SqlDatabase::SQLiteVersion() { return SQLITE_VERSION; }

void SqlDatabase::setSqlTraceHandler(void(*pHandler)(void*,const char*), void* customArg) {
	sqlite3_trace(mpDB, pHandler, customArg);
}
