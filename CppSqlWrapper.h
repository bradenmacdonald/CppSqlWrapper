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
#ifndef CPP_SQL_WRAPPER_H
#define CPP_SQL_WRAPPER_H

#include <stdint.h>     // Needed for int64 type
#include <stdarg.h>     // Needed for the definition of va_list
#include <string>
#include <stdexcept>

// Forward declaration:
class SqlDatabase;
// Declare SQLite internal structures:
struct sqlite3;
struct sqlite3_stmt;

// Declare the SQLite3 datatypes so that "sqlite3.h" does not need to be included
#ifndef SQLITE_INTEGER
enum SqlType {
	SQLITE_INTEGER=1,
	SQLITE_FLOAT=2,
	SQLITE_TEXT=3,
	SQLITE_BLOB=4,
	SQLITE_NULL=5,
};
#else
typedef int SqlType;
#endif

class SqlDatabaseException : public std::runtime_error {
public:
	SqlDatabaseException(const std::string& reason) : std::runtime_error(std::string("Database error: ").append(reason)) { }
};
class SqlDatabaseBusyException : public SqlDatabaseException {
public:
	SqlDatabaseBusyException() : SqlDatabaseException("Database error: Database busy.") { }
};

class SqlStatement {
	friend class ResultRow;
public:
	SqlStatement();
	SqlStatement(sqlite3_stmt* pVM);
	SqlStatement(const SqlStatement& rStatement);
	~SqlStatement() { destroy(); }
	SqlStatement& operator=(const SqlStatement& rStatement);

	/////////// Parameter Binding Methods /////////////
	// Each of these will bind the supplied value to the next unset parameter
	// Returns a self-reference, so one can say: statement.bind("a").bind("b").execute();
	SqlStatement &bind(const char* szValue);
    SqlStatement &bind(const int nValue);
	SqlStatement &bind(const int64_t nValue);
    SqlStatement &bind(const double dwValue);
    SqlStatement &bind(const unsigned char* blobValue, int nLen);
    SqlStatement &bindNull();
	SqlStatement &bindSame(); // leave a bound parameter unchanged
	
	/////////// The two methods to run the SQL 
	// After binding all parameters, call execute() or query()

	// Execute the SQL statement (and, if applicable, get the first resulting row)
	// This object will be reset and can then be re-used
	// by binding new parameters and calling execute() again
	// Returns a self-reference
	SqlStatement &execute();
	// TODO: getSingleRow() method which returns one row or causes error.

	class ResultRow { // This class only exists to make the syntax a bit cleaner
		friend class SqlStatement;
	public:
		ResultRow(SqlStatement* parent) : mpParent(parent) {}
		
		// the number of columns in the current result row:
		int numFields() const;

		int fieldIndex(const char* szField) const;
		const char* fieldName(int nField) const;

		const char* fieldDeclType(int nField) const;
		SqlType fieldDataType(int nField) const;

		int getIntField(int nField, int nNullValue=0) const;
		int getIntField(const char* szField, int nNullValue=0) const;
		
		int64_t getInt64Field(int nField, int nNullValue=0) const;
		int64_t getInt64Field(const char* szField, int nNullValue=0) const;

		double getFloatField(int nField, double fNullValue=0.0) const;
		double getFloatField(const char* szField, double fNullValue=0.0) const;

		const char* getStringField(int nField, const char* szNullValue="") const;
		const char* getStringField(const char* szField, const char* szNullValue="") const;

		const unsigned char* getBlobField(int nField, int& nLen) const;
		const unsigned char* getBlobField(const char* szField, int& nLen) const;

		bool fieldIsNull(int nField) const { return (fieldDataType(nField) == SQLITE_NULL); }
		bool fieldIsNull(const char* szField) const { return fieldIsNull(fieldIndex(szField)); }
	private:
		inline void checkIndex(int nField) const;
		const SqlStatement* mpParent;

		ResultRow(const ResultRow& rResultRow) : mpParent(rResultRow.mpParent) {}
		ResultRow& operator=(const ResultRow& rResultRow) {
			mpParent = rResultRow.mpParent;	return *this;
		}
	};

	// Get the current result row. Will be the first row if you haven't called nextRow()
	// ** Do not store the result, but always access it like the following:
	//    statement.currentRow().getStringField("username");
	const ResultRow& currentRow() const;
	// Do not call currentRow() without checking one of the following first:
	bool hasRow() const { return !mEndOfRows; }
	bool nextRow(); // Advance current row forward; returns false if we were at the last row

	// Free all resources associated with this sql statement:
	// In general, resources will automatically be freed by this statement's destructor as
	// it goes out of scope. Use this method only if you are being very conscious of memory
	// use, or you need to close the database before this object goes out of scope.
	void destroy();
private:
	inline void onBind();
    sqlite3_stmt* mpVM;
	int mBindNext;
	bool mEndOfRows; // when this is true, currentRow() is invalid.
	ResultRow mResult;
	int mColsInResult; // Number of columns in the result set
};


class SqlDatabase {
public:
	///////// Open and close a database //////////////////////////////////////////////////////
	

	// Open a database, creating the file if it doesn't exist
	// useExclusiveWAL: if true, this will enable Write-Ahead Logging and set the database to 
	//         exclusive locking mode. This is highly recommended for performance reasons,
	//         but means no other processes can access the database concurrently and that
	//         the resulting database file cannot be opened by SQLite < 3.7.0
	// Returns true on success, false on failure.
    SqlDatabase(const char* szFile, bool useExclusiveWAL = true );
	// Close a database. All SqlStatement objects must be freed (go out of scope, with 
	// 'delete', or using their destroy() method) before close() will work successfully.
    void close();
	// Destructor (calls close())
	virtual ~SqlDatabase();

	///////// Methods for executing SQL commands and queries /////////////////////////////////

    SqlStatement sqlCompile(const char* szSQL);
	SqlStatement sqlCompile(const std::string& szSQL) { return sqlCompile(szSQL.c_str()); }

	// Execute the given SQL code.
	void sqlExecute(const char* szSQL);
	void sqlExecute(const std::string& szSQL) { sqlExecute(szSQL.c_str()); }

	// Format SQL with given arguments, then execute it.
	// Supports "%q", "%Q", and "%z" formatting options, which should always be preferred to %s
	// See http://www.sqlite.org/c3ref/mprintf.html for details on using these formatting options
	void sqlExec(const char* szSQL, ...);
	void sqlExecVar(const char* szSQL, va_list args);

	// Compile and execute the given SQL code, and allow caller to access result rows one-by-one
	// Supports "%q", "%Q", and "%z" formatting options, which should always be preferred to %s
	// See http://www.sqlite.org/c3ref/mprintf.html for details on using these formatting options
	SqlStatement sqlQuery(const char* szSQL, ...);
	SqlStatement sqlQueryVar(const char* szSQL, va_list args);

	// Format using SQLite's printf-like formatting tools. The first version accepts only a single
	// argument, and its format code is a character ('q', 'Q', etc.) rather than a string like "%Q"
	// Supports "%q", "%Q", and "%z" formatting options, which should always be preferred to %s
	// See http://www.sqlite.org/c3ref/mprintf.html for details on using these formatting options
	std::string sqlFormat(char formatType, const char* str);
	std::string sqlFormat(const char* formatString, ...);

	///////// Methods returning information about the last SQL statement /////////////////////

	// Get the ROWID of the last successful INSERT statement:
	int64_t lastRowId();
	// Get the number of rows changed by the previous statement completed on this database:
	// This only counts changes by INSERT, UPDATE, and DELETE
	int numberOfRowsChanged() const;
	
	///////// Helpful Shortcut Methods ///////////////////////////////////////////////////////
	
	bool tableExists(const char* szTable);

    int getScalar(const char* szSQL, int errorValue = -1);
	int getScalar(const std::string& szSQL, int errorValue = -1) { return getScalar(szSQL.c_str(), errorValue); }

	///////// Miscellaneous //////////////////////////////////////////////////////////////////

    void interrupt(); // Abort any pending database operations
    void setBusyTimeout(int nMillisecs);
    static const char* SQLiteVersion();

	// If you want all SQL code to be traced out before each query, you can use this to
	// set a custom handler. The const char* parameter will be the full SQL query.
	void setSqlTraceHandler(void(*pHandler)(void*,const char*), void* customArg = 0);

private:
    SqlDatabase(const SqlDatabase& db);
    SqlDatabase& operator=(const SqlDatabase& db);

    sqlite3* mpDB;
    int mnBusyTimeoutMs;
};

#endif

