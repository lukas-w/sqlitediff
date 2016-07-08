/*
** 2015-04-06
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This is a utility program that computes the differences in content
** between two SQLite databases.
**
** To compile, simply link against SQLite.
**
** See the showHelp() routine below for a brief description of how to
** run the utility.
*/
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>

#include "sqliteint.h"
#include "sqlite3.h"

/*
** All global variables are gathered into the "g" singleton.
*/
struct GlobalVars {
  int bSchemaOnly;          /* Only show schema differences */
  unsigned fDebug;          /* Debug flags */
  sqlite3 *db;              /* The database connection */
} g;

/*
** Allowed values for g.fDebug
*/
#define DEBUG_COLUMN_NAMES  0x000001
#define DEBUG_DIFF_SQL      0x000002

/*
** Dynamic string object
*/
typedef struct Str Str;
struct Str {
  char *z;        /* Text of the string */
  int nAlloc;     /* Bytes allocated in z[] */
  int nUsed;      /* Bytes actually used in z[] */
};

/*
** Initialize a Str object
*/
static void strInit(Str *p){
  p->z = 0;
  p->nAlloc = 0;
  p->nUsed = 0;
}

/*
** Print an error message for an error that occurs at runtime, then
** abort the program.
*/
static int runtimeError(const char *zFormat, ...){
  va_list ap;
  fprintf(stderr, "sqldiff: ");
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  return 1;
}

/*
** Free all memory held by a Str object
*/
static void strFree(Str *p){
  sqlite3_free(p->z);
  strInit(p);
}

/*
** Add formatted text to the end of a Str object
*/
static void strPrintf(Str *p, const char *zFormat, ...){
  int nNew;
  for(;;){
    if( p->z ){
      va_list ap;
      va_start(ap, zFormat);
      sqlite3_vsnprintf(p->nAlloc-p->nUsed, p->z+p->nUsed, zFormat, ap);
      va_end(ap);
      nNew = (int)strlen(p->z + p->nUsed);
    }else{
      nNew = p->nAlloc;
    }
    if( p->nUsed+nNew < p->nAlloc-1 ){
      p->nUsed += nNew;
      break;
    }
    p->nAlloc = p->nAlloc*2 + 1000;
    p->z = sqlite3_realloc(p->z, p->nAlloc);
    if( p->z==0 ) runtimeError("out of memory");
  }
}



/* Safely quote an SQL identifier.  Use the minimum amount of transformation
** necessary to allow the string to be used with %s.
**
** Space to hold the returned string is obtained from sqlite3_malloc().  The
** caller is responsible for ensuring this space is freed when no longer
** needed.
*/
static char *safeId(const char *zId){
  /* All SQLite keywords, in alphabetical order */
  static const char *azKeywords[] = {
    "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS",
    "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY",
    "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
    "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE",
    "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH",
    "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN",
    "FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", "GROUP", "HAVING", "IF",
    "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER",
    "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY",
    "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL", "NO", "NOT", "NOTNULL",
    "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PLAN", "PRAGMA",
    "PRIMARY", "QUERY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP",
    "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT",
    "ROLLBACK", "ROW", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP",
    "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER", "UNION", "UNIQUE",
    "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
    "WITH", "WITHOUT",
  };
  int lwr, upr, mid, c, i, x;
  if( zId[0]==0 ) return sqlite3_mprintf("\"\"");
  for(i=x=0; (c = zId[i])!=0; i++){
    if( !isalpha(c) && c!='_' ){
      if( i>0 && isdigit(c) ){
        x++;
      }else{
        return sqlite3_mprintf("\"%w\"", zId);
      }
    }
  }
  if( x ) return sqlite3_mprintf("%s", zId);
  lwr = 0;
  upr = sizeof(azKeywords)/sizeof(azKeywords[0]) - 1;
  while( lwr<=upr ){
    mid = (lwr+upr)/2;
    c = sqlite3_stricmp(azKeywords[mid], zId);
    if( c==0 ) return sqlite3_mprintf("\"%w\"", zId);
    if( c<0 ){
      lwr = mid+1;
    }else{
      upr = mid-1;
    }
  }
  return sqlite3_mprintf("%s", zId);
}

/*
** Prepare a new SQL statement.  Print an error and abort if anything
** goes wrong.
*/
static sqlite3_stmt *db_vprepare(const char *zFormat, va_list ap){
  char *zSql;
  int rc;
  sqlite3_stmt *pStmt;

  zSql = sqlite3_vmprintf(zFormat, ap);
  if( zSql==0 ) runtimeError("out of memory");
  rc = sqlite3_prepare_v2(g.db, zSql, -1, &pStmt, 0);
  if( rc ){
    runtimeError("SQL statement error: %s\n\"%s\"", sqlite3_errmsg(g.db),
                 zSql);
  }
  sqlite3_free(zSql);
  return pStmt;
}
static sqlite3_stmt *db_prepare(const char *zFormat, ...){
  va_list ap;
  sqlite3_stmt *pStmt;
  va_start(ap, zFormat);
  pStmt = db_vprepare(zFormat, ap);
  va_end(ap);
  return pStmt;
}

/*
** Check that table zTab exists and has the same schema in both the "main"
** and "aux" databases currently opened by the global db handle. If they
** do not, output an error message on stderr and exit(1). Otherwise, if
** the schemas do match, return control to the caller.
*/
static void checkSchemasMatch(const char *zTab){
  sqlite3_stmt *pStmt = db_prepare(
      "SELECT A.sql=B.sql FROM main.sqlite_master A, aux.sqlite_master B"
      " WHERE A.name=%Q AND B.name=%Q", zTab, zTab
  );
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    if( sqlite3_column_int(pStmt,0)==0 ){
      runtimeError("schema changes for table %s", safeId(zTab));
    }
  }else{
    runtimeError("table %s missing from one or both databases", safeId(zTab));
  }
  sqlite3_finalize(pStmt);
}
/*
** Write a 64-bit signed integer as a varint onto out
*/
static void putsVarint(FILE *out, sqlite3_uint64 v){
  int i, n;
  unsigned char p[12];
  if( v & (((sqlite3_uint64)0xff000000)<<32) ){
    p[8] = (unsigned char)v;
    v >>= 8;
    for(i=7; i>=0; i--){
      p[i] = (unsigned char)((v & 0x7f) | 0x80);
      v >>= 7;
    }
    fwrite(p, 8, 1, out);
  }else{
    n = 9;
    do{
      p[n--] = (unsigned char)((v & 0x7f) | 0x80);
      v >>= 7;
    }while( v!=0 );
    p[9] &= 0x7f;
    fwrite(p+n+1, 9-n, 1, out);
  }
}

/*
** Write an SQLite value onto out.
*/
static void putValue(FILE *out, sqlite3_value *pVal){
  int iDType = sqlite3_value_type(pVal);
  sqlite3_int64 iX;
  double rX;
  sqlite3_uint64 uX;
  int j;

  putc(iDType, out);
  switch( iDType ){
    case SQLITE_INTEGER:
      iX = sqlite3_value_int64(pVal);
      memcpy(&uX, &iX, 8);
      for(j=56; j>=0; j-=8) putc((uX>>j)&0xff, out);
      break;
    case SQLITE_FLOAT:
      rX = sqlite3_value_double(pVal);
      memcpy(&uX, &rX, 8);
      for(j=56; j>=0; j-=8) putc((uX>>j)&0xff, out);
      break;
    case SQLITE_TEXT:
      iX = sqlite3_value_bytes(pVal);
      putsVarint(out, (sqlite3_uint64)iX);
      fwrite(sqlite3_value_text(pVal),1,(size_t)iX,out);
      break;
    case SQLITE_BLOB:
      iX = sqlite3_value_bytes(pVal);
      putsVarint(out, (sqlite3_uint64)iX);
      fwrite(sqlite3_value_blob(pVal),1,(size_t)iX,out);
      break;
    case SQLITE_NULL:
      break;
  }
}

/*
** Generate a CHANGESET for all differences from main.zTab to aux.zTab.
*/
static void changeset_one_table(const char *zTab, FILE *out){
  sqlite3_stmt *pStmt;          /* SQL statment */
  char *zId = safeId(zTab);     /* Escaped name of the table */
  char **azCol = 0;             /* List of escaped column names */
  int nCol = 0;                 /* Number of columns */
  int *aiFlg = 0;               /* 0 if column is not part of PK */
  int *aiPk = 0;                /* Column numbers for each PK column */
  int nPk = 0;                  /* Number of PRIMARY KEY columns */
  Str sql;                      /* SQL for the diff query */
  int i, k;                     /* Loop counters */
  const char *zSep;             /* List separator */

  /* Check that the schemas of the two tables match. Exit early otherwise. */
  checkSchemasMatch(zTab);

  pStmt = db_prepare("PRAGMA main.table_info=%Q", zTab);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    nCol++;
    azCol = sqlite3_realloc(azCol, sizeof(char*)*nCol);
    if( azCol==0 ) runtimeError("out of memory");
    aiFlg = sqlite3_realloc(aiFlg, sizeof(int)*nCol);
    if( aiFlg==0 ) runtimeError("out of memory");
    azCol[nCol-1] = safeId((const char*)sqlite3_column_text(pStmt,1));
    aiFlg[nCol-1] = i = sqlite3_column_int(pStmt,5);
    if( i>0 ){
      if( i>nPk ){
        nPk = i;
        aiPk = sqlite3_realloc(aiPk, sizeof(int)*nPk);
        if( aiPk==0 ) runtimeError("out of memory");
      }
      aiPk[i-1] = nCol-1;
    }
  }
  sqlite3_finalize(pStmt);
  if( nPk==0 ) goto end_changeset_one_table; 
  strInit(&sql);
  if( nCol>nPk ){
    strPrintf(&sql, "SELECT %d", SQLITE_UPDATE);
    for(i=0; i<nCol; i++){
      if( aiFlg[i] ){
        strPrintf(&sql, ",\n       A.%s", azCol[i]);
      }else{
        strPrintf(&sql, ",\n       A.%s IS NOT B.%s, A.%s, B.%s",
                  azCol[i], azCol[i], azCol[i], azCol[i]);
      }
    }
    strPrintf(&sql,"\n  FROM main.%s A, aux.%s B\n", zId, zId);
    zSep = " WHERE";
    for(i=0; i<nPk; i++){
      strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
      zSep = " AND";
    }
    zSep = "\n   AND (";
    for(i=0; i<nCol; i++){
      if( aiFlg[i] ) continue;
      strPrintf(&sql, "%sA.%s IS NOT B.%s", zSep, azCol[i], azCol[i]);
      zSep = " OR\n        ";
    }
    strPrintf(&sql,")\n UNION ALL\n");
  }
  strPrintf(&sql, "SELECT %d", SQLITE_DELETE);
  for(i=0; i<nCol; i++){
    if( aiFlg[i] ){
      strPrintf(&sql, ",\n       A.%s", azCol[i]);
    }else{
      strPrintf(&sql, ",\n       1, A.%s, NULL", azCol[i]);
    }
  }
  strPrintf(&sql, "\n  FROM main.%s A\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM aux.%s B\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n UNION ALL\n");
  strPrintf(&sql, "SELECT %d", SQLITE_INSERT);
  for(i=0; i<nCol; i++){
    if( aiFlg[i] ){
      strPrintf(&sql, ",\n       B.%s", azCol[i]);
    }else{
      strPrintf(&sql, ",\n       1, NULL, B.%s", azCol[i]);
    }
  }
  strPrintf(&sql, "\n  FROM aux.%s B\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM main.%s A\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n");
  strPrintf(&sql, " ORDER BY");
  zSep = " ";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s %d", zSep, aiPk[i]+2);
    zSep = ",";
  }
  strPrintf(&sql, ";\n");

  if( g.fDebug ){ 
    printf("SQL for %s:\n%s\n", zId, sql.z);
    //goto end_changeset_one_table;
  }

  putc('T', out);
  putsVarint(out, (sqlite3_uint64)nCol);
  for(i=0; i<nCol; i++) putc(aiFlg[i]!=0, out);
  fwrite(zTab, 1, strlen(zTab), out);
  putc(0, out);

  pStmt = db_prepare("%s", sql.z);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    int iType = sqlite3_column_int(pStmt,0);
    putc(iType, out);
    putc(0, out);
    switch( sqlite3_column_int(pStmt,0) ){
      case SQLITE_UPDATE: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else if( sqlite3_column_int(pStmt,k) ){
            putValue(out, sqlite3_column_value(pStmt,k+1));
            k += 3;
          }else{
            putc(0, out);
            k += 3;
          }
        }
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putc(0, out);
            k++;
          }else if( sqlite3_column_int(pStmt,k) ){
            putValue(out, sqlite3_column_value(pStmt,k+2));
            k += 3;
          }else{
            putc(0, out);
            k += 3;
          }
        }
        break;
      }
      case SQLITE_INSERT: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else{
            putValue(out, sqlite3_column_value(pStmt,k+2));
            k += 3;
          }
        }
        break;
      }
      case SQLITE_DELETE: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else{
            putValue(out, sqlite3_column_value(pStmt,k+1));
            k += 3;
          }
        }
        break;
      }
    }
  }
  sqlite3_finalize(pStmt);
  
end_changeset_one_table:
  while( nCol>0 ) sqlite3_free(azCol[--nCol]);
  sqlite3_free(azCol);
  sqlite3_free(aiPk);
  sqlite3_free(zId);
}

int sqlitediff_diff_prepared(
  sqlite3 *db,
  const char* zTab, /* name of table to diff, or NULL for all tables */
  FILE* out     /* Output stream */
) {
  sqlite3_stmt *pStmt;

  g.db = db;

  if( zTab ){
    changeset_one_table(zTab, out);
  }else{
    /* Handle tables one by one */
    pStmt = db_prepare(
      "SELECT name FROM main.sqlite_master\n"
      " WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%%'\n"
      " UNION\n"
      "SELECT name FROM aux.sqlite_master\n"
      " WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%%'\n"
      " ORDER BY name"
    );

    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      changeset_one_table((const char*)sqlite3_column_text(pStmt,0), out);
    }
    sqlite3_finalize(pStmt);
  }

  return 0;
}

int sqlitediff_diff(const char* zDb1, const char* zDb2, const char* zTab, FILE* out){
  int i;
  int rc;
  char *zErrMsg = 0;
  char *zSql;

  sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);

  g.fDebug = 0;

  rc = sqlite3_open(zDb1, &g.db);
  if( rc ){
    return runtimeError("cannot open database file \"%s\"", zDb1);
  }
  rc = sqlite3_exec(g.db, "SELECT * FROM sqlite_master", 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    return runtimeError("\"%s\" does not appear to be a valid SQLite database", zDb1);
  }

  zSql = sqlite3_mprintf("ATTACH %Q as aux;", zDb2);
  rc = sqlite3_exec(g.db, zSql, 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    return runtimeError("cannot attach database \"%s\"", zDb2);
  }
  rc = sqlite3_exec(g.db, "SELECT * FROM aux.sqlite_master", 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    return runtimeError("\"%s\" does not appear to be a valid SQLite database", zDb2);
  }

  rc = sqlitediff_diff_prepared(g.db, zTab, out);

  /* TBD: Handle trigger differences */
  /* TBD: Handle view differences */
  sqlite3_close(g.db);
  return rc;
}

int sqlitediff_diff_file(
  const char* zDb1,
  const char* zDb2,
  const char* zTab,
  const char* out
) {
  FILE* fp = fopen(out, "wb");
  sqlitediff_diff(zDb1, zDb2, zTab, fp);
  fclose(fp);
}
