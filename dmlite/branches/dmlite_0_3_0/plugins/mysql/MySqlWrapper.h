/// @file    plugins/mysql/MySqlWrapper.h
/// @brief   MySQL Wrapper.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQLWRAPPER_H
#define	MYSQLWRAPPER_H

#include <dmlite/dm_exceptions.h>
#include <mysql/mysql.h>
#include <stdint.h>
#include <map>
#include <vector>

namespace dmlite {

/// Lock and transaction handler
/// Used to avoid manual tracking of exit paths, so any destruction
/// before a commit() call will be a rollback.
class Transaction {
public:
  Transaction(MYSQL* conn) throw (DmException);
  ~Transaction() throw (DmException);

  void commit(void) throw (DmException);
  void rollback(void) throw (DmException);
private:
  bool   pending_;
  MYSQL *connection_;
};

/// Prepared statement wrapper.
class Statement {
public:
  Statement(MYSQL* conn, const std::string& db, const char* query) throw (DmException);
  ~Statement() throw ();

  void bindParam(unsigned index, unsigned long      value) throw (DmException);
  void bindParam(unsigned index, const std::string& value) throw (DmException);
  void bindParam(unsigned index, const char* value, size_t size) throw (DmException);

  unsigned long execute(bool autobind = false) throw (DmException);

  void bindResult(unsigned index, short*              destination) throw (DmException);
  void bindResult(unsigned index, signed int*         destination) throw (DmException);
  void bindResult(unsigned index, unsigned int*       destination) throw (DmException);
  void bindResult(unsigned index, signed long*        destination) throw (DmException);
  void bindResult(unsigned index, unsigned long*      destination) throw (DmException);
  void bindResult(unsigned index, signed long long*   destination) throw (DmException);
  void bindResult(unsigned index, unsigned long long* destination) throw (DmException);
  void bindResult(unsigned index, char*               destination, size_t size) throw (DmException);
  void bindResult(unsigned index, char*               destination, size_t size, int) throw (DmException); // For blobs

  unsigned long count(void) throw ();

  bool fetch(void) throw (DmException);
  
  // When autobind is set an internal allocated memory area is used, so
  // get* can be used
  unsigned    getFieldIndex(const std::string& fieldname);
  int         getInt   (unsigned index);
  std::string getString(unsigned index);
  
protected:
private:
  enum Step {STMT_CREATED, STMT_EXECUTED,
             STMT_RESULTS_UNBOUND, STMT_RESULTS_BOUND,
             STMT_DONE, STMT_FAILED};

  MYSQL_STMT*   stmt_;
  unsigned long nParams_;
  unsigned long nFields_;
  MYSQL_BIND*   params_;
  MYSQL_BIND*   result_;
  Step          status_;
  
  // For autobind
  std::map<std::string, unsigned> fieldIndex_;
  std::vector<void*> fieldBuffer_;
};

};

#endif	// MYSQLWRAPPER_H
