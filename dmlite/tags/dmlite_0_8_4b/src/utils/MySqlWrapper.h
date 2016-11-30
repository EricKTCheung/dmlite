/// @file   MySqlWrapper.h
/// @brief  MySQL Wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQLWRAPPER_H
#define	MYSQLWRAPPER_H

#include <dmlite/cpp/exceptions.h>
#include <mysql/mysql.h>
#include <stdint.h>
#include <map>
#include <vector>

#include "utils/logger.h"

namespace dmlite {


extern Logger::bitmask mysqllogmask;
extern Logger::component mysqllogname;

/// Prepared statement wrapper.
class Statement {
public:
  Statement(MYSQL* conn, const std::string& db, const char* query) throw (DmException);
  ~Statement() throw ();

  void bindParam(unsigned index, int64_t) throw (DmException);
  void bindParam(unsigned index, const std::string& value) throw (DmException);
  void bindParam(unsigned index, const char* value, size_t size) throw (DmException);

  unsigned long execute(void) throw (DmException);

  void bindResult(unsigned index, short*              destination) throw (DmException);
  void bindResult(unsigned index, unsigned short*     destination) throw (DmException);
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
  my_bool*      result_null_;
  Step          status_;
  
  /// Throws the proper exception
  void throwException() throw (DmException);
  void zeroNullResults();
};

};

#endif	// MYSQLWRAPPER_H
