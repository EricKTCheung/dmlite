/// @file    plugins/mysql/MySqlWrapper.cpp
/// @brief   MySQL Wrapper.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "MySqlWrapper.h"

#include <cstring>
#include <cstdlib>

using namespace dmlite;

#define ASSIGN_POINTER_TYPECAST(type, where, value) (*((type*)where)) = value

#define SANITY_CHECK(status, method) \
if (this->status_ != status)\
  throw DmException(DM_INTERNAL_ERROR, #method" called out of order");\

#define BIND_PARAM_SANITY() \
if (this->status_ != STMT_CREATED)\
  throw DmException(DM_INTERNAL_ERROR, "bindParam called out of order");\
if (index > this->nParams_)\
  throw DmException(DM_QUERY_FAILED, "Wrong index in bindParam");

#define BIND_RESULT_SANITY() \
if (this->status_ < STMT_EXECUTED || this->status_ > STMT_RESULTS_BOUND)\
  throw DmException(DM_INTERNAL_ERROR, "bindResult called out of order");\
if (index > this->nResults_)\
  throw DmException(DM_QUERY_FAILED, "Wrong index in bindResult");


Transaction::Transaction(MYSQL* conn) throw (DmException)
{
  this->connection_ = conn;
  this->pending_    = true;

  if (mysql_query(conn, "BEGIN") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(conn));
}



Transaction::~Transaction() throw (DmException)
{
  if (this->pending_)
    this->rollback();
}



void Transaction::commit() throw (DmException)
{
  if (mysql_query(this->connection_, "COMMIT") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->connection_));
  this->pending_ = false;
}



void Transaction::rollback() throw (DmException)
{
  if (mysql_query(this->connection_, "ROLLBACK") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->connection_));
  this->pending_ = false;
}



Statement::Statement(MYSQL_STMT* statement) throw ():
  stmt_(statement), nResults_(0), results_(0x00), status_(STMT_CREATED)
{
  this->nParams_ = mysql_stmt_param_count(statement);
  this->params_  = new MYSQL_BIND [this->nParams_];
  std::memset(this->params_, 0x00, sizeof(MYSQL_BIND) * this->nParams_);
}



Statement::~Statement() throw ()
{
  mysql_stmt_free_result(this->stmt_);
  
  // Free param array
  if (this->params_ != 0x00) {
    for (unsigned long i = 0; i < this->nParams_; ++i) {
      if (this->params_[i].buffer != 0x00)
        std::free(this->params_[i].buffer);
      if (this->params_[i].length != 0x00)
        std::free(this->params_[i].length);
    }
    delete [] this->params_;
  }

  // Free result array
  if (this->results_ != 0x00) {
    delete [] this->results_;
  }
}



void Statement::bindParam(unsigned index, unsigned long value) throw (DmException)
{
  BIND_PARAM_SANITY();
  params_[index].buffer_type   = MYSQL_TYPE_LONG;
  params_[index].buffer        = std::malloc(sizeof(unsigned long));
  params_[index].is_unsigned   = true;
  params_[index].is_null_value = false;
  ASSIGN_POINTER_TYPECAST(unsigned long, params_[index].buffer, value);
}



void Statement::bindParam(unsigned index, const std::string& value) throw (DmException)
{
  BIND_PARAM_SANITY();

  size_t size = value.length();
  params_[index].buffer_type   = MYSQL_TYPE_VARCHAR;
  params_[index].length        = (unsigned long*)std::malloc(sizeof(unsigned long));
  params_[index].buffer        = std::malloc(sizeof(char) * size);
  params_[index].is_null_value = false;

  ASSIGN_POINTER_TYPECAST(unsigned long, params_[index].length, size);
  memcpy(params_[index].buffer, value.c_str(), size);
}



void Statement::bindParam(unsigned index, const char* value, size_t size) throw (DmException)
{
  BIND_PARAM_SANITY();

  params_[index].buffer_type  = MYSQL_TYPE_BLOB;
  params_[index].length_value = size;

  if (value != NULL) {
    params_[index].is_null_value = false;
    params_[index].buffer        = std::malloc(sizeof(char) * size);
    std::memcpy(params_[index].buffer, value, size);
  }
  else {
    params_[index].is_null_value = true;
  }
}



unsigned long Statement::execute(void) throw (DmException)
{
  SANITY_CHECK(STMT_CREATED, execute);

  mysql_stmt_bind_param(this->stmt_, this->params_);

  if (mysql_stmt_execute(this->stmt_) != 0) {
    this->status_ = STMT_FAILED;
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(this->stmt_));
  }

  // Count fields and reserve
  MYSQL_RES* meta = mysql_stmt_result_metadata(this->stmt_);

  if (meta == 0x00) {
    this->status_ = STMT_DONE; // No return set (UPDATE, DELETE, ...)
  }
  else {
    this->nResults_ = mysql_num_fields(meta);
    this->results_  = new MYSQL_BIND[this->nResults_];
    std::memset(this->results_, 0x00, sizeof(MYSQL_BIND) * this->nResults_);

    mysql_free_result(meta);

    this->status_ = STMT_EXECUTED;
  }

  return (unsigned long) mysql_stmt_affected_rows(this->stmt_);
}



void Statement::bindResult(unsigned index, short* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_SHORT;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = false;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, signed int* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = false;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, unsigned int* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = true;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, signed long* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONGLONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = false;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, unsigned long* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONGLONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = true;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, signed long long* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONGLONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = false;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, unsigned long long* destination) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type = MYSQL_TYPE_LONGLONG;
  results_[index].buffer      = destination;
  results_[index].is_unsigned = true;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, char* destination, size_t size) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type   = MYSQL_TYPE_STRING;
  results_[index].buffer        = destination;
  results_[index].buffer_length = size;
  this->status_ = STMT_RESULTS_UNBOUND;
}



void Statement::bindResult(unsigned index, char* destination, size_t size, int) throw (DmException)
{
  BIND_RESULT_SANITY();
  results_[index].buffer_type   = MYSQL_TYPE_BLOB;
  results_[index].buffer        = destination;
  results_[index].buffer_length = size;
  this->status_ = STMT_RESULTS_UNBOUND;
}



unsigned long Statement::count(void) throw ()
{
  if (this->status_ == STMT_RESULTS_UNBOUND) {
    mysql_stmt_bind_result(this->stmt_, this->results_);
    mysql_stmt_store_result(this->stmt_);
    this->status_ = STMT_RESULTS_BOUND;
  }
  SANITY_CHECK(STMT_RESULTS_BOUND, count);
  return mysql_stmt_num_rows(this->stmt_);
}



bool Statement::fetch(void) throw (DmException)
{
  if (this->status_ == STMT_RESULTS_UNBOUND) {
    mysql_stmt_bind_result(this->stmt_, this->results_);
    mysql_stmt_store_result(this->stmt_);
    this->status_ = STMT_RESULTS_BOUND;
  }

  SANITY_CHECK(STMT_RESULTS_BOUND, fetch);

  switch (mysql_stmt_fetch(this->stmt_)) {
    case 0:
      return true;
    case MYSQL_NO_DATA:
      this->status_ = STMT_DONE;
      return false;
    default:
      this->status_ = STMT_FAILED;
      throw DmException(DM_INTERNAL_ERROR, mysql_stmt_error(this->stmt_));
  }
}
