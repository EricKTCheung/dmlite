/*
 * Copyright 2015 CERN
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */



/** @file   DomeMySQL.cpp
 * @brief  A helper class that wraps DomeMySQL stuff
 * @author Fabrizio Furano
 * @date   Jan 2016
 */

#include "DomeMysql.h"
#include "utils/logger.h"
#include "utils/mysqlpools.h"
#include "utils/MySqlWrapper.h"
#include "DomeStatus.h"
#include "DomeLog.h"

using namespace dmlite;





int DomeMySql::begin()
{
  const char *fname = "DomeMySql::begin";
  Log(Logger::Lvl4, domelogmask, domelogname, "Starting transaction");

  if (!conn_) {
    conn_ = MySqlHolder::getMySqlPool().acquire();
  }

  if (!conn_) {
    Err(fname, "No MySQL connection handle");
    return -1;
  }

  if (this->transactionLevel_ == 0 && mysql_query(this->conn_, "BEGIN") != 0) {
    unsigned int merrno = mysql_errno(this->conn_);
    std::string merror = mysql_error(this->conn_);
    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;
    Err(fname, "Cannot start transaction: " << DMLITE_DBERR(merrno) << " " << merror);
    return -1;
  }
  
  this->transactionLevel_++;
  Log(Logger::Lvl3, domelogmask, fname, "Transaction started");
}



int DomeMySql::commit()
{
  const char *fname = "DomeMySql::commit";
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Commit transaction");

  if (this->transactionLevel_ == 0) {
    Err(fname, "INodeMySql::commit Inconsistent state (Maybe there is a\
 commit without a begin, or a badly handled error sequence.)");
    return -1;
  }

  if (!conn_) {
    Err(fname, "No MySQL connection handle");
    return -1;
  }
  
  this->transactionLevel_--;
  
  if (this->transactionLevel_ == 0) {
    int qret;
    unsigned int merrno = 0;
    std::string merror;

    Log(Logger::Lvl4, domelogmask, domelogname, "Releasing transaction.");
    qret = mysql_query(conn_, "COMMIT");
    if (qret != 0) {
      merrno = mysql_errno(this->conn_);
      merror = mysql_error(this->conn_);
    }
    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;
    if  (qret != 0) {
      Err(fname, "Cannot commit: " << DMLITE_DBERR(merrno) << " " << merror);
      return -1;
    }
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
}



int DomeMySql::rollback()
{
  Log(Logger::Lvl4, domelogmask, domelogname, "");
  
  this->transactionLevel_ = 0;

  if (conn_) {
    int qret;
    unsigned int merrno = 0;
    std::string merror;

    qret = mysql_query(this->conn_, "ROLLBACK");

    if (qret != 0) {
      merrno = mysql_errno(this->conn_);
      merror = mysql_error(this->conn_);
    }

    MySqlHolder::getMySqlPool().release(conn_);
    conn_ = 0;
  
    if (qret != 0) {
      Err(domelogname, "Cannot rollback: " << DMLITE_DBERR(merrno) << " " << merror);
      return -1;
      
    }
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting.");
}

int DomeMySql::getSpacesQuotas(DpmrStatus &st)
{

  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  
  // MySQL statement, we need no transaction here
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  //Statement stmt(conn, this->nsDb_, STMT_GET_FILE_REPLICAS);
  
  // blah do the rest
}
