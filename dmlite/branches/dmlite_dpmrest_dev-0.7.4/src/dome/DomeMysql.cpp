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

#include "boost/thread.hpp"

using namespace dmlite;



DomeMySql::DomeMySql() {
  conn_ = MySqlHolder::getMySqlPool().acquire();
}

DomeMySql::~DomeMySql() {
  MySqlHolder::getMySqlPool().release(conn_);
}



void DomeMySql::configure(std::string host, std::string username, std::string password, int port, int poolsize) {
  
  
  Log(Logger::Lvl4, domelogmask, domelogname, "Configuring MySQL access. host:'" << host <<
  "' user:'" << username <<
  "' port:'" << port <<
  "' poolsz:" << poolsize);;
  
  MySqlHolder::configure(host, username, password, port, poolsize);
  
  
}







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
  
  Statement stmt(conn_, "dpm_db", 
                 "SELECT rowid, u_token, t_space, path\
                 FROM dpm_space_reserv"
  );
  stmt.execute();
  
  DpmrQuotatoken qt;
  char buf1[1024], buf2[1024];
  
  stmt.bindResult(0, &qt.rowid);
  
  memset(buf1, 0, sizeof(buf1));
  stmt.bindResult(1, buf1, 256);
  
  stmt.bindResult(2, &qt.t_space);
  
  memset(buf2, 0, sizeof(buf2));
  stmt.bindResult(3, buf2, 256);
  
  int cnt = 0;
  try {
    bool end = false;
        
    do {
      end = stmt.fetch();
      boost::unique_lock<boost::recursive_mutex> l(st);
      
      
      qt.u_token = buf1;
      qt.path = buf2;
      
      Log(Logger::Lvl1, domelogmask, domelogname, " Fetched quotatoken. rowid:" << qt.rowid <<
      " u_token:" << qt.u_token << " t_space:" << qt.t_space << " path:" << qt.path);
      
      st.quotas[qt.path] = qt;
      
      cnt++;
    } while (!end);
  }
  catch ( ... ) {}
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}





int DomeMySql::getFilesystems(DpmrStatus &st)
{
  
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  DpmrFsInfo fs;
  
  Statement stmt(conn_, "dpm_db", 
                 "SELECT poolname, server, fs, status, weight\
                 FROM dpm_fs"
  );
  stmt.execute();
  
  char bufpoolname[1024], bufserver[1024], buffs[1024];
  int status;
  
  memset(bufpoolname, 0, sizeof(bufpoolname));
  stmt.bindResult(0, bufpoolname, 16);
  
  memset(bufserver, 0, sizeof(bufserver));
  stmt.bindResult(1, bufserver, 64);
  
  memset(buffs, 0, sizeof(buffs));
  stmt.bindResult(2, buffs, 80);
  
  stmt.bindResult(3, & fs.status);
  
  stmt.bindResult(4, & fs.weight);
  
  int cnt = 0;
  try {
    bool end = false;
        
    do {
      end = stmt.fetch();
      boost::unique_lock<boost::recursive_mutex> l(st);
      
      
      fs.poolname = bufpoolname;
      fs.server = bufserver;
      fs.fs = buffs;
      
      Log(Logger::Lvl1, domelogmask, domelogname, " Fetched filesystem. server:" << fs.server <<
      " fs:" << fs.fs << " st:" << fs.status << " weight:" << fs.weight << " pool:" << fs.poolname);
      
      st.fslist.push_back(fs);
      
      cnt++;
    } while (!end);
  }
  catch ( ... ) {}
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}
