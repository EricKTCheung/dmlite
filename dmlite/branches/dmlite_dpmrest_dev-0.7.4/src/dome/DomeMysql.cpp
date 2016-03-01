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
  
  return 0;
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
  return 0;
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
  return 0;
}



int DomeMySql::getQuotaTokenByKeys(DomeQuotatoken &qtk)
{
  
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  char buf1[1024], buf2[1024], buf3[1024];
  
  // Get it from the DB, using path and poolname as keys
  Statement stmt(conn_, "dpm_db", 
                 "SELECT rowid, u_token, t_space, poolname, path\
                 FROM dpm_space_reserv WHERE path = ? AND poolname = ?"
  );
  
  stmt.bindParam(0, qtk.path);
  stmt.bindParam(1, qtk.poolname);
  stmt.execute();
  
  stmt.bindResult(0, &qtk.rowid);
  
  memset(buf1, 0, sizeof(buf1));
  stmt.bindResult(1, buf1, 256);
  
  stmt.bindResult(2, &qtk.t_space);
  
  memset(buf3, 0, sizeof(buf3));
  stmt.bindResult(3, buf3, 16);
  
  memset(buf2, 0, sizeof(buf2));
  stmt.bindResult(4, buf2, 256);

  int cnt = 0;
  try {
        
    while ( stmt.fetch() ) {
  
      
      qtk.u_token = buf1;
      qtk.path = buf2;
      qtk.poolname = buf3;
      
      Log(Logger::Lvl1, domelogmask, domelogname, " Fetched quotatoken. rowid:" << qtk.rowid <<
      " u_token:" << qtk.u_token << " t_space:" << qtk.t_space << " poolname: '" << qtk.poolname << "' path:" << qtk.path);
      
      cnt++;
    }
  }
  catch ( ... ) {}
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}



int DomeMySql::getSpacesQuotas(DomeStatus &st)
{
  
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  
  Statement stmt(conn_, "dpm_db", 
                 "SELECT rowid, u_token, t_space, poolname, path\
                 FROM dpm_space_reserv"
  );
  stmt.execute();
  
  DomeQuotatoken qt;
  char buf1[1024], buf2[1024], buf3[1024];
  
  stmt.bindResult(0, &qt.rowid);
  
  memset(buf1, 0, sizeof(buf1));
  stmt.bindResult(1, buf1, 256);
  
  stmt.bindResult(2, &qt.t_space);
  
  memset(buf3, 0, sizeof(buf3));
  stmt.bindResult(3, buf3, 16);
  
  memset(buf2, 0, sizeof(buf2));
  stmt.bindResult(4, buf2, 256);
  

  int cnt = 0;
  try {
        
    while ( stmt.fetch() ) {
      boost::unique_lock<boost::recursive_mutex> l(st);
      
      qt.u_token = buf1;
      qt.path = buf2;
      qt.poolname = buf3;
      
      Log(Logger::Lvl1, domelogmask, domelogname, " Fetched quotatoken. rowid:" << qt.rowid <<
      " u_token:" << qt.u_token << " t_space:" << qt.t_space << " poolname: '" << qt.poolname << "' path:" << qt.path);
      
      st.insertQuotatoken(qt);
      
      cnt++;
    }
  }
  catch ( ... ) {}
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}

int DomeMySql::setQuotatoken(DomeQuotatoken &qtk, std::string &clientid) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );
  
  // First try updating it. Makes sense just to overwrite only description, space and pool
  Statement stmt(conn_, "dpm_db", 
                 "UPDATE dpm_space_reserv SET u_token = ? , t_space = ? , g_space = ? , u_space = ?\
                  WHERE path = ? AND poolname = ?");
  
  stmt.bindParam(0, qtk.u_token);
  stmt.bindParam(1, qtk.t_space);
  stmt.bindParam(2, qtk.t_space);
  stmt.bindParam(3, qtk.t_space);
  stmt.bindParam(4, qtk.path);
  stmt.bindParam(5, qtk.poolname);
  
  bool ok = true;
  long unsigned int nrows;
  try {
    // If no rows are affected then we should insert
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  // If updating the spacetk failed then we try adding a brand new one. In this case we also write the clientid,
  // and we initialize the other fields with default values.
  if (!ok) {
    
    Statement stmt(conn_, "dpm_db", 
       "INSERT INTO dpm_space_reserv(s_token, client_dn, s_uid, s_gid,\
        ret_policy, ac_latency, s_type, u_token, t_space, g_space, u_space,\
        poolname, assign_time, expire_time, groups, path)\
        VALUES (\
        uuid(), ?, 0, 0,\
        '_', 0, '-', ?, ?, ?, ?,\
        ?, ?, ?, '', ?\
        )" );
        
    time_t timenow, exptime;
    timenow = time(0);
    exptime = timenow + 86400 * 365 * 50; // yeah, 50 years is a silly enough value for a silly feature
    
    stmt.bindParam(0, clientid);
    stmt.bindParam(1, qtk.u_token);
    stmt.bindParam(2, qtk.t_space);
    stmt.bindParam(3, qtk.t_space);
    stmt.bindParam(4, qtk.t_space);
    stmt.bindParam(5, qtk.poolname);
    stmt.bindParam(6, timenow);
    stmt.bindParam(7, exptime);
    stmt.bindParam(8, qtk.path);
    
    ok = true;
    nrows = 0;
    try {
      // If no rows are affected then we should insert
      if ( (nrows = stmt.execute() == 0) )
        ok = false;
      
    }
    catch ( dmlite::DmException e ) {
      ok = false;
      
    }
  }
  
  if (!ok) {
    Err( domelogname, "Could not set quotatoken u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows );
      return 1;
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken set. u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows; );
  
  return 0;
}



int DomeMySql::delQuotatoken(DomeQuotatoken &qtk, std::string &clientid) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );
  
  // First try updating it. Makes sense just to overwrite only description, space and pool
  Statement stmt(conn_, "dpm_db", 
                 "DELETE FROM dpm_space_reserv\
                  WHERE path = ? AND poolname = ?");
  stmt.bindParam(0, qtk.path);
  stmt.bindParam(1, qtk.poolname);
  
  bool ok = true;
  long unsigned int nrows;
  try {
    // If no rows are affected then we should insert
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  
  if (!ok) {
    Err( domelogname, "Could not delete quotatoken from DB. u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows );
      return 1;
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken deleted. u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows; );
  
  return 0;
}

/// Removes a pool and all the related filesystems
int DomeMySql::rmPool(std::string &poolname)  {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << poolname << "'" );
  
  
  // Delete the pool itself
  Statement stmt(conn_, "dpm_db", 
                 "DELETE FROM dpm_pool\
                  WHERE poolname = ?");
  
  stmt.bindParam(0, poolname);
  
  bool ok = true;
  long unsigned int nrows;
  try {
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  if (!ok) {
    Err( domelogname, "Could not delete pool: '" << poolname << "' from DB. Proceeding anyway to delete the filesystems. nrows: " << nrows );
  }
  
  // Now remove all the filesystems it had
  Statement stmt2(conn_, "dpm_db", 
                 "DELETE FROM dpm_fs\
                  WHERE poolname = ?");
  
  stmt2.bindParam(0, poolname);
  
  ok = true;
  try {
    if ( (nrows = stmt2.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  if (!ok) {
    Err( domelogname, "Could not delete filesystems of pool: '" << poolname << "' from DB." << nrows );
  }
  
  // Let's log this at level 1, as it's particularly destructive
  Log(Logger::Lvl1, domelogmask, domelogname, "Pool '" << poolname << "' removed. Removed filesystems: " << nrows; );
  
  return 0;
}

int DomeMySql::getFilesystems(DomeStatus &st)
{
  
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  DomeFsInfo fs;
  
  Statement stmt(conn_, "dpm_db", 
                 "SELECT poolname, server, fs, status FROM dpm_fs"
  );
  stmt.execute();
  
  char bufpoolname[1024], bufserver[1024], buffs[1024];
  
  memset(bufpoolname, 0, sizeof(bufpoolname));
  stmt.bindResult(0, bufpoolname, 16);
  
  memset(bufserver, 0, sizeof(bufserver));
  stmt.bindResult(1, bufserver, 64);
  
  memset(buffs, 0, sizeof(buffs));
  stmt.bindResult(2, buffs, 80);
  
  stmt.bindResult(3, (int *)&fs.status);
  
  int cnt = 0;
  st.fslist.clear();
  try {
           
    while ( stmt.fetch() ) {
      
      if ( !strcmp(bufserver, st.myhostname.c_str()) || (st.role == DomeStatus::roleHead) ) {
      boost::unique_lock<boost::recursive_mutex> l(st);
      
      
      fs.poolname = bufpoolname;
      fs.server = bufserver;
      st.servers.insert(bufserver);
      fs.fs = buffs;
      
      Log(Logger::Lvl1, domelogmask, domelogname, " Fetched filesystem. server:" << fs.server <<
      " fs:" << fs.fs << " st:" << fs.status << " pool:" << fs.poolname);
      
      st.fslist.push_back(fs);
      
      cnt++;
      }
    }
  }
  catch ( ... ) {}
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}

int DomeMySql::addPool(std::string& poolname) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << poolname << "'" );

  Statement stmt(conn_, "dpm_db", 
                 "INSERT INTO dpm_pool\
                 (poolname, defsize, gc_start_thresh, gc_stop_thresh,\
                 def_lifetime, defpintime, max_lifetime, maxpintime,\
                 fss_policy, gc_policy, mig_policy, rs_policy,\
                 groups, ret_policy, s_type)\
                 VALUES \
                 (?, 104857600, 0, 0,\
                 604800, 7200, 2592000, 43200,\
                 'maxfreespace', 'lru', 'none', 'fifo',\
                 '', 'R', '-')");
  
  stmt.bindParam(0, poolname);

  bool ok = true;
  long unsigned int nrows;
  try {
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  if (!ok) {
    Log(Logger::Lvl4, domelogmask, domelogname, "Could not insert new pool: '" << poolname << "' It likely already exists. nrows: " << nrows );
    return 1;
  }
  return 0;
}

int DomeMySql::addFsPool(DomeFsInfo &newfs) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << newfs.poolname << "'" );
  this->addPool(newfs.poolname);
  
  // Now insert the filesystem, using poolname
  Statement stmt(conn_, "dpm_db", 
                 "INSERT INTO dpm_fs\
                 (poolname, server, fs, status, weight)\
                 VALUES \
                 (?, ?, ?, 0, 0)");
  
  stmt.bindParam(0, newfs.poolname);
  stmt.bindParam(1, newfs.server);
  stmt.bindParam(2, newfs.fs);
  
  long unsigned int nrows;
  bool ok = true;
  try {
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  if (!ok) {
    Err(domelogname, "Could not insert new filesystem: '" << newfs.server << ":" << newfs.fs << "' for pool: '" << newfs.poolname << "' It likely already exists. nrows: " << nrows );
    return 1;
  }
  
  return 0;
}


  
int DomeMySql::rmFs(std::string& server, std::string& fs) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. server: '" << server << "' fs: '" << fs << "'" );
  
  // First try updating it. Makes sense just to overwrite only description, space and pool
  Statement stmt(conn_, "dpm_db", 
                 "DELETE FROM dpm_fs\
                  WHERE server = ? AND fs = ?");
  stmt.bindParam(0, server);
  stmt.bindParam(1, fs);
  
  bool ok = true;
  long unsigned int nrows;
  try {
    // If no rows are affected then we should insert
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }
  
  if (!ok) {
    Err( domelogname, "Failed deleting filesystem '" << fs << "' of server '" << server << "'");
    return 1;
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Deleted filesystem '" << fs << "' of server '" << server << "'");
  
  return 0;
}

