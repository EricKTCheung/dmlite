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
#include "utils/DomeUtils.h"
#include "DomeStatus.h"
#include "DomeLog.h"
#include "inode.h"
#include "utils/urls.h"

#include "boost/thread.hpp"

using namespace dmlite;


DomeMySql::DomeMySql() {
  this->transactionLevel_ = 0;
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
  int cnt = 0;
  try {
    Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
    char buf1[1024], buf2[1024], buf3[1024], buf4[1024], buf5[1024];

    // Get it from the DB, using path and poolname as keys
    Statement stmt(conn_, "dpm_db",
                   "SELECT rowid, u_token, t_space, poolname, path, s_token, groups\
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

    memset(buf4, 0, sizeof(buf4));
    stmt.bindResult(5, buf4, 256);

    memset(buf5, 0, sizeof(buf4));
    stmt.bindResult(6, buf5, 256);

    while ( stmt.fetch() ) {
      qtk.u_token = buf1;
      qtk.path = buf2;
      qtk.poolname = buf3;
      qtk.s_token = buf4;
      qtk.groupsforwrite = DomeUtils::split(std::string(buf5), ",");

      Log(Logger::Lvl2, domelogmask, domelogname, " Fetched quotatoken. rowid:" << qtk.rowid << " s_token:" << qtk.s_token <<
        " u_token:" << qtk.u_token << " t_space:" << qtk.t_space << " poolname: '" << qtk.poolname << "' path:" << qtk.path <<
        " groups: '" << qtk.getGroupsString() << "'"
      );

      cnt++;
    }
  }
  catch ( ... ) {}

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}

int DomeMySql::getSpacesQuotas(DomeStatus &st)
{
  int cnt = 0;
  try {
    Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");

    Statement stmt(conn_, "dpm_db",
                   "SELECT rowid, u_token, t_space, poolname, path, s_token, groups, s_uid, s_gid\
                    FROM dpm_space_reserv"
    );
    stmt.execute();

    DomeQuotatoken qt;
    char buf1[1024], buf2[1024], buf3[1024], buf4[1024], buf5[1024];

    stmt.bindResult(0, &qt.rowid);

    memset(buf1, 0, sizeof(buf1));
    stmt.bindResult(1, buf1, 256);

    stmt.bindResult(2, &qt.t_space);

    memset(buf3, 0, sizeof(buf3));
    stmt.bindResult(3, buf3, 16);

    memset(buf2, 0, sizeof(buf2));
    stmt.bindResult(4, buf2, 256);

    memset(buf4, 0, sizeof(buf4));
    stmt.bindResult(5, buf4, 256);

    memset(buf5, 0, sizeof(buf5));
    stmt.bindResult(6, buf5, 256);

    stmt.bindResult(7, &qt.s_uid);

    stmt.bindResult(8, &qt.s_gid);

    std::vector<DomeQuotatoken> tokens;
    while ( stmt.fetch() ) {
      boost::unique_lock<boost::recursive_mutex> l(st);

      qt.u_token = buf1;
      qt.path = buf2;
      qt.poolname = buf3;
      qt.s_token = buf4;

      // parse the groups into a vector
      qt.groupsforwrite = DomeUtils::split(std::string(buf5), ",");

      Log(Logger::Lvl2, domelogmask, domelogname, " Fetched quotatoken. rowid:" << qt.rowid << " s_token:" << qt.s_token <<
        " u_token:" << qt.u_token << " t_space:" << qt.t_space << " poolname: '" << qt.poolname <<
        "' groupsforwrite(" << qt.groupsforwrite.size() << ") : '" << buf5 <<
        "'  path:" << qt.path);

      tokens.push_back(qt);
      cnt++;
    }
    st.updateQuotatokens(tokens);
  }
  catch ( ... ) {}

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}

int DomeMySql::getGroups(DomeStatus &st)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  int cnt = 0;

  try {
    Statement stmt(conn_, "cns_db",
                   "SELECT gid, groupname, banned, xattr\
                    FROM Cns_groupinfo"
    );
    stmt.execute();

    DomeGroupInfo gi;
    char buf1[1024], buf2[1024];
    int banned;

    stmt.bindResult(0, &gi.groupid);

    memset(buf1, 0, sizeof(buf1));
    stmt.bindResult(1, buf1, 256);

    stmt.bindResult(2, &banned);

    memset(buf2, 0, sizeof(buf2));
    stmt.bindResult(3, buf2, 256);

    {
      boost::unique_lock<boost::recursive_mutex> l(st);
      while ( stmt.fetch() ) {
        gi.groupname = buf1;
        gi.xattr = buf2;
        gi.banned = (banned != 0);

        Log(Logger::Lvl2, domelogmask, domelogname, " Fetched group. id:" << gi.groupid <<
        " groupname:" << gi.groupname << " banned:" << gi.banned << " xattr: '" << gi.xattr);

        st.insertGroup(gi);

        cnt++;
      }
    }
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading groups. Groups read:" << cnt);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Groups read:" << cnt);
  return cnt;
}


int DomeMySql::getUsers(DomeStatus &st)
{
  int cnt = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");

  try {
    Statement stmt(conn_, "cns_db",
                   "SELECT userid, username, banned, xattr\
                    FROM Cns_userinfo"
    );
    stmt.execute();

    DomeUserInfo ui;
    char buf1[1024], buf2[1024];
    int banned;

    stmt.bindResult(0, &ui.userid);

    memset(buf1, 0, sizeof(buf1));
    stmt.bindResult(1, buf1, 256);

    stmt.bindResult(2, &banned);

    memset(buf2, 0, sizeof(buf2));
    stmt.bindResult(3, buf2, 256);

    {
    boost::unique_lock<boost::recursive_mutex> l(st);

    while ( stmt.fetch() ) {
      ui.username = buf1;
      ui.xattr = buf2;
      ui.banned = (banned != 0);

      Log(Logger::Lvl2, domelogmask, domelogname, " Fetched user. id:" << ui.userid <<
      " username:" << ui.username << " banned:" << ui.banned << " xattr: '" << ui.xattr);

      st.insertUser(ui);

      cnt++;
    }
    }
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading users. Users read:" << cnt);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Users read:" << cnt);
  return cnt;
}

int DomeMySql::setQuotatokenByStoken(DomeQuotatoken &qtk) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );

  bool ok = false;
  long unsigned int nrows = 0;

  try {
    Statement stmt(conn_, "dpm_db",
                   "UPDATE dpm_space_reserv SET u_token = ?, t_space = ?, groups = ?, "
                   "path = ?, poolname = ? WHERE s_token = ?");

    stmt.bindParam(0, qtk.u_token);
    stmt.bindParam(1, qtk.t_space);
    stmt.bindParam(2, qtk.getGroupsString(true));
    stmt.bindParam(3, qtk.path);
    stmt.bindParam(4, qtk.poolname);
    stmt.bindParam(5, qtk.s_token);

    nrows = stmt.execute();
    ok = (nrows != 0);
  }
  catch (...) {
    ok = false;
  }

  if(!ok) {
    Err( domelogname, "Could not set quotatoken s_token: '" << qtk.s_token << "' u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows );
    return 1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken set. s_token: '" << qtk.s_token << "' u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows; );
  return 0;
}

int DomeMySql::setQuotatoken(DomeQuotatoken &qtk, std::string &clientid) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );

  bool ok = true;
  long unsigned int nrows = 0;

  try {
    // First try updating it. Makes sense just to overwrite only description, space and pool and groups
    Statement stmt(conn_, "dpm_db",
                   "UPDATE dpm_space_reserv SET u_token = ? , t_space = ?, s_token = ?, groups = ? \
                    WHERE path = ? AND poolname = ?");

    stmt.bindParam(0, qtk.u_token);
    stmt.bindParam(1, qtk.t_space);
    stmt.bindParam(2, qtk.s_token);
    stmt.bindParam(3, qtk.getGroupsString(true));
    stmt.bindParam(4, qtk.path);
    stmt.bindParam(5, qtk.poolname);

    // If no rows are affected then we should insert
    if ( (nrows = stmt.execute() == 0) )
      ok = false;

    // If updating the spacetk failed then we try adding a brand new one. In this case we also write the clientid,
    // and we initialize the other fields with default values.
    if (!ok) {
      if (qtk.s_token.size() > 0) {
        // If the client also gave an s_token (usually an uuid) then use it, because likely the
        // client wants to recreate a quotatk for files that already exist in the db
        Statement stmt(conn_, "dpm_db",
                     "INSERT INTO dpm_space_reserv(client_dn, s_uid, s_gid,\
                     ret_policy, ac_latency, s_type, u_token, t_space, g_space, u_space,\
                     poolname, assign_time, expire_time, path, s_token, groups)\
                     VALUES (\
                     ?, 0, 0,\
                     '_', 0, '-', ?, ?, ?, ?,\
                     ?, ?, ?, '0', ?, ?, ?\
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
        stmt.bindParam(9, qtk.s_token);
        stmt.bindParam(10, qtk.getGroupsString(true));

        ok = true;
        nrows = 0;

        if ( (nrows = stmt.execute() == 0) )
          ok = false;

      } else {
        // The client did not specify a s_token. Let the db create one for us
        Statement stmt(conn_, "dpm_db",
          "INSERT INTO dpm_space_reserv(s_token, client_dn, s_uid, s_gid,\
          ret_policy, ac_latency, s_type, u_token, t_space, g_space, u_space,\
          poolname, assign_time, expire_time, groups, path)\
          VALUES (\
          uuid(), ?, 0, 0,\
          '_', 0, '-', ?, ?, ?, ?,\
          ?, ?, ?, ?, ?\
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
        stmt.bindParam(8, qtk.getGroupsString(true));
        stmt.bindParam(9, qtk.path);

        ok = true;
        nrows = 0;
        if ( (nrows = stmt.execute() == 0) )
          ok = false;
      }
    }
  }
  catch (...) {
    ok = false;
  }

  if (!ok) {
    Err( domelogname, "Could not set quotatoken s_token: '" << qtk.s_token << "' u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows );
      return 1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken set. s_token: '" << qtk.s_token << "' u_token: '" << qtk.u_token << "' client_dn: '" << clientid << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' nrows: " << nrows; );

  return 0;
}

int DomeMySql::delQuotatoken(DomeQuotatoken &qtk, std::string &clientid) {
  bool ok = true;
  long unsigned int nrows = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );

  try {
    Statement stmt(conn_, "dpm_db",
                   "DELETE FROM dpm_space_reserv\
                    WHERE path = ? AND poolname = ?");
    stmt.bindParam(0, qtk.path);
    stmt.bindParam(1, qtk.poolname);

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

/// Add/subtract an integer to the u_space of a quota(space)token
/// u_space is the free space for the legacy DPM daemon, to be decremented on write
int DomeMySql::addtoQuotatokenUspace(DomeQuotatoken &qtk, int64_t increment) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
    " poolname: '" << qtk.poolname << "' path: '" << qtk.path );
  bool ok = true;
  long unsigned int nrows = 0;

  try {
    Statement stmt(conn_, "dpm_db",
                   "UPDATE dpm_space_reserv\
                    SET u_space = u_space + ( ? )\
                    WHERE path = ? AND poolname = ?");
    stmt.bindParam(0, increment);
    stmt.bindParam(1, qtk.path);
    stmt.bindParam(2, qtk.poolname);

    // If no rows are affected then we should insert
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }

  if (!ok) {
    Err( domelogname, "Could not update u_space quotatoken from DB. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' increment: " << increment << " nrows: " << nrows );
      return 1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken u_space updated. u_token: '" << qtk.u_token << "' t_space: " << qtk.t_space <<
      " poolname: '" << qtk.poolname << "' path: '" << qtk.path << "' increment: " << increment << " nrows: " << nrows; );

  return 0;
};



/// Add/subtract an integer to the u_space of a quota(space)token
/// u_space is the free space for the legacy DPM daemon, to be decremented on write
int DomeMySql::addtoQuotatokenUspace(std::string &s_token, int64_t increment) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. s_token: '" << s_token << "' increment: " << increment );
  bool ok = true;
  long unsigned int nrows;

  try {
    Statement stmt(conn_, "dpm_db",
                   "UPDATE dpm_space_reserv\
                    SET u_space = u_space + ( ? )\
                    WHERE s_token = ?");

    stmt.bindParam(0, increment);
    stmt.bindParam(1, s_token);

    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }

  if (!ok) {
    Err( domelogname, "Could not update u_space quotatoken from DB. s_token: '" << s_token <<
      "' increment: " << increment << " nrows: " << nrows );
    return 1;
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Quotatoken u_space updated. s_token: '" << s_token <<
  "' increment: " << increment << " nrows: " << nrows );

  return 0;
};

/// Removes a pool and all the related filesystems
int DomeMySql::rmPool(std::string &poolname)  {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << poolname << "'" );
  bool ok = true;
  long unsigned int nrows;

  try {
    // Delete the pool itself
    Statement stmt(conn_, "dpm_db",
                   "DELETE FROM dpm_pool\
                    WHERE poolname = ?");

    stmt.bindParam(0, poolname);
    if ( (nrows = stmt.execute() == 0) )
      ok = false;
  }
  catch ( ... ) { ok = false; }

  if (!ok) {
    Err( domelogname, "Could not delete pool: '" << poolname << "' from DB. Proceeding anyway to delete the filesystems. nrows: " << nrows );
  }

  try {
    // Now remove all the filesystems it had
    Statement stmt(conn_, "dpm_db",
                   "DELETE FROM dpm_fs\
                    WHERE poolname = ?");

    stmt.bindParam(0, poolname);
    ok = true;
    if ( (nrows = stmt.execute() == 0) )
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




// Load all the pools
int DomeMySql::getPools(DomeStatus &st)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  if (st.role != DomeStatus::roleHead) return -1;

  DomePoolInfo pinfo;
  int cnt = 0;

  try {
    Statement stmt(conn_, "dpm_db",
                   "SELECT poolname, defsize, s_type FROM dpm_pool " );
    stmt.execute();

    char bufpoolname[1024];

    memset(bufpoolname, 0, sizeof(bufpoolname));
    stmt.bindResult(0, bufpoolname, 16);

    stmt.bindResult(1, &pinfo.defsize);

    stmt.bindResult(2, &pinfo.stype, 1);

    {
      boost::unique_lock<boost::recursive_mutex> l(st);
      st.poolslist.clear();
      while ( stmt.fetch() ) {

          pinfo.poolname = bufpoolname;

          Log(Logger::Lvl1, domelogmask, domelogname, " Fetched pool: '" << pinfo.poolname << "' defsize: " << pinfo.defsize <<
          " stype: '" << pinfo.stype << "'");

          st.poolslist[bufpoolname] = pinfo;

          cnt++;

      }
    }
  }
  catch ( ... ) {}

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}










int DomeMySql::getFilesystems(DomeStatus &st)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  DomeFsInfo fs;
  int cnt = 0;

  try {
    Statement stmt(conn_, "dpm_db",
                   "SELECT poolname, server, fs, status "
                   "FROM dpm_fs " );
    stmt.execute();

    char bufpoolname[1024], bufserver[1024], buffs[1024];

    memset(bufpoolname, 0, sizeof(bufpoolname));
    stmt.bindResult(0, bufpoolname, 16);

    memset(bufserver, 0, sizeof(bufserver));
    stmt.bindResult(1, bufserver, 64);

    memset(buffs, 0, sizeof(buffs));
    stmt.bindResult(2, buffs, 80);

    stmt.bindResult(3, (int *)&fs.status);



    {
      boost::unique_lock<boost::recursive_mutex> l(st);
      std::vector <DomeFsInfo> newfslist;
      st.servers.clear();

      while ( stmt.fetch() ) {
        DomeFsInfo oldfs;

        fs.poolname = bufpoolname;
        fs.server = bufserver;
        fs.fs = buffs;

        st.servers.insert(fs.server);

        // If the fs was already in memory, keep its status
        if ( st.PfnMatchesAnyFS(fs.server, fs.fs, oldfs) ) {
          fs.activitystatus = oldfs.activitystatus;
          fs.freespace = oldfs.freespace;
          fs.physicalsize = oldfs.physicalsize;
        }

        Log(Logger::Lvl1, domelogmask, domelogname, " Fetched filesystem. server: '" << fs.server <<
        "' fs: '" << fs.fs << "' st: " << fs.status << " pool: '" << fs.poolname << "'");

        newfslist.push_back(fs);

        cnt++;
      }

      st.fslist = newfslist;

    }

  }
  catch ( ... ) {}

  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Elements read:" << cnt);
  return cnt;
}

int DomeMySql::addPool(std::string& poolname, long defsize, char stype) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << poolname <<
  " stype: '" << stype << "'");
  bool ok = true;
  long unsigned int nrows = 0;

  try {
    Statement stmt(conn_, "dpm_db",
                   "INSERT INTO dpm_pool\
                   (poolname, defsize, gc_start_thresh, gc_stop_thresh,\
                   def_lifetime, defpintime, max_lifetime, maxpintime,\
                   fss_policy, gc_policy, mig_policy, rs_policy,\
                   groups, ret_policy, s_type)\
                   VALUES \
                   (?, ?, 0, 0,\
                   604800, 7200, 2592000, 43200,\
                   'maxfreespace', 'lru', 'none', 'fifo',\
                   '0', 'R', ?)");
    stmt.bindParam(0, poolname);
    stmt.bindParam(1, defsize);
    stmt.bindParam(2, std::string(1, stype));

    if ( (nrows = stmt.execute() == 0) )
      ok = false;

    }
    catch(...) { ok = false; }

    if (!ok) {
      Log(Logger::Lvl4, domelogmask, domelogname, "Could not insert new pool: '" << poolname << "' It likely already exists. nrows: " << nrows );
      Log(Logger::Lvl1, domelogmask, domelogname, "Trying to modify pool: '" << poolname << "'" );
      ok = true;

      try {
        Statement stmt(conn_, "dpm_db",
                     "UPDATE dpm_pool SET\
                     defsize=?, s_type=? WHERE poolname=?");


        stmt.bindParam(0, defsize);
        stmt.bindParam(1, stype);
        stmt.bindParam(2, poolname);

        if ( (nrows = stmt.execute()) == 0 )
          ok = false;
      }
      catch(...) { ok = false; }

    }


  if (!ok) {
    Err(domelogname, "Could not insert or modify pool: '" << poolname << "' nrows:" << nrows );
    return 1;
  }

  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << poolname << "'" );
  return 0;
}

int DomeMySql::addFs(DomeFsInfo &newfs) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << newfs.poolname << "'" );
  long unsigned int nrows = 0;
  bool ok = true;

  try {

    // Now insert the filesystem, using poolname
    Statement stmt(conn_, "dpm_db",
                   "INSERT INTO dpm_fs\
                   (poolname, server, fs, status, weight)\
                   VALUES \
                   (?, ?, ?, 0, 1)");

    stmt.bindParam(0, newfs.poolname);
    stmt.bindParam(1, newfs.server);
    stmt.bindParam(2, newfs.fs);

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

int DomeMySql::modifyFs(DomeFsInfo &newfs) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. poolname: '" << newfs.poolname << "'" );
  long unsigned int nrows = 0;
  bool ok = true;

  try {

    // Now insert the filesystem, using poolname
    Statement stmt(conn_, "dpm_db",
                   "UPDATE dpm_fs\
                    SET poolname=?, status=? WHERE server=? AND fs=?");

    stmt.bindParam(0, newfs.poolname);
    stmt.bindParam(1, newfs.status);
    stmt.bindParam(2, newfs.server);
    stmt.bindParam(3, newfs.fs);

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
  bool ok = true;
  long unsigned int nrows;

  try {
    Statement stmt(conn_, "dpm_db",
                   "DELETE FROM dpm_fs\
                    WHERE server = ? AND fs = ?");
    stmt.bindParam(0, server);
    stmt.bindParam(1, fs);

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


DmStatus DomeMySql::getStatbyLFN(dmlite::ExtendedStat &meta, std::string path, bool followSym) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. lfn: '" << path << "'" );


  // Split the path always assuming absolute
  std::vector<std::string> components = Url::splitPath(path);

  // Iterate starting from absolute root (parent of /) (0)
  uint64_t     parent       = 0;
  unsigned     symLinkLevel = 0;
  std::string  c;

  meta = ExtendedStat(); // ensure it's clean

  // We process only absolute paths here
  if (path[0] != '/' ) {

    // return an error!
    return DmStatus(ENOTDIR, "'" + meta.name + "' is not an absolute path.");
  }

  DmStatus st = getStatbyParentFileid(meta, 0, "/");
  if(!st.ok()) return st;

  parent = meta.stat.st_ino;
  components.erase(components.begin());

  for (unsigned i = 0; i < components.size(); ) {
    // Check that the parent is a directory first
    if (!S_ISDIR(meta.stat.st_mode) && !S_ISLNK(meta.stat.st_mode))
      return DmStatus(ENOTDIR, "'" + meta.name + "' is not a directory, and is referenced by '" + path + "'. Internal DB error.");

    // Pop next component
    c = components[i];

    // Stay here
    if (c == ".") {
      // Nothing
    }
    // Up one level
    else if (c == "..") {
      parent = meta.parent;
      DmStatus st = getStatbyFileid(meta, parent);
      if(!st.ok()) return st;
    }
    // Regular entry
    else {
      // Stat, but capture ENOENT to improve error code
      DmStatus st = getStatbyParentFileid(meta, parent, c);
      if(!st.ok()) {
        if(st.code() != ENOENT) return st;

        while (i < components.size()) {
          components.pop_back();
        }

        //re-add / at the beginning
        components.insert( components.begin(),std::string(1,'/'));
        return DmStatus(ENOENT, "Entry '%s' not found under '%s'",
                        c.c_str(), Url::joinPath(components).c_str());
      }

      // Symbolic link!, follow that instead
      if (S_ISLNK(meta.stat.st_mode) && followSym) {
        SymLink link;
        DmStatus res = readLink(link, meta.stat.st_ino);
        if (res.code() != DMLITE_SUCCESS)
          return res;

        ++symLinkLevel;
        if (symLinkLevel > 16) {
          return DmStatus(DMLITE_SYSERR(ELOOP),
                          "Symbolic links limit exceeded: > %d",
                          16);
        }

        // We have the symbolic link now. Split it and push
        // into the component
        std::vector<std::string> symPath = Url::splitPath(link.link);

        for (unsigned j = i + 1; j < components.size(); ++j)
          symPath.push_back(components[j]);

        components.swap(symPath);
        i = 0;

        // If absolute, need to reset parent
        if (link.link[0] == '/') {
          parent = 0;
          meta.stat.st_mode = S_IFDIR | 0555;
        }
        // If not, meta has the symlink data, which isn't nice.
        // Stat the parent again!
        else {
          DmStatus st = getStatbyFileid(meta, meta.parent);
          if(!st.ok()) return st;
        }

        continue; // Jump directly to the beginning of the loop
      }
      // Next one!
      else {
        parent = meta.stat.st_ino;
      }
    }
    ++i; // Next in array
  }

  checksums::fillChecksumInXattr(meta);

  Log(Logger::Lvl3, domelogmask, domelogname, "Stat-ed '" << path << "' sz: " << meta.stat.st_size);
  return DmStatus();

}




/// Struct used internally to bind when reading
struct CStat {
  ino_t       parent;
  struct stat stat;
  char        status;
  short       type;
  char        name[256];
  char        guid[37];
  char        csumtype[4];
  char        csumvalue[34];
  char        acl[300 * 13]; // Maximum 300 entries of 13 bytes each
  char        xattr[1024];
};


/// Takes the content of a CStat structure, as it comes from the queries
/// and use it to fill the final ExtendedStat object
/// We also take some corrective action on checksums, to make sure that
/// the legacy chksum fields and the xattrs are somehow coherent, or at least
/// not half empty
static inline void dumpCStat(const CStat& cstat, ExtendedStat* xstat)
{

  xstat->clear();
  Log(Logger::Lvl4, domelogmask, domelogname,
      " name: " << cstat.name <<
      " parent: " << cstat.parent <<
      " csumtype: " << cstat.csumtype <<
      " csumvalue: " << cstat.csumvalue <<
      " acl: " << cstat.acl);

  xstat->stat      = cstat.stat;
  xstat->csumtype  = cstat.csumtype;
  xstat->csumvalue = cstat.csumvalue;
  xstat->guid      = cstat.guid;
  xstat->name      = cstat.name;
  xstat->parent    = cstat.parent;
  xstat->status    = static_cast<ExtendedStat::FileStatus>(cstat.status);
  xstat->acl       = Acl(cstat.acl);
  xstat->clear();
  xstat->deserialize(cstat.xattr);

  // From LCGDM-1742
  xstat->fixchecksums();

  (*xstat)["type"] = cstat.type;
}


static inline void bindMetadata(Statement& stmt, CStat* meta) throw(DmException)
{
  memset(meta, 0, sizeof(CStat));
  stmt.bindResult( 0, &meta->stat.st_ino);
  stmt.bindResult( 1, &meta->parent);
  stmt.bindResult( 2, meta->guid, sizeof(meta->guid));
  stmt.bindResult( 3, meta->name, sizeof(meta->name));
  stmt.bindResult( 4, &meta->stat.st_mode);
  stmt.bindResult( 5, &meta->stat.st_nlink);
  stmt.bindResult( 6, &meta->stat.st_uid);
  stmt.bindResult( 7, &meta->stat.st_gid);
  stmt.bindResult( 8, &meta->stat.st_size);
  stmt.bindResult( 9, &meta->stat.st_atime);
  stmt.bindResult(10, &meta->stat.st_mtime);
  stmt.bindResult(11, &meta->stat.st_ctime);
  stmt.bindResult(12, &meta->type);
  stmt.bindResult(13, &meta->status, 1);
  stmt.bindResult(14, meta->csumtype,  sizeof(meta->csumtype));
  stmt.bindResult(15, meta->csumvalue, sizeof(meta->csumvalue));
  stmt.bindResult(16, meta->acl, sizeof(meta->acl), 0);
  stmt.bindResult(17, meta->xattr, sizeof(meta->xattr));
}





dmlite::DmStatus DomeMySql::readLink(SymLink link, int64_t fileid)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " fileid:" << fileid);

  try {
    Statement stmt(conn_, "cns_db",
                   "SELECT fileid, linkname FROM Cns_symlinks WHERE fileid = ?");

    char      clink[4096];

    memset(clink, 0, sizeof(clink));
    stmt.bindParam(0, fileid);
    stmt.execute();

    stmt.bindResult(0, &link.inode);
    stmt.bindResult(1, clink, sizeof(clink), 0);

    if (!stmt.fetch())
      return DmStatus(ENOENT, "Link %ld not found", fileid);

    link.link = clink;

    }
    catch ( ... ) {
      Err(domelogname, " Exception while reading symlink " << fileid);
      return DmStatus(ENOENT, "Cannot fetch link %ld", fileid);
    }
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. fileid:" << fileid << " --> " << link.link);
  return DmStatus();
}





/// Extended stat for inodes
DmStatus DomeMySql::getStatbyFileid(dmlite::ExtendedStat& xstat, int64_t fileid) {
  Log(Logger::Lvl4, domelogmask, domelogname, " fileid:" << fileid);
  try {
    Statement    stmt(conn_, "cns_db",
                    "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
                    filesize, atime, mtime, ctime, fileclass, status,\
                    csumtype, csumvalue, acl, xattr\
                    FROM Cns_file_metadata\
                    WHERE fileid = ?");
    CStat        cstat;
    xstat = ExtendedStat();

    stmt.bindParam(0, fileid);
    stmt.execute();

    bindMetadata(stmt, &cstat);

  if (!stmt.fetch())
    return DmStatus(ENOENT, fileid + " not found");

  dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of fileid " << fileid);
    return DmStatus(EINVAL, " Exception while reading stat of fileid " + fileid);
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. fileid:" << fileid << " name:" << xstat.name << " sz:" << xstat.size());
  return DmStatus();
}




// Extended stat for replica file names in rfio syntax
DmStatus DomeMySql::getStatbyRFN(dmlite::ExtendedStat &xstat, std::string rfn) {
  Log(Logger::Lvl4, domelogmask, domelogname, " rfn:" << rfn);
  try {
    Statement    stmt(conn_, "cns_db",
                      "SELECT m.fileid, m.parent_fileid, m.guid, m.name, m.filemode, m.nlink, m.owner_uid, m.gid,\
                      m.filesize, m.atime, m.mtime, m.ctime, m.fileclass, m.status,\
                      m.csumtype, m.csumvalue, m.acl, m.xattr\
                      FROM Cns_file_metadata m, Cns_file_replica r\
                      WHERE r.sfn = ? AND r.fileid = m.fileid");
    CStat        cstat;
    xstat = ExtendedStat();

    stmt.bindParam(0, rfn);
    stmt.execute();

    bindMetadata(stmt, &cstat);

    if (!stmt.fetch())
      return DmStatus(ENOENT, "replica '" + rfn + "' not found");

    dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of rfn " << rfn);
    return DmStatus(EINVAL, " Exception while reading stat of rfn " + rfn);
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. rfn:" << rfn << " name:" << xstat.name << " sz:" << xstat.stat.st_size);
  return DmStatus();
}


// Extended stat for replica file names in rfio syntax
DmStatus DomeMySql::getReplicabyRFN(dmlite::Replica &r, std::string rfn) {
  Log(Logger::Lvl4, domelogmask, domelogname, " rfn:" << rfn);
  try {
    Statement    stmt(conn_, "cns_db",
                      "SELECT rowid, fileid, nbaccesses,\
                      atime, ptime, ltime,\
                      status, f_type, setname, poolname, host, fs, sfn, COALESCE(xattr, '')\
                      FROM Cns_file_replica\
                      WHERE sfn = ?");


    stmt.bindParam(0, rfn);

    stmt.execute();

    r = Replica();

    char setnm[512];
    char cpool[512];
    char cserver[512];
    char cfilesystem[512];
    char crfn[4096];
    char cmeta[4096];
    char ctype, cstatus;

    stmt.bindResult( 0, &r.replicaid);
    stmt.bindResult( 1, &r.fileid);
    stmt.bindResult( 2, &r.nbaccesses);
    stmt.bindResult( 3, &r.atime);
    stmt.bindResult( 4, &r.ptime);
    stmt.bindResult( 5, &r.ltime);
    stmt.bindResult( 6, &cstatus, 1);
    stmt.bindResult( 7, &ctype, 1);
    stmt.bindResult( 8, setnm,       sizeof(setnm));
    stmt.bindResult( 9, cpool,       sizeof(cpool));
    stmt.bindResult(10, cserver,     sizeof(cserver));
    stmt.bindResult(11, cfilesystem, sizeof(cfilesystem));
    stmt.bindResult(12, crfn,        sizeof(crfn));
    stmt.bindResult(13, cmeta,       sizeof(cmeta));

    if (!stmt.fetch())
      return DmStatus(DMLITE_NO_SUCH_REPLICA, "Replica %s not found", rfn.c_str());

    r.rfn           = crfn;
    r.server        = cserver;
    r.setname       = std::string(setnm);
    r.status        = static_cast<Replica::ReplicaStatus>(cstatus);
    r.type          = static_cast<Replica::ReplicaType>(ctype);
    r.deserialize(cmeta);

    r["pool"]       = std::string(cpool);
    r["filesystem"] = std::string(cfilesystem);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of rfn " << rfn);
    return DmStatus(EINVAL, " Exception while reading stat of rfn " + rfn);
  }

  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. repl:" << r.rfn);
  return DmStatus();
}




/// Extended stat for inodes
DmStatus DomeMySql::getStatbyParentFileid(dmlite::ExtendedStat& xstat, int64_t fileid, std::string name) {
  Log(Logger::Lvl4, domelogmask, domelogname, " parent_fileid:" << fileid << " name: '" << name << "'");
  try {
    Statement    stmt(conn_, "cns_db",
                      "SELECT fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid,\
                      filesize, atime, mtime, ctime, fileclass, status,\
                      csumtype, csumvalue, acl, xattr\
                      FROM Cns_file_metadata\
                      WHERE parent_fileid = ? AND name = ?");
    CStat        cstat;
    xstat = ExtendedStat();

    stmt.bindParam(0, fileid);
    stmt.bindParam(1, name);
    stmt.execute();

    bindMetadata(stmt, &cstat);

    if (!stmt.fetch())
      return DmStatus(ENOENT, SSTR(fileid << " not found"));

    dumpCStat(cstat, &xstat);
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading stat of parent_fileid " << fileid);
    return DmStatus(ENOENT, SSTR(" Exception while reading stat of parent_fileid " << fileid));
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. parent_fileid:" << fileid << " name:" << name << " sz:" << xstat.size());
  return DmStatus();
}







DmStatus DomeMySql::setSize(std::string lfn, int64_t filesize) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Entering. lfn: '" << lfn << "' size: " << filesize );

  long unsigned int nrows = 0;
  dmlite::ExtendedStat st;

  // Do a stat to get the fileid
  DmStatus ret = getStatbyLFN(st, lfn);
  if (!ret.ok())
    return ret;

  try {
    // Just do the query on the fileid
    Statement stmt(conn_, "cns_db",
                   "UPDATE Cns_file_metadata SET filesize = ?, ctime = UNIX_TIMESTAMP() WHERE fileid = ?");

    stmt.bindParam(0, filesize);
    stmt.bindParam(1, st.stat.st_ino);

    // If no rows are affected then error
    if ( (nrows = stmt.execute() == 0) )
      return DmStatus(EINVAL, SSTR("Cannot set filesize for lfn: '" << lfn << "' fileid: " << st.stat.st_ino << " nrows: " << nrows));

  }
  catch (DmException e) {
    Err(domelogname, " Exception while setting filesize for lfn: '" << lfn << "' fileid: " << st.stat.st_ino << "err: '" << e.what());
    return DmStatus(EINVAL, SSTR(" Exception while setting filesize for lfn: '" << lfn << "' fileid: " << st.stat.st_ino << "err: '" << e.what()));
  }

  return DmStatus();
}
