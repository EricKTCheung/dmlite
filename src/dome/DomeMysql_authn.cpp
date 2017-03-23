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

#define CNS_DB "cns_db"



DmStatus DomeMySql::getGroupbyName(DomeGroupInfo &group, const std::string& groupName)
{
  gid_t     gid;
  int       banned;
  char      groupname[256], meta[1024];
  
  Log(Logger::Lvl4, domelogmask, domelogname, "group:" << groupName);
  
  
  try {
    Statement stmt(conn_, CNS_DB, "SELECT gid, groupname, banned, COALESCE(xattr, '')\
    FROM Cns_groupinfo\
    WHERE groupname = ?");
    
    stmt.bindParam(0, groupName);
    stmt.execute();
    
    stmt.bindResult(0, &gid);
    stmt.bindResult(1, groupname, sizeof(groupname));
    stmt.bindResult(2, &banned);
    stmt.bindResult(3, meta, sizeof(meta));
    
    if (!stmt.fetch()) {
      Err("DomeMySql::getGroup", "Group '" << groupName << "' not found.");
      return DmStatus(DMLITE_NO_SUCH_GROUP, SSTR("Group " << groupName << " not found."));
    }
    
    group.groupname      = groupname;
    group.groupid        = gid;
    group.banned         = (DomeGroupInfo::BannedStatus)banned;
    group.xattr = meta;
  } catch ( ... ) {
    Err(domelogname, SSTR("Exception while reading group '" << groupName << "'"));
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. group:" << groupname << " gid:" << gid);
  
  return DmStatus();
}


DmStatus DomeMySql::getGroupbyGid(DomeGroupInfo &group, gid_t gid)
{
  
  int       banned;
  char      groupname[256], meta[1024];
  
  Log(Logger::Lvl4, domelogmask, domelogname, "gid:" << gid);
  
  try {
    Statement stmt(conn_, CNS_DB, "SELECT gid, groupname, banned, COALESCE(xattr, '')\
    FROM Cns_groupinfo\
    WHERE gid = ?");
    
    stmt.bindParam(0, gid);
    stmt.execute();
    
    stmt.bindResult(0, &gid);
    stmt.bindResult(1, groupname, sizeof(groupname));
    stmt.bindResult(2, &banned);
    stmt.bindResult(3, meta, sizeof(meta));
    
    if (!stmt.fetch())
      return DmStatus(DMLITE_NO_SUCH_GROUP, SSTR("Group gid " << gid << " not found"));
    
    group.groupname      = groupname;
    group.groupid        = gid;
    group.banned         = (DomeGroupInfo::BannedStatus)banned;
    group.xattr = meta;
    
  } catch ( DmException e ) {
    return DmStatus(EINVAL, SSTR("Cannot access group with gid " << gid << " err: " << e.what()));
    
  }
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. group:" << groupname << " gid:" << gid);
  return DmStatus();
}



DmStatus DomeMySql::getUser(DomeUserInfo &user, const std::string& username) {
  char       ca[1024], uname[256], meta[1024];
  int ban;
  
  Log(Logger::Lvl4, domelogmask, domelogname, "usr:" << username);
  
    // Search in the DB
    
    Statement stmt(conn_, CNS_DB, "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
      FROM Cns_userinfo\
      WHERE username = ?");
    
    stmt.bindParam(0, username);
    stmt.execute();
    
    stmt.bindResult(0, &user.userid);
    stmt.bindResult(1, uname, sizeof(uname));
    stmt.bindResult(2, ca, sizeof(ca));
    stmt.bindResult(3, &ban);
    stmt.bindResult(4, meta, sizeof(meta));
    
    if (!stmt.fetch()) {
      Err("DomeMySql::getUser", "User '" << username << "' not found.");
      return DmStatus(DMLITE_NO_SUCH_USER, SSTR("User '" << username << "' not found."));
    }
    
    user.username      = uname;
    user.xattr = meta;
    user.banned = (DomeUserInfo::BannedStatus)ban;
  
  
    Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. usr:" << username << " uid:" << user.userid << " ban:" << ban);
  return DmStatus();
}


DmStatus DomeMySql::getUser(DomeUserInfo &user, int uid) {
  char       ca[1024], uname[256], meta[1024];
  int ban;
  Log(Logger::Lvl4, domelogmask, domelogname, "Userid: " << uid);
  
  // Search in the DB
  
  Statement stmt(conn_, CNS_DB, "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
  FROM Cns_userinfo\
  WHERE userid = ?");
  
  stmt.bindParam(0, uid);
  stmt.execute();
  
  stmt.bindResult(0, &user.userid);
  stmt.bindResult(1, uname, sizeof(uname));
  stmt.bindResult(2, ca, sizeof(ca));
  stmt.bindResult(3, &ban);
  stmt.bindResult(4, meta, sizeof(meta));
  
  if (!stmt.fetch()) {
    Err("DomeMySql::getUser", "Userid '" << uid << "' not found.");
    return DmStatus(DMLITE_NO_SUCH_USER, SSTR("Userid '" << uid << "' not found."));
  }
  
  user.username      = uname;
  user.xattr = meta;
  user.banned = (DomeUserInfo::BannedStatus)ban;
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. usr:" << uname << " uid:" << uid << " ban:" << ban);
  return DmStatus();
}

DmStatus DomeMySql::newUser(DomeUserInfo &user, const std::string& uname)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "usr:" << uname);
  uid_t uid=-1;
  try {
    // Start transaction
    DomeMySqlTrans trans(this);
    
    // Get the last uid, increment and update
    Statement uidStmt(conn_, CNS_DB, "SELECT id FROM Cns_unique_uid FOR UPDATE");
    
    uidStmt.execute();
    uidStmt.bindResult(0, &uid);
    
    // Update the uid
    if (uidStmt.fetch()) {
      Statement updateUidStmt(conn_, CNS_DB, "UPDATE Cns_unique_uid SET id = ?");
      ++uid;
      updateUidStmt.bindParam(0, uid);
      updateUidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertUidStmt(conn_, CNS_DB, "INSERT INTO Cns_unique_uid (id) VALUES (?)");
      uid = 1;
      insertUidStmt.bindParam(0, uid);
      insertUidStmt.execute();
    }
    
    // Insert the user
    Statement userStmt(conn_, CNS_DB, "INSERT INTO Cns_userinfo\
    (userid, username, user_ca, banned)\
    VALUES\
    (?, ?, '', ?)");
    
    userStmt.bindParam(0, uid);
    userStmt.bindParam(1, uname);
    userStmt.bindParam(2, 0);
    
    userStmt.execute();
    trans.Commit();
    
    // Build and return the UserInfo
    
    user.username      = uname;
    user.userid        = uid;
    user.banned        = DomeUserInfo::NoBan;
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot create new user: '" << uname << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting. usr:" << uname << " uid:" << uid);
  return DmStatus();
}


DmStatus DomeMySql::updateUser(const DomeUserInfo& user)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "usr:" << user.username);
  
  try {
    Statement stmt(conn_, CNS_DB, "UPDATE Cns_userinfo\
    SET banned = ?, xattr = ?\
    WHERE username = ?");
    
    stmt.bindParam(0, user.banned);
    stmt.bindParam(1, user.xattr);
    stmt.bindParam(2, user.username);
    
    stmt.execute();
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot update user: '" << user.username << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting. usr:" << user.username << " ban:" << boost::any_cast<bool>(user.banned) );
  return DmStatus();
}


DmStatus DomeMySql::deleteUser(const std::string& userName)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "usr:" << userName);
  
  try {
    
    Statement stmt(conn_, CNS_DB, "DELETE FROM Cns_userinfo\
    WHERE username = ?");
    
    stmt.bindParam(0, userName);
    
    stmt.execute();
    
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot delete user: '" << userName << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting user:" << userName);
  return DmStatus();
}




DmStatus DomeMySql::newGroup(DomeGroupInfo &group, const std::string& gname)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "group:" << gname);
  gid_t gid = -1;
  try {
    // Start transaction
    DomeMySqlTrans trans(this);
      
    // Get the last gid, increment and update
    Statement gidStmt(conn_, CNS_DB, "SELECT id FROM Cns_unique_gid FOR UPDATE");
    
    gidStmt.execute();
    gidStmt.bindResult(0, &gid);
    
    // Update the gid
    if (gidStmt.fetch()) {
      Statement updateGidStmt(conn_, CNS_DB, "UPDATE Cns_unique_gid SET id = ?");
      ++gid;
      updateGidStmt.bindParam(0, gid);
      updateGidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertGidStmt(conn_, CNS_DB, "INSERT INTO Cns_unique_gid (id) VALUES (?)");
      gid = 1;
      insertGidStmt.bindParam(0, gid);
      insertGidStmt.execute();
    }
    
    // Insert the group
    Statement groupStmt(conn_, CNS_DB, "INSERT INTO Cns_groupinfo\
    (gid, groupname, banned)\
    VALUES\
    (?, ?, ?)");
    
    groupStmt.bindParam(0, gid);
    groupStmt.bindParam(1, gname);
    groupStmt.bindParam(2, 0);
    
    groupStmt.execute();
    
    trans.Commit();
    
    
    // Build and return the GroupInfo
    group.groupname      = gname;
    group.groupid        = gid;
    group.banned         = DomeGroupInfo::NoBan;
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot create new group: '" << gname << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting. group: '" << gname << "' gid:" << gid);
  return DmStatus();
}


DmStatus DomeMySql::updateGroup(const DomeGroupInfo& group)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "grp:" << group.groupname);
  
  try {
  Statement stmt(conn_, CNS_DB, "UPDATE Cns_groupinfo\
  SET banned = ?, xattr = ?\
  WHERE groupname = ?");
  
  stmt.bindParam(0, group.banned);
  stmt.bindParam(1, group.xattr);
  stmt.bindParam(2, group.groupname);
  
  stmt.execute();
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot update group: '" << group.groupname << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl1, domelogmask, domelogname, "Exiting. group:" << group.groupname);
  return DmStatus();
}




DmStatus DomeMySql::deleteGroup(const std::string& groupName)
{
  Log(Logger::Lvl4, domelogmask, domelogname, "grp:" << groupName);
  try {
    
    Statement stmt(conn_, CNS_DB, "DELETE FROM Cns_groupinfo\
    WHERE groupname = ?");
    
    stmt.bindParam(0, groupName);
    
    stmt.execute();
  }
  catch (DmException e) {
    return DmStatus(EINVAL, SSTR("Cannot delete group: '" << groupName << "' err: '" << e.what()));
  }
  
  Log(Logger::Lvl2, domelogmask, domelogname, "Exiting. group:" << groupName);
  return DmStatus();
}




DmStatus DomeMySql::getGroupsVec(std::vector<DomeGroupInfo> &groups)
{
  DomeGroupInfo group;
  groups.clear();
  
  Log(Logger::Lvl4, domelogmask, domelogname, "");
  
  try {
    Statement stmt(conn_, CNS_DB, "SELECT gid, groupname, banned, COALESCE(xattr, '')\
    FROM Cns_groupinfo");
    stmt.execute();  
    
    gid_t gid;
    char  gname[256], meta[1024];
    int   banned;
    
    stmt.bindResult(0, &gid);
    stmt.bindResult(1, gname, sizeof(gname));
    stmt.bindResult(2, &banned);
    stmt.bindResult(3, meta, sizeof(meta));
    
    while (stmt.fetch()) {
      group.groupname      = gname;
      group.groupid        = gid;
      group.banned         = (DomeGroupInfo::BannedStatus)banned;
      group.xattr = meta;
      groups.push_back(group);
    }
  }
  catch ( ... ) {
    Err(domelogname, " Exception while reading groups. Groups read:" << groups.size());
    
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. ngroups:" << groups.size());
  return DmStatus();
}


int DomeMySql::getGroups(DomeStatus &st)
{
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  int cnt = 0;
  
  try {
    Statement stmt(conn_, CNS_DB,
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
        gi.banned = (DomeGroupInfo::BannedStatus)banned;
        
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

DmStatus DomeMySql::getUsersVec(std::vector<DomeUserInfo> &users)
{
  
  DomeUserInfo user;
  
  Log(Logger::Lvl4, domelogmask, domelogname, "");
  
  try {
    Statement stmt(conn_, CNS_DB, "SELECT userid, username, user_ca, banned, COALESCE(xattr, '')\
    FROM Cns_userinfo");
    stmt.execute();
    
    uid_t uid;
    char  uname[256], ca[512], meta[1024];
    int   banned;
    
    stmt.bindResult(0, &uid);
    stmt.bindResult(1, uname, sizeof(uname));
    stmt.bindResult(2, ca, sizeof(ca));
    stmt.bindResult(3, &banned);
    stmt.bindResult(4, meta, sizeof(meta));
    
    while (stmt.fetch()) {
      
      user.username      = uname;
      user.userid        = uid;
      user.banned        = (DomeUserInfo::BannedStatus)banned;
      user.ca            = std::string(ca);
      user.xattr = meta;
      
      users.push_back(user);
    }
    
    
  }
  catch ( DmException e ) {
    return DmStatus(EINVAL, SSTR(" Exception while reading users. Users read:" << users.size() << " err: " << e.what()));
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, "Exiting. nusers:" << users.size());
  return DmStatus();
}



int DomeMySql::getUsers(DomeStatus &st)
{
  int cnt = 0;
  Log(Logger::Lvl4, domelogmask, domelogname, " Entering ");
  
  try {
    Statement stmt(conn_, CNS_DB,
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
        ui.banned = (DomeUserInfo::BannedStatus)banned;
        
        Log(Logger::Lvl2, domelogmask, domelogname, " Fetched user. id:" << ui.userid <<
        " username:" << ui.username << " banned:" << ui.banned << " xattr: '" << ui.xattr);
        
        st.insertUser(ui);
        
        cnt++;
      }
    }
  }
  catch ( ... ) {
    Err("DomeMySql::getUsers", " Exception while reading users. Users read:" << cnt);
  }
  
  Log(Logger::Lvl3, domelogmask, domelogname, " Exiting. Users read:" << cnt);
  return cnt;
}

