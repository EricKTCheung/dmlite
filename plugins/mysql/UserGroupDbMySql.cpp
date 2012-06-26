/// @file    plugins/mysql/UserGroupDbMySql.cpp
/// @brief   MySQL UserGroupDB Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/common/Security.h>
#include "UserGroupDbMySql.h"
#include "Queries.h"

using namespace dmlite;

UserGroupDbMySql::UserGroupDbMySql(PoolContainer<MYSQL*>* connPool,
                  const std::string& db) throw(DmException):
  nsDb_(db)
{
  this->connectionPool_ = connPool;
  this->conn_ = connPool->acquire();
}



UserGroupDbMySql::~UserGroupDbMySql() throw(DmException)
{
  this->connectionPool_->release(this->conn_);
}



std::string UserGroupDbMySql::getImplId(void) throw()
{
  return std::string("UserGroupDbMySql");
}



SecurityContext* UserGroupDbMySql::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.getClientName(), cred.getFqans(), &user, &groups);
  return new SecurityContext(cred, user, groups);
}



GroupInfo UserGroupDbMySql::newGroup(const std::string& gname) throw (DmException)
{
  if (mysql_query(this->conn_, "BEGIN") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));
  
  
  gid_t gid;
  try {
    // Get the last gid, increment and update
    Statement gidStmt(this->conn_, this->nsDb_, STMT_GET_UNIQ_GID_FOR_UPDATE);

    gidStmt.execute();
    gidStmt.bindResult(0, &gid);

    // Update the gid
    if (gidStmt.fetch()) {
      Statement updateGidStmt(this->conn_, this->nsDb_, STMT_UPDATE_UNIQ_GID);
      ++gid;
      updateGidStmt.bindParam(0, gid);
      updateGidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertGidStmt(this->conn_, this->nsDb_, STMT_INSERT_UNIQ_GID);
      gid = 1;
      insertGidStmt.bindParam(0, gid);
      insertGidStmt.execute();
    }

    // Insert the group
    Statement groupStmt(this->conn_, this->nsDb_, STMT_INSERT_GROUP);

    groupStmt.bindParam(0, gid);
    groupStmt.bindParam(1, gname);
    groupStmt.bindParam(2, 0);

    groupStmt.execute();
  }
  catch (...) {
    mysql_query(this->conn_, "ROLLBACK");
    throw;
  }

  mysql_query(this->conn_, "COMMIT");

  // Build and return the GroupInfo
  GroupInfo g;
  g.gid = gid;
  strncpy(g.name, gname.c_str(), sizeof(g.name));
  g.banned = 0;

  return g;
}



GroupInfo UserGroupDbMySql::getGroup(gid_t gid) throw (DmException)
{
  GroupInfo group;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_GROUPINFO_BY_GID);

  stmt.bindParam(0, gid);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group %ld not found", gid);

  return group;
}



GroupInfo UserGroupDbMySql::getGroup(const std::string& groupName) throw (DmException)
{
  GroupInfo group;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_GROUPINFO_BY_NAME);

  stmt.bindParam(0, groupName);
  stmt.execute();

  stmt.bindResult(0, &group.gid);
  stmt.bindResult(1, group.name, sizeof(group.name));
  stmt.bindResult(2, &group.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_GROUP, "Group " + groupName + " not found");
  
  return group;
}



UserInfo UserGroupDbMySql::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  if (mysql_query(this->conn_, "BEGIN") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));

  uid_t uid;
  
  try {
    // Get the last uid, increment and update
    Statement uidStmt(this->conn_, this->nsDb_, STMT_GET_UNIQ_UID_FOR_UPDATE);

    uidStmt.execute();
    uidStmt.bindResult(0, &uid);

    // Update the uid
    if (uidStmt.fetch()) {
      Statement updateUidStmt(this->conn_, this->nsDb_, STMT_UPDATE_UNIQ_UID);
      ++uid;
      updateUidStmt.bindParam(0, uid);
      updateUidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertUidStmt(this->conn_, this->nsDb_, STMT_INSERT_UNIQ_UID);
      uid = 1;
      insertUidStmt.bindParam(0, uid);
      insertUidStmt.execute();
    }

    // Insert the user
    Statement userStmt(this->conn_, this->nsDb_, STMT_INSERT_USER);

    userStmt.bindParam(0, uid);
    userStmt.bindParam(1, uname);
    userStmt.bindParam(2, ca);
    userStmt.bindParam(3, 0);

    userStmt.execute();
  }
  catch (...) {
    mysql_query(this->conn_, "ROLLBACK");
    throw;
  }

  if (mysql_query(this->conn_, "COMMIT") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->conn_));

  // Build and return the UserInfo
  UserInfo u;
  u.uid = uid;
  strncpy(u.name, uname.c_str(), sizeof(u.name));
  strncpy(u.ca,   ca.c_str(),    sizeof(u.ca));
  u.banned = 0;

  return u;
}



UserInfo UserGroupDbMySql::getUser(uid_t uid) throw (DmException)
{
  UserInfo  user;
  Statement stmt(this->conn_, this->nsDb_, STMT_GET_USERINFO_BY_UID);

  stmt.bindParam(0, uid);
  stmt.execute();

  stmt.bindResult(0, &user.uid);
  stmt.bindResult(1, user.name, sizeof(user.name));
  stmt.bindResult(2, user.ca, sizeof(user.ca));
  stmt.bindResult(3, &user.banned);

  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_USER, "User %ld not found", uid);

  return user;
}



UserInfo UserGroupDbMySql::getUser(const std::string& userName) throw (DmException)
{
  UserInfo  user;
  
  // If the username is the host DN, root!
  if (userName == dmlite::getHostDN()) {
    strncpy(user.name, userName.c_str(), sizeof(user.name));
    user.ca[0] = '\0';
    user.banned = 0;
    user.uid    = 0;
  }
  else {
    // Search in the DB
    Statement stmt(this->conn_, this->nsDb_, STMT_GET_USERINFO_BY_NAME);

    stmt.bindParam(0, userName);
    stmt.execute();

    stmt.bindResult(0, &user.uid);
    stmt.bindResult(1, user.name, sizeof(user.name));
    stmt.bindResult(2, user.ca, sizeof(user.ca));
    stmt.bindResult(3, &user.banned);

    if (!stmt.fetch())
      throw DmException(DM_NO_SUCH_USER, "User " + userName + " not found");
  }
  
  return user;
}



void UserGroupDbMySql::getIdMap(const std::string& userName,
                                const std::vector<std::string>& groupNames,
                                UserInfo* user,
                                std::vector<GroupInfo>* groups) throw (DmException)
{
  std::string vo;
  GroupInfo   group;

  // Clear
  groups->clear();

  // User mapping
  try {
    *user = this->getUser(userName);
  }
  catch (DmException e) {
    if (e.code() == DM_NO_SUCH_USER)
      *user = this->newUser(userName, "");
    else
      throw;
  }

  // No VO information, so use the mapping file to get the group
  if (groupNames.empty()) {
    vo = voFromDn("/etc/lcgdm-mapfile", userName);
    try {
      group = this->getGroup(vo);
    }
    catch (DmException e) {
      if (e.code() == DM_NO_SUCH_GROUP)
        group = this->newGroup(vo);
      else
        throw;
    }
    groups->push_back(group);
  }
  else {
    // Get group info
    std::vector<std::string>::const_iterator i;
    for (i = groupNames.begin(); i != groupNames.end(); ++i) {
      vo = voFromRole(*i);
      try {
        group = this->getGroup(vo);
      }
      catch (DmException e) {
        if (e.code() == DM_NO_SUCH_GROUP)
          group = this->newGroup(vo);
        else
          throw;
      }
      groups->push_back(group);
    }
  }
}
