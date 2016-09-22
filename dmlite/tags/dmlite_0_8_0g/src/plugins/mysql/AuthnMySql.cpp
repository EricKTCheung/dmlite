/// @file   AuthnMySql.cpp
/// @brief  MySQL Authn Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/utils/security.h>
#include "AuthnMySql.h"
#include "Queries.h"
#include "MySqlFactories.h"
#include "utils/logger.h"

using namespace dmlite;

AuthnMySql::AuthnMySql(NsMySqlFactory* factory,
                       const std::string& db,
                       const std::string& mapfile,
                       bool hostDnIsRoot, const std::string& hostDn) throw(DmException):
  factory_(factory), nsDb_(db), mapFile_(mapfile), hostDnIsRoot_(hostDnIsRoot),
  hostDn_(hostDn)
{
  
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ctor");
  
  // Nothing
}



AuthnMySql::~AuthnMySql()
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Dtor");
  // Nothing
}



std::string AuthnMySql::getImplId(void) const throw()
{
  return std::string("AuthnMySql");
}



SecurityContext* AuthnMySql::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, cred.clientName << " " << cred.remoteAddress);
  
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.clientName, cred.fqans, &user, &groups);
  
  SecurityContext* sec = new SecurityContext(cred, user, groups);
  
  Log(Logger::Lvl1, mysqllogmask, mysqllogname, cred.clientName << " " << cred.remoteAddress);
  return sec;
}



SecurityContext* AuthnMySql::createSecurityContext(void) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  
  UserInfo user;
  std::vector<GroupInfo> groups;
  GroupInfo group;

  user.name    = "root";
  user["uid"]  = 0;
  group.name   = "root";
  group["gid"] = 0;
  groups.push_back(group);

  SecurityContext* sec = new SecurityContext(SecurityCredentials(), user, groups);
  Log(Logger::Lvl1, mysqllogmask, mysqllogname, SecurityCredentials().clientName << " " << SecurityCredentials().remoteAddress);
  
  return sec;
}



GroupInfo AuthnMySql::newGroup(const std::string& gname) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "group:" << gname);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());

  if (mysql_query(conn, "BEGIN") != 0)
    throw DmException(DMLITE_DBERR(mysql_errno(conn)),
                      mysql_error(conn));
  
  
  gid_t gid;
  try {
    // Get the last gid, increment and update
    Statement gidStmt(conn, this->nsDb_, STMT_GET_UNIQ_GID_FOR_UPDATE);

    gidStmt.execute();
    gidStmt.bindResult(0, &gid);

    // Update the gid
    if (gidStmt.fetch()) {
      Statement updateGidStmt(conn, this->nsDb_, STMT_UPDATE_UNIQ_GID);
      ++gid;
      updateGidStmt.bindParam(0, gid);
      updateGidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertGidStmt(conn, this->nsDb_, STMT_INSERT_UNIQ_GID);
      gid = 1;
      insertGidStmt.bindParam(0, gid);
      insertGidStmt.execute();
    }

    // Insert the group
    Statement groupStmt(conn, this->nsDb_, STMT_INSERT_GROUP);

    groupStmt.bindParam(0, gid);
    groupStmt.bindParam(1, gname);
    groupStmt.bindParam(2, 0);

    groupStmt.execute();
  }
  catch (...) {
    mysql_query(conn, "ROLLBACK");
    throw;
  }

  mysql_query(conn, "COMMIT");

  // Build and return the GroupInfo
  GroupInfo g;
  g.name      = gname;
  g["gid"]    = gid;
  g["banned"] = 0;

  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. group:" << gname << " gid:" << gid);
  return g;
}



GroupInfo AuthnMySql::getGroup(const std::string& groupName) throw (DmException)
{
  GroupInfo group;
  gid_t     gid;
  int       banned;
  char      groupname[256], meta[1024];
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "group:" << groupName);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  
  Statement stmt(conn, this->nsDb_, STMT_GET_GROUPINFO_BY_NAME);

  stmt.bindParam(0, groupName);
  stmt.execute();

  stmt.bindResult(0, &gid);
  stmt.bindResult(1, groupname, sizeof(groupname));
  stmt.bindResult(2, &banned);
  stmt.bindResult(3, meta, sizeof(meta));
  
  if (!stmt.fetch())
    throw DmException(DMLITE_NO_SUCH_GROUP, "Group %s not found", groupName.c_str());
  
  group.name      = groupname;
  group["gid"]    = gid;
  group["banned"] = banned;
  group.deserialize(meta);
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. group:" << groupname << " gid:" << gid);
  
  return group;
}



GroupInfo AuthnMySql::getGroup(const std::string& key,
                               const boost::any& value) throw (DmException)
{
  GroupInfo group;
  gid_t     gid;
  int       banned;
  char      groupname[256], meta[1024];
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "key:" << key);
  
  if (key != "gid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "AuthnMySql does not support querying by %s",
                      key.c_str());
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());

  gid = Extensible::anyToUnsigned(value);
  Statement stmt(conn, this->nsDb_, STMT_GET_GROUPINFO_BY_GID);
  
  stmt.bindParam(0, gid);
  stmt.execute();
  
  stmt.bindResult(0, &gid);
  stmt.bindResult(1, groupname, sizeof(groupname));
  stmt.bindResult(2, &banned);
  stmt.bindResult(3, meta, sizeof(meta));
  
  if (!stmt.fetch())
    throw DmException(DMLITE_NO_SUCH_GROUP, "Group %u not found", gid);
  
  group.name      = groupname;
  group["gid"]    = gid;
  group["banned"] = banned;
  group.deserialize(meta);
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. group:" << groupname << " gid:" << gid);
  return group;
}



void AuthnMySql::updateGroup(const GroupInfo& group) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "grp:" << group.name);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_UPDATE_GROUP);
  
  stmt.bindParam(0, group.getLong("banned"));
  GroupInfo g = group;
  g.erase("gid");
  g.erase("banned");
  stmt.bindParam(1, g.serialize());
  stmt.bindParam(2, group.name);
  
  stmt.execute();
  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting. group:" << group.name);
}



void AuthnMySql::deleteGroup(const std::string& groupName) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "grp:" << groupName);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_DELETE_GROUP);
  
  stmt.bindParam(0, groupName);
  
  stmt.execute();
  Log(Logger::Lvl2, mysqllogmask, mysqllogname, "Exiting. group:" << groupName);
}



UserInfo AuthnMySql::newUser(const std::string& uname) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "usr:" << uname);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());

  if (mysql_query(conn, "BEGIN") != 0)
    throw DmException(mysql_errno(conn), mysql_error(conn));

  uid_t uid;
  
  try {
    // Get the last uid, increment and update
    Statement uidStmt(conn, this->nsDb_, STMT_GET_UNIQ_UID_FOR_UPDATE);

    uidStmt.execute();
    uidStmt.bindResult(0, &uid);

    // Update the uid
    if (uidStmt.fetch()) {
      Statement updateUidStmt(conn, this->nsDb_, STMT_UPDATE_UNIQ_UID);
      ++uid;
      updateUidStmt.bindParam(0, uid);
      updateUidStmt.execute();
    }
    // Couldn't get, so insert it instead
    else {
      Statement insertUidStmt(conn, this->nsDb_, STMT_INSERT_UNIQ_UID);
      uid = 1;
      insertUidStmt.bindParam(0, uid);
      insertUidStmt.execute();
    }

    // Insert the user
    Statement userStmt(conn, this->nsDb_, STMT_INSERT_USER);

    userStmt.bindParam(0, uid);
    userStmt.bindParam(1, uname);
    userStmt.bindParam(2, 0);

    userStmt.execute();
  }
  catch (...) {
    mysql_query(conn, "ROLLBACK");
    throw;
  }

  if (mysql_query(conn, "COMMIT") != 0)
    throw DmException(mysql_errno(conn), mysql_error(conn));

  // Build and return the UserInfo
  UserInfo u;
  u.name      = uname;
  u["uid"]    = uid;
  u["banned"] = 0;

  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting. usr:" << uname << " uid:" << uid);
  
  return u;
}



UserInfo AuthnMySql::getUser(const std::string& userName) throw (DmException)
{
  UserInfo   user;
  uid_t      uid;
  int        banned;
  char       ca[1024], username[256], meta[1024];
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "usr:" << userName);
  
  // If the username is the host DN, root!
  if (this->hostDnIsRoot_ && userName == this->hostDn_) {
    user.name      = userName;
    user["ca"]     = std::string();
    user["banned"] = 0;
    user["uid"]    = 0u;
  }
  else {
    // Search in the DB
    PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
    Statement stmt(conn, this->nsDb_, STMT_GET_USERINFO_BY_NAME);

    stmt.bindParam(0, userName);
    stmt.execute();

    stmt.bindResult(0, &uid);
    stmt.bindResult(1, username, sizeof(username));
    stmt.bindResult(2, ca, sizeof(ca));
    stmt.bindResult(3, &banned);
    stmt.bindResult(4, meta, sizeof(meta));

    if (!stmt.fetch())
      throw DmException(DMLITE_NO_SUCH_USER, "User %s not found", userName.c_str());
    
    user.name      = username;
    user["uid"]    = uid;
    user["banned"] = banned;
    user.deserialize(meta);
  }
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. usr:" << username << " uid:" << uid << " ban:" << banned);
  return user;
}



UserInfo AuthnMySql::getUser(const std::string& key,
                             const boost::any& value) throw (DmException)
{
  UserInfo   user;
  uid_t      uid;
  int        banned;
  char       ca[1024], username[256], meta[1024];
  
   Log(Logger::Lvl4, mysqllogmask, mysqllogname, "key:" << key);
   
  if (key != "uid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "AuthnMySql does not support querying by %s",
                      key.c_str());
  
  uid = Extensible::anyToUnsigned(value);
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_USERINFO_BY_UID);

  stmt.bindParam(0, uid);
  stmt.execute();

  stmt.bindResult(0, &uid);
  stmt.bindResult(1, username, sizeof(username));
  stmt.bindResult(2, ca, sizeof(ca));
  stmt.bindResult(3, &banned);
  stmt.bindResult(4, meta, sizeof(meta));

  if (!stmt.fetch())
    throw DmException(DMLITE_NO_SUCH_USER, "User %u not found", uid);

  user.name      = username;
  user["uid"]    = uid;
  user["banned"] = banned;
  user.deserialize(meta);
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. usr:" << username << " uid:" << uid << " ban:" << banned);
  return user;
}



void AuthnMySql::updateUser(const UserInfo& user) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "usr:" << user.name);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_UPDATE_USER);
  
  stmt.bindParam(0, user.getLong("banned"));
  UserInfo u = user;
  u.erase("uid");
  u.erase("banned");
  stmt.bindParam(1, u.serialize());
  stmt.bindParam(2, user.name);
  
  stmt.execute();
  
  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting. usr:" << user.name << " ban:" << boost::any_cast<bool>(user["banned"]) );
  
}



void AuthnMySql::deleteUser(const std::string& userName) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "usr:" << userName);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_DELETE_USER);
  
  stmt.bindParam(0, userName);
  
  stmt.execute();
  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting usr:" << userName);
}



std::vector<GroupInfo> AuthnMySql::getGroups(void) throw (DmException)
{
  std::vector<GroupInfo> groups;
  GroupInfo group;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_ALL_GROUPS);
  stmt.execute();  
  
  gid_t gid;
  char  gname[256], meta[1024];
  int   banned;

  stmt.bindResult(0, &gid);
  stmt.bindResult(1, gname, sizeof(gname));
  stmt.bindResult(2, &banned);
  stmt.bindResult(3, meta, sizeof(meta));
  
  while (stmt.fetch()) {
    group.clear();
    group.name      = gname;
    group["gid"]    = gid;
    group["banned"] = banned;
    group.deserialize(meta);
    groups.push_back(group);
  }
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. ngroups:" << groups.size());
  return groups;
}



std::vector<UserInfo> AuthnMySql::getUsers(void) throw (DmException)
{
  std::vector<UserInfo> users;
  UserInfo user;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->nsDb_, STMT_GET_ALL_USERS);
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
    user.clear();
    
    user.name      = uname;
    user["uid"]    = uid;
    user["banned"] = banned;
    user["ca"]     = std::string(ca);
    user.deserialize(meta);
    
    users.push_back(user);
  }
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. nusers:" << users.size());
  return users;
}



void AuthnMySql::getIdMap(const std::string& userName,
                          const std::vector<std::string>& groupNames,
                          UserInfo* user,
                          std::vector<GroupInfo>* groups) throw (DmException)
{
  std::string vo;
  GroupInfo   group;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "usr:" << userName);
  
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());

  // Clear
  groups->clear();

  // User mapping
  try {
    *user = this->getUser(userName);
  }
  catch (DmException& e) {
    if (DMLITE_ERRNO(e.code()) == DMLITE_NO_SUCH_USER)
      *user = this->newUser(userName);
    else
      throw;
  }

  //check if the user DN is the host DN and avoid getting the mapping
  if (this->hostDnIsRoot_ && userName == this->hostDn_)  {
    group.name   = "root";
    group["gid"] = 0;
    groups->push_back(group);
  } // No VO information, so use the mapping file to get the group
  else if (groupNames.empty()) {
    vo = voFromDn(this->mapFile_, userName);
    try {
      group = this->getGroup(vo);
    }
    catch (DmException& e) {
      if (DMLITE_ERRNO(e.code()) == DMLITE_NO_SUCH_GROUP)
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
      catch (DmException& e) {
        if (DMLITE_ERRNO(e.code()) == DMLITE_NO_SUCH_GROUP)
          group = this->newGroup(vo);
        else
          throw;
      }
      groups->push_back(group);
    }
  }
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. usr:" << userName);
}
