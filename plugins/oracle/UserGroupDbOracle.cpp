#include <cstring>
#include <dmlite/cpp/utils/dm_security.h>
#include "Queries.h"
#include "UserGroupDbOracle.h"

using namespace dmlite;
using namespace oracle;


UserGroupDbOracle::UserGroupDbOracle(oracle::occi::ConnectionPool* pool,
                                     oracle::occi::Connection* conn,
                                     const std::string& mapfile) throw(DmException):
  pool_(pool), conn_(conn), mapFile_(mapfile)
{
  // Nothing
}
  


UserGroupDbOracle::~UserGroupDbOracle() throw(DmException)
{
  this->pool_->terminateConnection(this->conn_);
}



std::string UserGroupDbOracle::getImplId(void) throw()
{
  return std::string("UserGroupDbOracle");
}



SecurityContext* UserGroupDbOracle::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.getClientName(), cred.getFqans(), &user, &groups);
  return new SecurityContext(cred, user, groups);
}



GroupInfo UserGroupDbOracle::newGroup(const std::string& gname) throw (DmException)
{
  gid_t     gid;

  try {
    // Get the last gid, increment and update
    occi::Statement* gidStmt = getPreparedStatement(this->conn_, STMT_GET_UNIQ_GID_FOR_UPDATE);

    occi::ResultSet* rs = gidStmt->executeQuery();

    // Update the gid
    if (rs->next()) {
      gid = rs->getNumber(1);
      ++gid;

      occi::Statement* updateGidStmt = getPreparedStatement(this->conn_, STMT_UPDATE_UNIQ_GID);
      updateGidStmt->setNumber(1, gid);
      updateGidStmt->executeUpdate();

      this->conn_->terminateStatement(updateGidStmt);
    }
    // Couldn't get, so insert it instead
    else {
      gid = 1;
      occi::Statement* insertGidStmt = getPreparedStatement(this->conn_, STMT_INSERT_UNIQ_GID);
      insertGidStmt->setNumber(1, gid);
      insertGidStmt->executeUpdate();

      this->conn_->terminateStatement(insertGidStmt);
    }

    gidStmt->closeResultSet(rs);
    this->conn_->terminateStatement(gidStmt);

    // Insert the group
    occi::Statement* groupStmt = getPreparedStatement(this->conn_, STMT_INSERT_GROUP);

    groupStmt->setNumber(1, gid);
    groupStmt->setString(2, gname);
    groupStmt->setNumber(3, 0);

    groupStmt->executeUpdate();

    this->conn_->terminateStatement(groupStmt);

    // Commit
    this->conn_->commit();
  }
  catch (occi::SQLException& e) {
    this->conn_->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }

  // Build and return the GroupInfo
  GroupInfo g;
  g.gid = gid;
  strncpy(g.name, gname.c_str(), sizeof(g.name));
  g.banned = 0;

  return g;
}



GroupInfo UserGroupDbOracle::getGroup(gid_t gid) throw (DmException)
{
  GroupInfo group;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_GROUPINFO_BY_GID);

  stmt->setNumber(1, gid);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_GROUP, "Group %ld not found", gid);

  group.gid = rs->getNumber(1);
  strncpy(group.name, rs->getString(2).c_str(), sizeof(group.name));
  group.banned = rs->getNumber(3);

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  return group;
}



GroupInfo UserGroupDbOracle::getGroup(const std::string& groupName) throw (DmException)
{
  GroupInfo group;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_GROUPINFO_BY_NAME);

  stmt->setString(1, groupName);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_GROUP, "Group " + groupName + " not found");

  group.gid = rs->getNumber(1);
  strncpy(group.name, rs->getString(2).c_str(), sizeof(group.name));
  group.banned = rs->getNumber(3);

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  return group;
}



UserInfo UserGroupDbOracle::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  uid_t uid;
  
  try {
    // Get the last uid, increment and update
    occi::Statement* uidStmt = getPreparedStatement(this->conn_, STMT_GET_UNIQ_UID_FOR_UPDATE);

    occi::ResultSet* rs = uidStmt->executeQuery();

    // Update the uid
    if (rs->next()) {
      uid = rs->getNumber(1);
      ++uid;

      occi::Statement* updateUidStmt = getPreparedStatement(this->conn_, STMT_UPDATE_UNIQ_UID);
      updateUidStmt->setNumber(1, uid);
      updateUidStmt->executeUpdate();
      this->conn_->terminateStatement(updateUidStmt);
    }
    // Couldn't get, so insert it instead
    else {
      uid = 1;

      occi::Statement* insertUidStmt = getPreparedStatement(this->conn_, STMT_INSERT_UNIQ_UID);
      insertUidStmt->setNumber(1, uid);
      insertUidStmt->executeUpdate();
      this->conn_->terminateStatement(insertUidStmt);
    }

    uidStmt->closeResultSet(rs);
    this->conn_->terminateStatement(uidStmt);

    // Insert the user
    occi::Statement* userStmt = getPreparedStatement(this->conn_, STMT_INSERT_USER);

    userStmt->setNumber(1, uid);
    userStmt->setString(2, uname);
    userStmt->setString(3, ca);
    userStmt->setNumber(4, 0);

    userStmt->executeUpdate();

    this->conn_->terminateStatement(userStmt);

    // Commit
    this->conn_->commit();
  }
  catch (occi::SQLException& e) {
    this->conn_->rollback();
    throw DmException(DM_INTERNAL_ERROR, e.getMessage());
  }

  // Build and return the UserInfo
  UserInfo u;
  u.uid = uid;
  strncpy(u.name, uname.c_str(), sizeof(u.name));
  strncpy(u.ca,   ca.c_str(),    sizeof(u.ca));
  u.banned = 0;

  return u;
}



UserInfo UserGroupDbOracle::getUser(uid_t uid) throw (DmException)
{
  UserInfo  user;
  occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_USERINFO_BY_UID);

  stmt->setNumber(1, uid);
  occi::ResultSet* rs = stmt->executeQuery();

  if (!rs->next())
    throw DmException(DM_NO_SUCH_USER, "User %ld not found", uid);

  user.uid = rs->getNumber(1);
  strncpy(user.name, rs->getString(2).c_str(), sizeof(user.name));
  strncpy(user.ca, rs->getString(3).c_str(), sizeof(user.ca));
  user.banned = rs->getNumber(4);

  stmt->closeResultSet(rs);
  this->conn_->terminateStatement(stmt);

  return user;
}



UserInfo UserGroupDbOracle::getUser(const std::string& userName) throw (DmException)
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
    occi::Statement* stmt = getPreparedStatement(this->conn_, STMT_GET_USERINFO_BY_NAME);

    stmt->setString(1, userName);
    occi::ResultSet* rs = stmt->executeQuery();

    if (!rs->next())
      throw DmException(DM_NO_SUCH_USER, "User " + userName + " not found");

    user.uid = rs->getNumber(1);
    strncpy(user.name, rs->getString(2).c_str(), sizeof(user.name));
    strncpy(user.ca, rs->getString(3).c_str(), sizeof(user.ca));
    user.banned = rs->getNumber(4);

    stmt->closeResultSet(rs);
    this->conn_->terminateStatement(stmt);
  }

  return user;
}



void UserGroupDbOracle::getIdMap(const std::string& userName,
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
    vo = voFromDn(this->mapFile_, userName);
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
