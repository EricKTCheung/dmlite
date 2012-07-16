/// @file     core/builtin/UserGroupDb.cpp
/// @brief    User and group mapping using the system's ones.
/// @detailed This will be used by default when no other UserGroupDb
///           implementeation is loaded.
/// @author   Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/cpp/utils/dm_security.h>
#include <grp.h>
#include <pwd.h>
#include "UserGroupDb.h"

using namespace dmlite;



BuiltInUserGroupDb::BuiltInUserGroupDb()
{
  // Nothing
}



BuiltInUserGroupDb::~BuiltInUserGroupDb()
{
  // Nothing
}



std::string BuiltInUserGroupDb::getImplId() throw ()
{
  return std::string("BuiltInUserGroupDb");
}



SecurityContext* BuiltInUserGroupDb::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.getClientName(), cred.getFqans(), &user, &groups);
  return new SecurityContext(cred, user, groups);
}



GroupInfo BuiltInUserGroupDb::getGroup(gid_t gid) throw (DmException)
{
  struct group  grp;
  struct group *result;
  char          buffer[16384];

  getgrgid_r(gid, &grp, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DM_NO_SUCH_GROUP, "Group %d not found", gid);
  
  GroupInfo gi;
  
  gi.banned = 0;
  gi.gid    = result->gr_gid;
  strncpy(gi.name, result->gr_name, sizeof(gi.name));
  
  return gi;
}



GroupInfo BuiltInUserGroupDb::getGroup(const std::string& groupName) throw (DmException)
{
  struct group  grp;
  struct group *result;
  char          buffer[16384];

  getgrnam_r(groupName.c_str(), &grp, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DM_NO_SUCH_GROUP, "Group %s not found", groupName.c_str());
  
  GroupInfo gi;
  
  gi.banned = 0;
  gi.gid    = result->gr_gid;
  strncpy(gi.name, result->gr_name, sizeof(gi.name));
  
  return gi;
}



UserInfo BuiltInUserGroupDb::getUser(const std::string& userName, gid_t* group) throw (DmException)
{
  struct passwd  pwd;
  struct passwd *result;
  char           buffer[16384];

  getpwnam_r(userName.c_str(), &pwd, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DM_NO_SUCH_GROUP, "User %s not found", userName.c_str());
  
  UserInfo ui;
  
  ui.banned = 0;
  ui.uid    = result->pw_uid;
  strncpy(ui.name, result->pw_name, sizeof(ui.name));
  
  *group = result->pw_gid;
  
  return ui;
}



UserInfo BuiltInUserGroupDb::getUser(const std::string& userName) throw (DmException)
{
  gid_t ignore;
  return this->getUser(userName, &ignore);
}



GroupInfo BuiltInUserGroupDb::newGroup(const std::string& gname) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "newGroup not supported in BuiltInuserGroupDb");
}



UserInfo BuiltInUserGroupDb::newUser(const std::string& uname, const std::string& ca) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "newUser not supported in BuiltInuserGroupDb");
}



void BuiltInUserGroupDb::getIdMap(const std::string& userName,
                                  const std::vector<std::string>& groupNames,
                                  UserInfo* user,
                                  std::vector<GroupInfo>* groups) throw (DmException)
{
  std::string vo;
  GroupInfo   group;
  gid_t       gid;

  groups->clear();

  *user = this->getUser(userName, &gid);
  
  // No VO information, so use the default from passwd
  if (groupNames.empty()) {
    group = this->getGroup(gid);
    groups->push_back(group);
  }
  else {
    // Get group info
    std::vector<std::string>::const_iterator i;
    for (i = groupNames.begin(); i != groupNames.end(); ++i) {
      vo = voFromRole(*i);
      group = this->getGroup(vo);
      groups->push_back(group);
    }
  }
}



BuiltInUserGroupDbFactory::BuiltInUserGroupDbFactory()
{
  // Nothing
}



BuiltInUserGroupDbFactory::~BuiltInUserGroupDbFactory()
{
  // Nothing
}



void BuiltInUserGroupDbFactory::configure(const std::string& key,
                                          const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



UserGroupDb* BuiltInUserGroupDbFactory::createUserGroupDb(PluginManager* pm) throw (DmException)
{
  return new BuiltInUserGroupDb();
}
