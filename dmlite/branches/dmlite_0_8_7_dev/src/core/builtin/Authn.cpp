/// @file    core/builtin/Authn.cpp
/// @brief   User and group mapping using the system's ones.
/// @details This will be used by default when no other Authn
///          implementeation is loaded.
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/cpp/utils/security.h>
#include <grp.h>
#include <pwd.h>
#include "Authn.h"

#include "utils/logger.h"

using namespace dmlite;



BuiltInAuthn::BuiltInAuthn(const std::string& nb, const std::string& ng):
    nobody_(nb), nogroup_(ng)
{
  // Nothing
}



BuiltInAuthn::~BuiltInAuthn()
{
  // Nothing
}



std::string BuiltInAuthn::getImplId() const throw ()
{
  return std::string("BuiltInAuthn");
}



SecurityContext* BuiltInAuthn::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  Log(Logger::Lvl4, Logger::unregistered, "BuiltInAuthnFactory",   " clientname: " << cred.clientName);
  
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.clientName, cred.fqans, &user, &groups);
  
  SecurityContext *sec = new SecurityContext(cred, user, groups);
  
  Log(Logger::Lvl3, Logger::unregistered, "BuiltInAuthnFactory",   "Exiting. clientname: " << cred.clientName);
  return sec;
}

SecurityContext* BuiltInAuthn::createSecurityContext() throw (DmException)
{
  Log(Logger::Lvl4, Logger::unregistered, "BuiltInAuthnFactory",   "");
  
  Log(Logger::Lvl3, Logger::unregistered, "BuiltInAuthnFactory",   "Exiting with NULL");
  return NULL;
}

GroupInfo BuiltInAuthn::getGroup(gid_t gid) throw (DmException)
{
  struct group  grp;
  struct group *result;
  char          buffer[16384];

  getgrgid_r(gid, &grp, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DMLITE_NO_SUCH_GROUP,
                      "Group %d not found", gid);
  
  GroupInfo gi;
  
  gi["banned"] = 0;
  gi["gid"]    = result->gr_gid;
  gi.name      = result->gr_name;
  
  return gi;
}



GroupInfo BuiltInAuthn::getGroup(const std::string& groupName) throw (DmException)
{
  struct group  grp;
  struct group *result;
  char          buffer[16384];

  getgrnam_r(groupName.c_str(), &grp, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DMLITE_NO_SUCH_GROUP,
                      "Group %s not found", groupName.c_str());
  
  GroupInfo gi;
  
  gi["banned"] = 0;
  gi["gid"]    = result->gr_gid;
  gi.name      = result->gr_name;
  
  return gi;
}


GroupInfo BuiltInAuthn::getGroup(const std::string& key,
                                 const boost::any& value) throw (DmException)
{
  if (key != "gid")
    throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_KEY),
                      "BuiltInAuthn does not support querying by %s",
                      key.c_str());
  
  gid_t gid = Extensible::anyToUnsigned(value);
  return this->getGroup(gid);
}



UserInfo BuiltInAuthn::getUser(const std::string& userName, gid_t* group) throw (DmException)
{
  struct passwd  pwd;
  struct passwd *result;
  char           buffer[16384];

  getpwnam_r(userName.c_str(), &pwd, buffer, sizeof(buffer), &result);
  
  if (result == NULL)
    throw DmException(DMLITE_NO_SUCH_USER,
                      "User %s not found", userName.c_str());
  
  UserInfo ui;
  
  ui["banned"] = 0;
  ui["uid"]    = result->pw_uid;
  ui.name      = result->pw_name;
  
  *group = result->pw_gid;
  
  return ui;
}



UserInfo BuiltInAuthn::getUser(const std::string& userName) throw (DmException)
{
  gid_t ignore;
  return this->getUser(userName, &ignore);
}



UserInfo BuiltInAuthn::getUser(const std::string& key,
                               const boost::any& value) throw (DmException)
{
  if (key != "uid")
    throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_KEY),
                      "BuiltInAuthn does not support querying by %s",
                      key.c_str());
  
  uid_t         uid = Extensible::anyToUnsigned(value);
  struct passwd pwdbuf, *upwd;
  char          buffer[512];
  
  getpwuid_r(uid, &pwdbuf, buffer, sizeof(buffer), &upwd);
  if (upwd == NULL)
    throw DmException(DMLITE_NO_SUCH_USER,
                      "User %u not found", uid);
  
  UserInfo u;
  u.name   = upwd->pw_name;
  u["uid"] = upwd->pw_uid;
  return u;
}



GroupInfo BuiltInAuthn::newGroup(const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "newGroup not supported in BuiltInAuthn");
}



void BuiltInAuthn::updateGroup(const GroupInfo&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "updateGroup not supported in BuiltInAuthn");
}



UserInfo BuiltInAuthn::newUser(const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "newUser not supported in BuiltInAuthn");
}



void BuiltInAuthn::updateUser(const UserInfo&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "updateUser not supported in BuiltInAuthn");
}



void BuiltInAuthn::deleteGroup(const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "deleteGroup not supported in BuiltInAuthn");
}



void BuiltInAuthn::deleteUser(const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS), "deleteUser not supported in BuiltInAuthn");
}



std::vector<GroupInfo> BuiltInAuthn::getGroups(void) throw (DmException)
{
  std::vector<GroupInfo> groups;
  GroupInfo group;
  struct group* ent;
  
  while ((ent = getgrent()) != NULL) {
    group.clear();
    group.name   = ent->gr_name;
    group["gid"] = ent->gr_gid;
  }
  
  return groups;
}



std::vector<UserInfo> BuiltInAuthn::getUsers(void) throw (DmException)
{
  std::vector<UserInfo> users;
  UserInfo user;
  struct passwd* ent;
  
  while ((ent = getpwent()) != NULL) {
    user.clear();
    user.name = ent->pw_name;
    user["uid"] = ent->pw_uid;
    users.push_back(user);
  }
  
  return users;
}



void BuiltInAuthn::getIdMap(const std::string& userName,
                            const std::vector<std::string>& groupNames,
                            UserInfo* user,
                            std::vector<GroupInfo>* groups) throw (DmException)
{
  std::string vo;
  GroupInfo   group;
  gid_t       gid;

  groups->clear();

  try {
    *user = this->getUser(userName, &gid);
  }
  catch (DmException &e) {
    if (DMLITE_ERRNO(e.code()) != DMLITE_NO_SUCH_USER)
      throw;
    // Try again with nobody
    *user = this->getUser(nobody_, &gid);
  }
  
  // No VO information, so use the default from passwd
  if (groupNames.empty()) {
    group = this->getGroup(gid);
    groups->push_back(group);
  }
  else {
    // Get group info
    try {
      std::vector<std::string>::const_iterator i;
      for (i = groupNames.begin(); i != groupNames.end(); ++i) {
        vo = voFromRole(*i);
        group = this->getGroup(vo);
        groups->push_back(group);
      }
    }
    catch (DmException& e) {
      if (DMLITE_ERRNO(e.code()) != DMLITE_NO_SUCH_GROUP)
        throw;
      group = this->getGroup(nogroup_);
      groups->push_back(group);
    }
  }
}



BuiltInAuthnFactory::BuiltInAuthnFactory(): nobody_("nobody"), nogroup_("nobody")
{
  // Nothing
}



BuiltInAuthnFactory::~BuiltInAuthnFactory()
{
  // Nothing
}



void BuiltInAuthnFactory::configure(const std::string& key,
                                    const std::string& value) throw (DmException)
{
  bool gotit = true;
  Log(Logger::Lvl4, Logger::unregistered, "BuiltInAuthnFactory",   " Key: " << key << " Value: " << value);
  
  if (key == "AnonymousUser")
    this->nobody_ = value;
  else if (key == "AnonymousGroup")
    this->nogroup_ = value;
  else gotit = false;
  
  if (gotit)
    Log(Logger::Lvl4, Logger::unregistered, "BuiltInAuthnFactory", "Setting parms. Key: " << key << " Value: " << value);
    
}



Authn* BuiltInAuthnFactory::createAuthn(PluginManager* pm) throw (DmException)
{
  return new BuiltInAuthn(this->nobody_, this->nogroup_);
}
