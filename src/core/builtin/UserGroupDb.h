/// @file     core/builtin/UserGroupDb.h
/// @brief    User and group mapping using the system's ones.
/// @detailed This will be used by default when no other UserGroupDb
///           implementeation is loaded.
/// @author   Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef BUILTIN_AUTH_H
#define	BUILTIN_AUTH_H

#include <dmlite/cpp/dm_auth.h>

namespace dmlite {
  
class BuiltInUserGroupDb: public UserGroupDb {
public:
  BuiltInUserGroupDb();
  ~BuiltInUserGroupDb();
  
  std::string getImplId(void) throw();
  
  SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);
  
  GroupInfo newGroup(const std::string& gname) throw (DmException);
  GroupInfo getGroup(gid_t gid) throw (DmException);
  GroupInfo getGroup(const std::string& groupName) throw (DmException);

  UserInfo newUser(const std::string& uname, const std::string& ca) throw (DmException);
  UserInfo getUser(const std::string& userName) throw (DmException);
  UserInfo getUser(const std::string& userName, gid_t* group) throw (DmException);
  
  void getIdMap(const std::string& userName,
                const std::vector<std::string>& groupNames,
                UserInfo* user,
                std::vector<GroupInfo>* groups) throw (DmException);
};

class BuiltInUserGroupDbFactory: public UserGroupDbFactory {
public:
  BuiltInUserGroupDbFactory();
  ~BuiltInUserGroupDbFactory();
  
  void configure(const std::string& key, const std::string& value) throw (DmException);

  UserGroupDb* createUserGroupDb(PluginManager* pm) throw (DmException);
};
  
};

#endif	// BUILTIN_AUTH_H
