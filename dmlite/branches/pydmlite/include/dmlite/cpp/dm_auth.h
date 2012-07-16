/// @file   include/dmlite/dm_auth.h
/// @brief  Security API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_AUTH_H
#define	DMLITEPP_AUTH_H

#include <string>
#include <vector>
#include "dm_basefactory.h"
#include "dm_exceptions.h"
#include "../common/dm_types.h"

namespace dmlite {

/// This is just for convenience
class SecurityCredentials: public Credentials {
public:
  SecurityCredentials();
  SecurityCredentials(const Credentials&);
  SecurityCredentials(const SecurityCredentials&);
  ~SecurityCredentials();

  std::string getSecurityMechanism() const throw();
  std::string getClientName()        const throw();
  std::string getRemoteAddress()     const throw();

  const std::vector<std::string>& getFqans() const throw();

  std::string getSessionId() const throw();

  const SecurityCredentials& operator = (const SecurityCredentials&);
private:
  std::vector<std::string> vfqans;
};

/// Wraps a security context
class SecurityContext {
public:
  SecurityContext();
  SecurityContext(const SecurityCredentials&);
  SecurityContext(const SecurityCredentials&, const UserInfo&, const std::vector<GroupInfo>&);
  ~SecurityContext();

  const UserInfo&  getUser() const                  throw();
  const GroupInfo& getGroup(unsigned idx = 0) const throw(DmException);
  unsigned groupCount() const                       throw();

  UserInfo&  getUser()  throw();
  GroupInfo& getGroup(unsigned idx = 0) throw(DmException);
  void resizeGroup(unsigned size) throw();

  const SecurityCredentials& getCredentials() const throw();
  void setCredentials(const SecurityCredentials&) throw();

  bool hasGroup(gid_t gid) const throw();

private:
  SecurityCredentials credentials_;

  UserInfo  user_;
  std::vector<GroupInfo> groups_;
};

// Advanced declaration
class StackInstance;

/// User and group handling.
class UserGroupDb {
public:
  /// Destructor
  virtual ~UserGroupDb();
  
  /// String ID of the user DB implementation.
  virtual std::string getImplId(void) throw() = 0;
  
  /// Create a security context from the credentials.
  /// @param cred The security credentials.
  virtual SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException) = 0;
  
  /// Create a new group.
  /// @param gname The new group name.
  virtual GroupInfo newGroup(const std::string& gname) throw (DmException) = 0;

  /// Get the group id of a specific group name.
  /// @param groupName The group name.
  /// @return          The group.
  virtual GroupInfo getGroup(const std::string& groupName) throw (DmException) = 0;

  /// Create a new user.
  /// @param uname The new user name.
  /// @param ca    The user Certification Authority.
  virtual UserInfo newUser(const std::string& uname, const std::string& ca) throw (DmException) = 0;

  /// Get the user id of a specific user name.
  /// @param userName The user name.
  virtual UserInfo getUser(const std::string& userName) throw (DmException) = 0;
  
  /// Get the mapping of a user/group.
  /// @param userName   The user name.
  /// @param groupNames The different groups. Can be empty.
  /// @param user       Pointer to an UserInfo struct where to put the data.
  /// @param groups     Pointer to a vector where the group mapping will be put.
  /// @note If groupNames is empty, grid mapfile will be used to retrieve the default group.
  virtual void getIdMap(const std::string& userName,
                        const std::vector<std::string>& groupNames,
                        UserInfo* user,
                        std::vector<GroupInfo>* groups) throw (DmException) = 0;
};


class PluginManager;


/// UserGroupDbFactory
class UserGroupDbFactory: public virtual BaseFactory {
public:
  /// Destructor
  virtual ~UserGroupDbFactory();
  
  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

protected:
  // Stack instance is allowed to instantiate UserGroupDb
  friend class StackInstance;
  
  /// Children of UserGroupDbFactory are allowed to instantiate too (decorator)
  static UserGroupDb* createUserGroupDb(UserGroupDbFactory* factory, PluginManager* pm) throw (DmException);
  
  /// Instantiate a implementation of UserGroupDb
  virtual UserGroupDb* createUserGroupDb(PluginManager* pm) throw (DmException) = 0;

private:
};

};

#endif	// DMLITEPP_AUTH_H
