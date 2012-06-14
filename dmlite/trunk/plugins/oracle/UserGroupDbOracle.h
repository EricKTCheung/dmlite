/// @file    plugins/oracle/UserGroupDbOracle.h
/// @brief   Oracle UserGroupDB Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef USERGROUPDBORACLEL_H
#define	USERGROUPDBORACLE_H

#include <dmlite/dm_auth.h>
#include <occi.h>

namespace dmlite {
  
class UserGroupDbOracle: public UserGroupDb {
public:
  /// Constructor.
  UserGroupDbOracle(oracle::occi::ConnectionPool* pool,
                    oracle::occi::Connection* conn) throw(DmException);
  
  /// Destructor.
  ~UserGroupDbOracle() throw(DmException);
  
  // Override
  
  std::string getImplId(void) throw();

  SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);
  
  GroupInfo newGroup(const std::string& gname) throw (DmException);
  GroupInfo getGroup(gid_t gid) throw (DmException);
  GroupInfo getGroup(const std::string& groupName) throw (DmException);

  UserInfo newUser(const std::string& uname, const std::string& ca) throw (DmException);
  UserInfo getUser(uid_t uid) throw (DmException);
  UserInfo getUser(const std::string& userName) throw (DmException);

  void getIdMap(const std::string& userName,
                const std::vector<std::string>& groupNames,
                UserInfo* user,
                std::vector<GroupInfo>* groups) throw (DmException);
  
protected:
  /// The Oracle connection pool.
  oracle::occi::ConnectionPool* pool_;

  /// The Oracle connection
  oracle::occi::Connection* conn_;

private:
};
  
};

#endif	// USERGROUPDBMYSQL_H
