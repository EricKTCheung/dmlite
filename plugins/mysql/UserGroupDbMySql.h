/// @file    plugins/mysql/UserGroupDbMySql.h
/// @brief   MySQL UserGroupDB Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef USERGROUPDBMYSQL_H
#define	USERGROUPDBMYSQL_H

#include <dmlite/cpp/utils/dm_poolcontainer.h>
#include <dmlite/cpp/dm_auth.h>
#include <mysql/mysql.h>
#include "MySqlWrapper.h"

namespace dmlite {
  
class UserGroupDbMySql: public UserGroupDb {
public:
  /// Constructor.
  UserGroupDbMySql(PoolContainer<MYSQL*>* connPool,
                   const std::string& db,
                   const std::string& mapfile) throw(DmException);
  
  /// Destructor.
  ~UserGroupDbMySql() throw(DmException);
  
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
  /// The MySQL connection
  MYSQL* conn_;

  /// The connection pool.
  PoolContainer<MYSQL*>* connectionPool_;

private:
  /// NS DB.
  std::string nsDb_;
  
  /// Mapfile
  std::string mapFile_;
};
  
};

#endif	// USERGROUPDBMYSQL_H
