/// @file   AuthnMySql.h
/// @brief  MySQL Authn Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef AUTHNMYSQL_H
#define	AUTHNMYSQL_H

#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <mysql/mysql.h>
#include "utils/MySqlWrapper.h"

namespace dmlite {
  
class NsMySqlFactory;
  
  class AuthnMySql: public Authn {
   public:
    /// Constructor.
    AuthnMySql(NsMySqlFactory* factory,
               const std::string& db,
               const std::string& mapfile,
               bool hostDnIsRoot, const std::string& hostDn) throw(DmException);

    /// Destructor.
    ~AuthnMySql();

    // Override

    std::string getImplId(void) const throw();

    SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);
    SecurityContext* createSecurityContext(void) throw (DmException);

    GroupInfo newGroup   (const std::string& gname) throw (DmException);
    GroupInfo getGroup   (const std::string& groupName) throw (DmException);
    GroupInfo getGroup   (const std::string& key,
                          const boost::any& value) throw (DmException);
    void      updateGroup(const GroupInfo& group) throw (DmException);
    void      deleteGroup(const std::string& groupName) throw (DmException);

    UserInfo newUser   (const std::string& uname) throw (DmException);
    UserInfo getUser   (const std::string& userName) throw (DmException);
    UserInfo getUser   (const std::string& key,
                        const boost::any& value) throw (DmException);
    void     updateUser(const UserInfo& user) throw (DmException);
    void     deleteUser(const std::string& userName) throw (DmException);
    
    std::vector<GroupInfo> getGroups(void) throw (DmException);
    std::vector<UserInfo>  getUsers (void) throw (DmException);

    void getIdMap(const std::string& userName,
                  const std::vector<std::string>& groupNames,
                  UserInfo* user,
                  std::vector<GroupInfo>* groups) throw (DmException);

   protected:
    /// The corresponding factory.
    NsMySqlFactory* factory_;

   private:
    /// NS DB.
    std::string nsDb_;

    /// Mapfile
    std::string mapFile_;
    
    /// Map host DN to root user (0)
    bool hostDnIsRoot_;

    /// Host DN
    std::string hostDn_;
  };
  
};

#endif // AUTHNMYSQL_H
