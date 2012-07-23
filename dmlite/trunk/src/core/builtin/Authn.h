/// @file     core/builtin/Authn.h
/// @brief    User and group mapping using the system's ones.
/// @detailed This will be used by default when no other Authn
///           implementeation is loaded.
/// @author   Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef BUILTIN_AUTHN_H
#define	BUILTIN_AUTHN_H

#include <dmlite/cpp/authn.h>

namespace dmlite {
  
  class BuiltInAuthn: public Authn {
   public:
    BuiltInAuthn();
    ~BuiltInAuthn();

    std::string getImplId(void) const throw();

    SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);

    GroupInfo newGroup(const std::string& gname) throw (DmException);
    GroupInfo getGroup(gid_t gid) throw (DmException);
    GroupInfo getGroup(const std::string& groupName) throw (DmException);

    UserInfo newUser(const std::string& uname)    throw (DmException);
    UserInfo getUser(const std::string& userName) throw (DmException);
    UserInfo getUser(const std::string& userName, gid_t* group) throw (DmException);

    void getIdMap(const std::string& userName,
                  const std::vector<std::string>& groupNames,
                  UserInfo* user,
                  std::vector<GroupInfo>* groups) throw (DmException);
  };

  class BuiltInAuthnFactory: public AuthnFactory {
   public:
    BuiltInAuthnFactory();
    ~BuiltInAuthnFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);

    Authn* createAuthn(PluginManager* pm) throw (DmException);
  };
  
};

#endif	// BUILTIN_AUTHN_H
