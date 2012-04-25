/// @file   include/dmlite/dm_auth.h
/// @brief  Security API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_AUTH_H
#define	DMLITE_AUTH_H

#include <string>
#include <vector>
#include "dm_exceptions.h"
#include "dm_types.h"

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

};

#endif	// DMLITE_AUTH_H
