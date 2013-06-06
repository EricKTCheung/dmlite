#include <dmlite/cpp/authn.h>
#include "NotImplemented.h"

using namespace dmlite;



AuthnFactory::~AuthnFactory()
{
  // Nothing
}



Authn* AuthnFactory::createAuthn(AuthnFactory* f, PluginManager* pm) throw (DmException)
{
  return f->createAuthn(pm);
}



FACTORY_NOT_IMPLEMENTED(Authn* AuthnFactory::createAuthn(PluginManager*) throw (DmException));



Authn::~Authn()
{
  // Nothing
}



NOT_IMPLEMENTED(SecurityContext* Authn::createSecurityContext(const SecurityCredentials&) throw (DmException));
NOT_IMPLEMENTED(SecurityContext* Authn::createSecurityContext(void) throw (DmException))
NOT_IMPLEMENTED(GroupInfo Authn::newGroup(const std::string&) throw (DmException));
NOT_IMPLEMENTED(GroupInfo Authn::getGroup(const std::string&) throw (DmException));
NOT_IMPLEMENTED(GroupInfo Authn::getGroup(const std::string&, const boost::any&) throw (DmException));
NOT_IMPLEMENTED(std::vector<GroupInfo> Authn::getGroups(void) throw (DmException));
NOT_IMPLEMENTED(void Authn::updateGroup(const GroupInfo&) throw (DmException));
NOT_IMPLEMENTED(void Authn::deleteGroup(const std::string&) throw (DmException));
NOT_IMPLEMENTED(UserInfo Authn::newUser(const std::string&) throw (DmException));
NOT_IMPLEMENTED(UserInfo Authn::getUser(const std::string&) throw (DmException));
NOT_IMPLEMENTED(UserInfo Authn::getUser(const std::string&, const boost::any&) throw (DmException));
NOT_IMPLEMENTED(std::vector<UserInfo> Authn::getUsers(void) throw (DmException));
NOT_IMPLEMENTED(void Authn::updateUser(const UserInfo&) throw (DmException));
NOT_IMPLEMENTED(void Authn::deleteUser(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void Authn::getIdMap(const std::string&, const std::vector<std::string>&,
                UserInfo*, std::vector<GroupInfo>*) throw (DmException));
