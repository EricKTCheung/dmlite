/// @file   DomeAdapterAuthn.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#ifndef DOME_ADAPTER_AUTHN_H
#define DOME_ADAPTER_AUTHN_H

#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include "DomeAdapter.h"
#include "utils/DomeTalker.h"
#include "DomeAdapterIdMapCache.hh"

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterAuthn : public Authn {
  public:
  	/// Constructor
    DomeAdapterAuthn(DomeAdapterFactory *factory);

    // Overload
    std::string getImplId(void) const throw ();

    SecurityContext* createSecurityContext(const SecurityCredentials& cred) throw (DmException);
    SecurityContext* createSecurityContext() throw (DmException);

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
    void uncachedGetIdMap(const std::string& userName,
                  const std::vector<std::string>& groupNames,
                  UserInfo* user,
                  std::vector<GroupInfo>* groups) throw (DmException);

    IdMapCache idmapCache;

    StackInstance* si_;
    const DomeCredentials emptycreds;
    DomeAdapterFactory* factory_;
  };
}


#endif
