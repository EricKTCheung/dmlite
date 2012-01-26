/// @file   include/dm/dm_auth.h
/// @brief  Security API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_AUTH_H
#define	DMLITE_AUTH_H

#include <string>
#include <vector>
#include "dm_exceptions.h"

namespace dmlite {
  
class AuthBase {
public:
  /// Destructor.
  virtual ~AuthBase();

  /// Set the user ID that will perform the actions.
  /// @param uid The UID.
  /// @param gid The GID.
  /// @param dn  The full DN (i.e. /DC=ch/DC=cern/OU=Organic Units/...).
  virtual void setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException) = 0;

  /// Set the user associated VO data.
  /// @param vo     The main Virtual Organization (i.e. dteam).
  /// @param fqans  The FQANS.
  virtual void setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException) = 0;
};

};

#endif	// DMLITE_AUTH_H
