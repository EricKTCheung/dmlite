/// @file    include/dmlite/common/Security.h
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef SECURITY_H
#define	SECURITY_H

#include <dmlite/dm_auth.h>
#include <dmlite/dm_exceptions.h>
#include <dmlite/dm_types.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace dmlite {

/// Check if a specific user has the demanded rights.
/// @param context The security context.
/// @param acl     A string with the ACL to check.
/// @param stat    A struct stat which mode will be checked.
/// @param mode    The mode to be checked.
/// @return        0 if the mode is allowed, 1 if not.
int checkPermissions(const SecurityContext& contex,
                     const std::string& acl, const struct stat& stat,
                     mode_t mode);

/// Wrapper for checkPermissions accepting NULL pointers
inline int checkPermissions(const SecurityContext& context,
                            const char* acl, const struct stat& stat,
                            mode_t mode)
{
  if (acl == 0x00) acl = "";
  return checkPermissions(context, std::string(acl), stat, mode);
}

/// Get the VO from a full DN.
/// @param mapfile The file that contains the user => group mapping.
/// @param dn      The DN to parse.
/// @return        The mapped VO.
std::string voFromDn(const std::string& mapfile, const std::string& dn);

/// Get the VO from a role.
/// @param role The role.
/// @return     The VO.
std::string voFromRole(const std::string& role);

/// Get an array of Acl structs from the string serialization.
std::vector<Acl> deserializeAcl(const std::string& aclStr);

/// Get the string serialization of the array of acls.
std::string serializeAcl(const std::vector<Acl>& acl);

/// Validate an ACL string.
void validateAcl(const std::string& acl) throw (DmException);

/// Validate an ACL vector.
void validateAcl(const std::vector<Acl>& acl) throw (DmException);

/// Inherit ACL.
/// @param parentAcl The parent's ACL vector.
/// @param uid       The current user uid.
/// @param gid       The current user gid.
/// @param fmode     The current file mode. It will be modified to fit the inheritance.
/// @param mode      The creation mode.
/// @return          A vector with the inherited mode, and fmode set properly.
std::vector<Acl> inheritAcl(const std::vector<Acl>& parentAcl, uid_t uid, gid_t gid, mode_t* fmode, mode_t mode);

};

#endif	// SECURITY_H

