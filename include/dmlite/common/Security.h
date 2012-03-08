/// @file    include/dmlite/common/Security.h
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef SECURITY_H
#define	SECURITY_H

#include <dmlite/dm_types.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace dmlite {

/// Return true if gid is in the set of groups.
bool gidInGroups(gid_t gid, const std::vector<GroupInfo> &groups);

/// Check if a specific user has the demanded rights.
/// @param user   The user.
/// @param group  The user main group.
/// @param groups A set with the secundary groups.
/// @param acl    A string with the ACL to check.
/// @param stat   A struct stat which mode will be checked.
/// @param mode   The mode to be checked.
/// @return       0 if the mode is allowed, 1 if not.
int checkPermissions(const UserInfo& user, const GroupInfo& group,
                     const std::vector<GroupInfo>& groups,
                     const std::string& acl, const struct stat& stat,
                     mode_t mode);

/// Wrapper for checkPermissions accepting NULL pointers
inline int checkPermissions(const UserInfo& user, const GroupInfo& group,
                            const std::vector<GroupInfo>& groups,
                            const char* acl, const struct stat& stat,
                            mode_t mode)
{
  if (acl == 0x00) acl = "";
  return checkPermissions(user, group, groups, std::string(acl), stat, mode);
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
std::string serializeAcl(const std::vector<Acl>& acls);
};

#endif	// SECURITY_H

