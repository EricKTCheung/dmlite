/// @file    common/Security.cpp
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cctype>
#include <cerrno>
#include <dmlite/dm_exceptions.h>
#include <dmlite/dm_errno.h>
#include <dmlite/common/Security.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <sstream>
#include <queue>

using namespace dmlite;

struct MapFileEntry {
  time_t      lastModified;
  std::map<std::string, std::string> voForDn;
};



bool dmlite::gidInGroups(gid_t gid, const std::vector<GroupInfo>& groups)
{
  std::vector<GroupInfo>::const_iterator i;

  for (i = groups.begin(); i != groups.end(); ++i)
    if (i->gid == gid && !i->banned)
      return true;
  return false;
}



int dmlite::checkPermissions(const UserInfo &user, const GroupInfo &group,
                             const std::vector<GroupInfo>& groups,
                             const std::string& acl, const struct stat &stat,
                             mode_t mode)
{
  size_t      iacl, p;
  char        aclMask = 0x7F;
  uid_t       aclId;
  int         accPerm;
  int         nGroups = 0;

  // Root can do anything
  if (user.uid == 0)
    return 0;

  // Banned user, rejected
  if (user.banned)
    return 1;

  // Check user. If owner, straigh-forward.
  if (stat.st_uid == user.uid)
    return ((stat.st_mode & mode) != mode);

  // There is no ACL's?
  if (acl.empty()) {
    // The user is not the owner
    mode >>= 3;
    // Belong to the group?
    if (!(stat.st_gid == group.gid && !group.banned) &&
        !gidInGroups(stat.st_gid, groups))
      mode >>= 3;

    return ((stat.st_mode & mode) != mode);
  }

  // We have ACL's!
  // Adapted from Cns_acl.c

  // Get ACL_MASK if any
  if ((iacl = acl.find(ACL_MASK|'@')) != std::string::npos)
    aclMask =  acl[iacl + 1] - '0';
  mode >>= 6;

  // check ACL_USER entries if any
  iacl = 0;
  for (iacl = 0; iacl != std::string::npos; iacl = p)
  {
    if (acl[iacl] - '@' > ACL_USER)
      break;
    p = acl.find(',', iacl);
    if (p != std::string::npos)
      p++;
    if (acl[iacl] - '@' < ACL_USER)
      continue;
    aclId = atoi(acl.substr(iacl + 2).c_str());
    if (user.uid == aclId)
      return ((acl[iacl + 1] & aclMask & mode) != mode);
    if (user.uid < aclId)
      break;
  }

  // Check GROUP
  iacl = acl.find(ACL_GROUP_OBJ|'@', iacl);
  if ((stat.st_gid == group.gid && !group.banned) ||
      gidInGroups(stat.st_gid, groups)) {
    accPerm = acl[iacl + 1];
    nGroups++;
    if (aclMask == 0x7F) // no extended ACLs
      return ((accPerm & aclMask & mode) != mode);
  }

  // Check ACL_GROUP entries if any
  for ( ; iacl != std::string::npos; iacl = p) {
    if (acl[iacl] - '@' > ACL_GROUP)
      break;
    p = acl.find(',', iacl);
    if (p != std::string::npos)
      p++;
    if (acl[iacl] - '@' < ACL_GROUP)
      continue;
    aclId = atoi (acl.substr(iacl + 2).c_str());
    if ((aclId == group.gid && !group.banned) ||
        gidInGroups(aclId, groups)) {
      accPerm |= acl[iacl + 1];
      nGroups++;
    }
  }
  if (nGroups)
    return ((accPerm & aclMask & mode) != mode);

  // OTHER
  return ((stat.st_mode & mode) != mode);
}



std::string dmlite::voFromDn(const std::string& mapfile, const std::string& dn)
{
  static std::map<std::string, struct MapFileEntry*> cache;

  MapFileEntry* mfe = cache[mapfile];
  if (mfe == 0x00) {
    mfe = new MapFileEntry();
    mfe->lastModified = 0;
    cache[mapfile] = mfe;
  }

  // Check the last modified time
  struct stat mfStat;
  if (stat(mapfile.c_str(), &mfStat) == -1)
    throw DmException(DM_NO_SUCH_FILE, "Can not stat " + mapfile);

  if (mfStat.st_mtime > mfe->lastModified) {
    // Need to update the mapping!
    static pthread_mutex_t update = PTHREAD_MUTEX_INITIALIZER;

    // Do the update
    if (pthread_mutex_trylock(&update) == 0) {
      char  buf[1024];
      char *p, *q;
      char *user, *vo;
      FILE *mf;

      if ((mf = fopen(mapfile.c_str(), "r")) == NULL)
        throw DmException(DM_NO_SUCH_FILE, "Can not open " + mapfile);

      while (fgets(buf, sizeof(buf), mf)) {
        buf[strlen (buf) - 1] = '\0';
        p = buf;

        // Skip leading blanks
        while (isspace(*p))
          p++;

        if (*p == '\0') continue; // Empty line
        if (*p == '#') continue;  // Comment

        if (*p == '"') {
          q = p + 1;
          if ((p = strrchr (q, '"')) == NULL) continue;
        }
        else {
          q = p;
          while (!isspace(*p) && *p != '\0')
            p++;
          if (*p == '\0') continue;	// No VO
        }

        *p = '\0';
        user = q;
        p++;

        // Skip blanks between DN and VO
        while (isspace(*p))
          p++;
        q = p;

        while (!isspace(*p) && *p != '\0' && *p != ',')
          p++;
        *p = '\0';
        vo = q;

        // Insert
        mfe->voForDn[user] = vo;
      }

      fclose (mf);
      mfe->lastModified = mfStat.st_mtime;
      pthread_mutex_unlock(&update);
    }
    // Someone else is updating, so wait until they are done
    else {
      pthread_mutex_lock(&update);
      pthread_mutex_unlock(&update);
    }
  }

  // Done here
  if (mfe->voForDn.count(dn) == 0)
    throw DmException(DM_NO_USER_MAPPING, "Could not map " + dn);

  return mfe->voForDn[dn];
}



std::string dmlite::voFromRole(const std::string& role)
{
  std::string vo(role);
  size_t      i;

  if (vo[0] == '/')
    vo.erase(0, 1);

  if ((i = vo.find("/Role=NULL")) != std::string::npos)
    return vo.substr(0, i);
  else if ((i = vo.find("/Capability=NULL")) != std::string::npos)
    return vo.substr(0, i);
  else
    return vo;
}



std::vector<Acl> dmlite::deserializeAcl(const std::string& aclStr)
{
  std::vector<Acl> acls;
  Acl              acl;

  size_t i = 0;
  while (i < aclStr.length()) {
    acl.type = aclStr[i++] - '@';
    acl.perm = aclStr[i++] - '0';
    acl.id   = atoi(aclStr.c_str() + i);
    acls.push_back(acl);
    if ((i = aclStr.find(',', i)) != std::string::npos)
      ++i;
  }

  return acls;
}



static bool aclCompare(const Acl& a, const Acl& b)
{
  return a.type < b.type;
}



std::string dmlite::serializeAcl(const std::vector<Acl>& acls)
{
  // First, we need a sorted copy of acls by type
  std::vector<Acl> copy(acls);
  std::sort(copy.begin(), copy.end(), aclCompare);

  // Build the ACL string from the sorted
  std::stringstream aclStr;
  size_t            i;

  for (i = 0; i < copy.size(); ++i) {
    aclStr << (char)('@' + copy[i].type)
           << (char)('0' + copy[i].perm)
           << copy[i].id;

    if (i + 1 < copy.size())
      aclStr <<  ',';
  }

  // Return
  return aclStr.str();
}
