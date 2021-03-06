/// @file    utils/Security.cpp
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <algorithm>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <cctype>
#include <cstring>
#include <dmlite/common/errno.h>
#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/utils/security.h>
#include <map>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <sstream>
#include "utils/logger.h"

using namespace dmlite;

const uint8_t AclEntry::kUserObj;
const uint8_t AclEntry::kUser;
const uint8_t AclEntry::kGroupObj;
const uint8_t AclEntry::kGroup;
const uint8_t AclEntry::kMask;
const uint8_t AclEntry::kOther;
const uint8_t AclEntry::kDefault;

struct MapFileEntry {
  time_t      lastModified;
  std::map<std::string, std::string> voForDn;
  boost::shared_mutex mutex;
};

typedef std::map<std::string, struct MapFileEntry*> MapFileCache;


bool AclEntry::operator == (const AclEntry& e) const
{
  return this->id   == e.id &&
         this->perm == e.perm &&
         this->type == e.type;
}



bool AclEntry::operator != (const AclEntry& e) const
{
  return !(*this == e);
}



bool AclEntry::operator < (const AclEntry& e) const
{
  return ((this->type < e.type) ||
          (this->type == e.type && this->id < e.id));
}



bool AclEntry::operator > (const AclEntry& e) const
{
  return *this < e;
}



Acl::Acl() throw ()
{

}



Acl::Acl(const std::string& aclStr) throw ()
{
  AclEntry acl;

  size_t i = 0;
  while (i < aclStr.length()) {
    acl.type = aclStr[i++] - '@';
    acl.perm = aclStr[i++] - '0';
    acl.id   = atoi(aclStr.c_str() + i);
    this->push_back(acl);
    if ((i = aclStr.find(',', i)) != std::string::npos)
      ++i;
  }
}



Acl::Acl(const Acl& parent, uid_t uid, gid_t gid, mode_t cmode, mode_t* fmode) throw ()
{
  bool thereIsMask = parent.has(AclEntry::kDefault | AclEntry::kMask);
  Acl::const_iterator i;

  // If directory, or default mask
  if (thereIsMask || S_ISDIR(*fmode)) {
    AclEntry entry;

    for (i = parent.begin(); i != parent.end(); ++i) {
      if (i->type & AclEntry::kDefault) {
        entry.id   = i->id;
        entry.type = i->type & ~AclEntry::kDefault;

        switch (i->type) {
          case AclEntry::kDefault | AclEntry::kUserObj:
            *fmode     = (*fmode & 0177077) | (cmode & i->perm << 6);
            entry.id   = uid;
            entry.perm = i->perm & (cmode >> 6 & 7);
            break;
          case AclEntry::kDefault | AclEntry::kGroupObj:
            *fmode     = (*fmode & 0177707) | (cmode & i->perm << 3);
            entry.id   = gid;
            entry.perm = i->perm & (cmode >> 3 & 7);
            break;
          case AclEntry::kDefault | AclEntry::kOther:
            *fmode     = (*fmode & 0177770) | (cmode & i->perm);
            entry.perm = i->perm & (cmode & 7);
            break;
          default:
            break;
        }

        this->push_back(entry);
        // Need to copy defaults for directories
        if (S_ISDIR(*fmode))
          this->push_back(*i);
      }
    }
  }
  // Else, just set the mode
  else {
    for (i = parent.begin(); i != parent.end(); ++i) {
      switch (i->type) {
        case AclEntry::kDefault | AclEntry::kUserObj:
          *fmode   = (*fmode & 0177077) | (cmode & i->perm << 6);
          break;
        case AclEntry::kDefault | AclEntry::kGroupObj:
          *fmode   = (*fmode & 0177707) | (cmode & i->perm << 3);
          break;
        case AclEntry::kDefault | AclEntry::kOther:
          *fmode   = (*fmode & 0177770) | (cmode & i->perm);
          break;
        default:
          break;
      }
    }
  }
}



int Acl::has(uint8_t type) const throw ()
{
  for (unsigned i = 0; i < this->size(); ++i) {
    if ((*this)[i].type == type)
      return i;
  }
  return -1;
}



static bool aclCompare(const AclEntry& a, const AclEntry& b)
{
  if (a.type == b.type) return a.id < b.id;
  return a.type < b.type;
}



std::string Acl::serialize() const throw ()
{
  // First, we need a sorted copy of acls by type
  Acl copy(*this);
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



void Acl::validate() const throw (DmException)
{
  if (this->empty())
    return;

  int ndefs = 0;
  int ndg = 0;
  int ndgo = 0;
  int ndm = 0;
  int ndo = 0;
  int ndu = 0;
  int nduo = 0;
  int ng = 0;
  int ngo = 0;
  int nm = 0;
  int no = 0;
  int nu = 0;
  int nuo = 0;
  Acl::const_iterator i;

  for (i = this->begin(); i != this->end(); ++i) {
    switch (i->type) {
      case AclEntry::kUserObj:
        nuo++;
        break;
      case AclEntry::kUser:
        nu++;
        break;
      case AclEntry::kGroupObj:
        ngo++;
        break;
      case AclEntry::kGroup:
        ng++;
        break;
      case AclEntry::kMask:
        nm++;
        break;
      case AclEntry::kOther:
        no++;
        break;
      case AclEntry::kDefault | AclEntry::kUserObj:
        ndefs++;
        nduo++;
        break;
      case AclEntry::kDefault | AclEntry::kUser:
        ndefs++;
        ndu++;
        break;
      case AclEntry::kDefault | AclEntry::kGroupObj:
        ndefs++;
        ndgo++;
        break;
      case AclEntry::kDefault | AclEntry::kGroup:
        ndefs++;
        ndg++;
        break;
      case AclEntry::kDefault | AclEntry::kMask:
        ndefs++;
        ndm++;
        break;
      case AclEntry::kDefault | AclEntry::kOther:
        ndefs++;
        ndo++;
        break;
      default:
        throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                          "Invalid ACL type: %c", i->type);
    }
    // Check perm
    if (i->perm > 7)
      throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                        "Invalid permission: %d", i->perm);

    // Check it isn't duplicated
    if (i != this->begin()) {
      if (i->type == (i - 1)->type && i->id == (i - 1)->id)
        throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                          "Duplicated USER or GROUP entry: %c%d", i->type, i->id);
    }
  }

  // There must be one and only one of each type USER_OBJ, GROUP_OBJ, OTHER
  if (nuo != 1 || ngo != 1 || no != 1)
    throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                      "There must be one and only one of each type USER_OBJ, GROUP_OBJ, OTHER");

  // If there is any USER or GROUP entry, there must be a MASK entry
  if ((nu || ng) && nm != 1)
    throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                      "If there is any USER or GROUP entry, there must be a MASK entry");

  // If there are any default ACL entries, there must be one and only one
  // entry of each type DEF_USER_OBJ, DEF_GROUP_OBJ, DEF_OTHER
  if (ndefs && (nduo != 1 || ndgo != 1 || ndo != 1))
    throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                      "If there are any default ACL entries, there must be one and only one entry of each type DEF_USER_OBJ, DEF_GROUP_OBJ, DEF_OTHER");

  if ((ndu || ndg) && ndm != 1)
    throw DmException(DMLITE_SYSERR(DMLITE_INVALID_ACL),
                      "If there is any default USER or default GROUP entry, there must be a default MASK entry");
}



bool dmlite::hasGroup(const std::vector<GroupInfo>& groups, gid_t gid)
{
  std::vector<GroupInfo>::const_iterator i;
  for (i = groups.begin(); i != groups.end(); ++i) {
    gid_t g = i->getUnsigned("gid");
    long  banned;
    if (i->hasField("banned"))
      banned = i->getLong("banned");
    else
      banned = 0;

    if (g == gid && !banned)
      return true;
  }

  return false;
}



int dmlite::checkPermissions(const SecurityContext* context,
                             const Acl& acl, const struct stat &stat,
                             mode_t mode)
{
  int iacl;
  uint8_t  aclMask = 0x7F;
  uid_t    aclId;
  int      accPerm = 0;
  int      nGroups = 0;

  // If Context is NULL, abort
  if (context == 0)
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "Security context not initialized!!");

  // Extract used metadata
  uid_t uid = context->user.getUnsigned("uid");

  // Root can do anything
  if (uid == 0)
    return 0;

  // Check if the user is banned
  long banned = context->user.getLong("banned");

  // If user's primary group is banned, the user is also banned
  if (context->groups.size() && context->groups[0].getLong("banned")) {
    Log(Logger::Lvl2, Logger::unregistered, Logger::unregisteredname, "Group" << context->groups[0].name << " is banned for user " << context->user.name);
    banned = 1;
  }

  // Banned user, rejected
  if (banned) {
    Err("checkPermissions", "Banned user " << context->user.name);
    return 1;
  }

  // Check user. If owner, straigh-forward.
  if (stat.st_uid == uid) {

    return ((stat.st_mode & mode) != mode);
  }

  // There is no ACL's?
  if (acl.empty()) {
    Log(Logger::Lvl4, Logger::unregistered, Logger::unregisteredname, "Empty acl for " << stat.st_ino);

    // The user is not the owner
    mode >>= 3;
    // Belong to the group?
    if (!hasGroup(context->groups, stat.st_gid))
      mode >>= 3;

    return ((stat.st_mode & mode) != mode);
  }

  Log(Logger::Lvl4, Logger::unregistered, Logger::unregisteredname, "Acl for " << stat.st_ino << ": " << acl.serialize());
  // We have ACL's!
  // Adapted from Cns_acl.c

  // Get ACL_MASK if any
  if ((iacl = acl.has(AclEntry::kMask)) != -1)
    aclMask =  acl[iacl].perm;
  mode >>= 6;

  // check ACL_USER entries if any
  for (iacl = 0; iacl < (int) acl.size(); ++iacl)
  {
    if (acl[iacl].type > AclEntry::kUser)
      break;
    if (acl[iacl].type < AclEntry::kUser)
      continue;
    aclId = acl[iacl].id;
    if (uid == aclId) {
      Log(Logger::Lvl2, Logger::unregistered, Logger::unregisteredname,
          "Matched user ACL entry id " << acl[iacl].id << "' (" << acl[iacl].perm << ") mask:" << aclMask << " mode:" << mode);
      return ((acl[iacl].perm & aclMask & mode) != mode);
    }
    if (uid < aclId)
      break;
  }

  // Check GROUP
  iacl = acl.has(AclEntry::kGroupObj);
  if (hasGroup(context->groups, stat.st_gid)) {
    accPerm = acl[iacl].perm;
    nGroups++;
    if (aclMask == 0x7F) // no extended ACLs
      return ((accPerm & aclMask & mode) != mode);
  }

  // Check ACL_GROUP entries if any
  for ( ; iacl < (int) acl.size(); ++iacl) {
    if (acl[iacl].type > AclEntry::kGroup)
      break;
    if (acl[iacl].type < AclEntry::kGroup)
      continue;
    aclId = acl[iacl].id;
    if (hasGroup(context->groups, aclId)) {
      Log(Logger::Lvl2, Logger::unregistered, Logger::unregisteredname,
          "Matched group ACL entry: '" << acl[iacl].id << "' (" << acl[iacl].perm << ") mask:" << aclMask << " mode:" << mode);
      accPerm |= acl[iacl].perm;
      nGroups++;
    }
  }
  if (nGroups)
    return ((accPerm & aclMask & mode) != mode);

  // OTHER
  return ((stat.st_mode & mode) != mode);
}



static void _fillVoMapping(MapFileEntry* mfe, FILE* mf)
{
  char  buf[1024];
  char *p, *q;
  char *user, *vo;

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
      if (*p == '\0') continue; // No VO
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
}



std::string dmlite::voFromDn(const std::string& mapfile, const std::string& dn)
{
  static MapFileCache cache;
  static boost::shared_mutex cacheMutex;

  // Scope so cacheCheckLock is automatically unlocked!
  {
    // If the mapfile is not in the cache, insert it
    boost::upgrade_lock<boost::shared_mutex> cacheCheckLock(cacheMutex);
    if (cache.find(mapfile) == cache.end()) {
      boost::upgrade_to_unique_lock<boost::shared_mutex> cacheWriteLock(cacheCheckLock);
      // Need to make sure again it is not there, maybe we locked waiting for another
      // thread inserting the same!
      if (cache.find(mapfile) == cache.end()) {
        MapFileEntry* empty = new MapFileEntry();
        empty->lastModified = 0;
        cache.insert(std::pair<std::string, MapFileEntry*>(mapfile, empty));
      }
    }
  }

  MapFileEntry* mfe = NULL;
  {
    boost::shared_lock<boost::shared_mutex> cacheReadLock(cacheMutex);
    mfe = cache[mapfile];
    // cacheReadLock released inmediately! We already got what we wanted
  }

  // Check the last modification time
  struct stat mfStat;
  if (stat(mapfile.c_str(), &mfStat) == -1)
    throw DmException(DMLITE_SYSERR(errno),
                      "Can not stat " + mapfile);

  if (mfStat.st_mtime > mfe->lastModified) {
    FILE *mf;

    // Need to update the mapping!
    boost::unique_lock<boost::shared_mutex> writeLock(mfe->mutex, boost::try_to_lock);

    if (writeLock.owns_lock()) {
      if ((mf = fopen(mapfile.c_str(), "r")) == NULL)
        throw DmException(DMLITE_SYSERR(errno),
                          "Can not open " + mapfile);

      _fillVoMapping(mfe, mf);

      fclose (mf);
      mfe->lastModified = mfStat.st_mtime;
    }
    // If didn't get the lock, will block later on until the thread who
    // got it updates and releases
  }

  // Get the entry
  boost::shared_lock<boost::shared_mutex> readLock(mfe->mutex);
  if (mfe->voForDn.count(dn) == 0)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_USER_MAPPING),
                      "Could not map " + dn);

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



std::string dmlite::getCertificateSubject(const std::string& path)
{
  X509*       certificate;
  BIO*        in = BIO_new(BIO_s_file());
  const char *cpath = path.c_str();

  if (BIO_read_filename(in, cpath) < 0) {
    BIO_free_all(in);
    throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                      "Could not read the certificate (BIO: %s)",
                      cpath);
  }

  certificate = PEM_read_bio_X509_AUX(in, NULL, NULL, NULL);
  BIO_free_all(in);
  if (certificate == NULL) {
    throw DmException(DMLITE_SYSERR(DMLITE_INTERNAL_ERROR),
                      "Could not read the certificate (X509: %s)",
                      cpath);
  }
  char * subj = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
  std::string subject = std::string(subj);
  OPENSSL_free(subj);
  X509_free(certificate);

  return subject;
}



// Base 64 encoding
static unsigned base64_encode(const char *input, unsigned length, char* output)
{
  BIO *bmem, *b64;
  BUF_MEM *bptr;
  unsigned written;

  b64  = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64  = BIO_push(b64, bmem);
  BIO_write(b64, input, length);
  BIO_ctrl(b64, BIO_CTRL_FLUSH, 0, NULL);
  BIO_get_mem_ptr(b64, &bptr);

  written = bptr->length - 1;
  memcpy(output, bptr->data, written);
  output[written] = 0;

  BIO_free_all(b64);

  return written;
}



std::string dmlite::generateToken(const std::string& id, const std::string& pfn,
                                  const std::string& passwd, time_t lifetime,
                                  bool write)
{
  char     buffer1[1024];
  char     buffer2[1024];
  unsigned inl, outl;

  time_t expires = time(NULL) + lifetime;

  // Concatenate
  inl = snprintf((char*)buffer1, sizeof(buffer1),
                 "%s\035%s\035%ld\035%d", pfn.c_str(), id.c_str(), expires, write);

  // HMAC auth code
  HMAC(EVP_sha1(), passwd.c_str(), passwd.length(),
       (unsigned char*)buffer1, inl,
       (unsigned char*)buffer2, &outl);

  // Base64
  outl = base64_encode(buffer2, outl, buffer1);
  snprintf(buffer1 + outl, sizeof(buffer1) - outl, "@%ld@%d", expires, write);

  // Done
  return std::string(buffer1);
}



TokenResult dmlite::validateToken(const std::string& token, const std::string& id,
                                  const std::string& pfn,
                                  const std::string& passwd, bool write)
{
  char     buffer1[1024];
  char     buffer2[1024];
  char     buffer1nouser[1024];
  char     buffer2nouser[1024];
  long     expires;
  unsigned tokenForWrite;
  unsigned inl, outl;
  unsigned inlnouser, outlnouser;

  // Search for '='
  size_t separator = token.find('@');
  if (separator == std::string::npos)
    return kTokenMalformed;

  // Grab expiration and write mode
  sscanf(token.c_str() + separator + 1, "%ld@%d", &expires, &tokenForWrite);

  // Generate validation string
  inl = snprintf(buffer1, sizeof(buffer1),
                 "%s\035%s\035%ld\035%d", pfn.c_str(), id.c_str(), expires, tokenForWrite);
  HMAC(EVP_sha1(), passwd.c_str(), passwd.length(),
       (unsigned char*)buffer1, inl,
       (unsigned char*)buffer2, &outl);
  outl = base64_encode(buffer2, outl, buffer1);

  // Generate user-independent validation string
  inlnouser = snprintf(buffer1nouser, sizeof(buffer1nouser),
                 "%s\035%s\035%ld\035%d", pfn.c_str(), kGenericUser.c_str(), expires, tokenForWrite);
  HMAC(EVP_sha1(), passwd.c_str(), passwd.length(),
       (unsigned char*)buffer1nouser, inlnouser,
       (unsigned char*)buffer2nouser, &outlnouser);
  outlnouser = base64_encode(buffer2nouser, outlnouser, buffer1nouser);

  // Compare validation strings
  if (strncmp(buffer1, token.c_str(), outl) != 0 &&
      strncmp(buffer1nouser, token.c_str(), outlnouser) != 0)
    return kTokenInvalid;

  // Expiration and mode
  if ((expires < time(NULL)))
    return kTokenExpired;

  if (write && !tokenForWrite)
    return kTokenInvalidMode;

  // We are good!
  return kTokenOK;
}
