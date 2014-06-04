/// @file    common/Security.cpp
/// @brief   Security functionality shared between modules.
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <dmlite/common/dm_config.h>
#include <dmlite/common/dm_errno.h>
#include <dmlite/cpp/dm_auth.h>
#include <dmlite/cpp/dm_exceptions.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <map>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <sstream>

using namespace dmlite;

struct MapFileEntry {
  time_t      lastModified;
  std::map<std::string, std::string> voForDn;
};



int dmlite::checkPermissions(const SecurityContext* context,
                             const std::string& acl, const struct stat &stat,
                             mode_t mode)
{
  size_t      iacl, p;
  char        aclMask = 0x7F;
  uid_t       aclId;
  int         accPerm;
  int         nGroups = 0;
  
  // If Context is NULL, die badly
  if (context == 0)
    throw DmException(DM_INVALID_VALUE, "Security context not initialized!!");

  // Root can do anything
  if (context->getUser().uid == 0)
    return 0;

  // Banned user, rejected
  if (context->getUser().banned)
    return 1;

  // Check user. If owner, straigh-forward.
  if (stat.st_uid == context->getUser().uid)
    return ((stat.st_mode & mode) != mode);

  // There is no ACL's?
  if (acl.empty()) {
    // The user is not the owner
    mode >>= 3;
    // Belong to the group?
    if (!context->hasGroup(stat.st_gid))
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
    if (context->getUser().uid == aclId)
      return ((acl[iacl + 1] & aclMask & mode) != mode);
    if (context->getUser().uid < aclId)
      break;
  }

  // Check GROUP
  iacl = acl.find(ACL_GROUP_OBJ|'@', iacl);
  if (context->hasGroup(stat.st_gid)) {
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
    if (context->hasGroup(aclId)) {
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
  static std::map<std::string, struct MapFileEntry> cache;

  if (cache.find(mapfile) == cache.end()) {
    struct MapFileEntry empty;
    empty.lastModified = 0;
    cache.insert(std::pair<std::string, struct MapFileEntry>(mapfile, empty));
  }

  MapFileEntry* mfe = &cache[mapfile];

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


// This hack allows to parse only once the file
static std::string initHostDN(void)
{
  X509*       hostX509;
  BIO*        in = BIO_new(BIO_s_file());

  if (BIO_read_filename(in, HOST_CERTIFICATE) < 0) {
    BIO_free_all(in);
    throw DmException(DM_INTERNAL_ERROR,
                      "Could not read the host certificate (BIO: %s)", HOST_CERTIFICATE);
  }

  hostX509 = PEM_read_bio_X509_AUX(in, NULL, NULL, NULL);
  BIO_free_all(in);
  if (hostX509 == NULL) {
    throw DmException(DM_INTERNAL_ERROR,
                      "Could not read the host certificate (X509: %s)", HOST_CERTIFICATE);
  }

  std::string hostDN = hostX509->name;
  X509_free(hostX509);
  
  CRYPTO_cleanup_all_ex_data();
  
  return hostDN;
}



std::string dmlite::getHostDN(void)
{
  static const std::string hostDN = initHostDN();
  return hostDN;
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



void dmlite::validateAcl(const std::string& acl) throw (DmException)
{
  dmlite::validateAcl(dmlite::deserializeAcl(acl));
}



void dmlite::validateAcl(const std::vector<Acl>& acls) throw (DmException)
{
  if (acls.empty())
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
  std::vector<Acl>::const_iterator i;

  for (i = acls.begin(); i != acls.end(); ++i) {
    switch (i->type) {
      case ACL_USER_OBJ:
        nuo++;
        break;
      case ACL_USER:
        nu++;
        break;
      case ACL_GROUP_OBJ:
        ngo++;
        break;
      case ACL_GROUP:
        ng++;
        break;
      case ACL_MASK:
        nm++;
        break;
      case ACL_OTHER:
        no++;
        break;
      case ACL_DEFAULT | ACL_USER_OBJ:
        ndefs++;
        nduo++;
        break;
      case ACL_DEFAULT | ACL_USER:
        ndefs++;
        ndu++;
        break;
      case ACL_DEFAULT | ACL_GROUP_OBJ:
        ndefs++;
        ndgo++;
        break;
      case ACL_DEFAULT | ACL_GROUP:
        ndefs++;
        ndg++;
        break;
      case ACL_DEFAULT | ACL_MASK:
        ndefs++;
        ndm++;
        break;
      case ACL_DEFAULT | ACL_OTHER:
        ndefs++;
        ndo++;
        break;
      default:
        throw DmException(DM_INVALID_ACL, "Invalid ACL type: %c", i->type);
    }
    // Check perm
    if (i->perm > 7)
      throw DmException(DM_INVALID_ACL, "Invalid permission: %d", i->perm);

    // Check it isn't duplicated
    if (i != acls.begin()) {
      if (i->type == (i - 1)->type && i->id == (i - 1)->id)
        throw DmException(DM_INVALID_ACL, "Duplicated USER or GROUP entry: %c%d", i->type, i->id);
    }
  }

  // There must be one and only one of each type USER_OBJ, GROUP_OBJ, OTHER
  if (nuo != 1 || ngo != 1 || no != 1)
    throw DmException(DM_INVALID_ACL,
                      "There must be one and only one of each type USER_OBJ, GROUP_OBJ, OTHER");
  
  // If there is any USER or GROUP entry, there must be a MASK entry
  if ((nu || ng) && nm != 1)
    throw DmException(DM_INVALID_ACL,
                      "If there is any USER or GROUP entry, there must be a MASK entry");

  // If there are any default ACL entries, there must be one and only one
  // entry of each type DEF_USER_OBJ, DEF_GROUP_OBJ, DEF_OTHER
  if (ndefs && (nduo != 1 || ndgo != 1 || ndo != 1))
    throw DmException(DM_INVALID_ACL,
                      "If there are any default ACL entries, there must be one and only one entry of each type DEF_USER_OBJ, DEF_GROUP_OBJ, DEF_OTHER");

  if ((ndu || ndg) && ndm != 1)
    throw DmException(DM_INVALID_ACL,
                      "If there is any default USER or default GROUP entry, there must be a default MASK entry");
}



std::vector<Acl> dmlite::inheritAcl(const std::vector<Acl>& parentAcl, uid_t uid, gid_t gid, mode_t* fmode, mode_t mode)
{
  std::vector<Acl> childAcl;
  std::vector<Acl>::const_iterator i;
  bool thereIsMask = false;

  // Search for the mask
  for (i = parentAcl.begin(); i != parentAcl.end() && !thereIsMask; ++i) {
    thereIsMask = (i->type == (ACL_DEFAULT | ACL_MASK));
  }

  // If directory, or default mask
  if (thereIsMask || S_ISDIR(*fmode)) {
    Acl acl;
    
    for (i = parentAcl.begin(); i != parentAcl.end(); ++i) {
      if (i->type & ACL_DEFAULT) {
        acl.id   = i->id;
        acl.type = i->type & ~ACL_DEFAULT;

        switch (i->type) {
          case ACL_DEFAULT | ACL_USER_OBJ:
            *fmode   = (*fmode & 0177077) | (mode & i->perm << 6);
            acl.id   = uid;
            acl.perm = i->perm & (mode >> 6 & 7);
            break;
          case ACL_DEFAULT | ACL_GROUP_OBJ:
            *fmode   = (*fmode & 0177707) | (mode & i->perm << 3);
            acl.id   = gid;
            acl.perm = i->perm & (mode >> 3 & 7);
            break;
          case ACL_DEFAULT | ACL_OTHER:
            *fmode   = (*fmode & 0177770) | (mode & i->perm);
            acl.perm = i->perm & (mode & 7);
            break;
        }

        childAcl.push_back(acl);
        childAcl.push_back(*i); // Need to copy defaults
      }
    }
  }
  // Else, just set the mode
  else {
    for (i = parentAcl.begin(); i != parentAcl.end(); ++i) {
      switch (i->type) {
        case ACL_DEFAULT | ACL_USER_OBJ:
          *fmode   = (*fmode & 0177077) | (mode & i->perm << 6);
          break;
        case ACL_DEFAULT | ACL_GROUP_OBJ:
          *fmode   = (*fmode & 0177707) | (mode & i->perm << 3);
          break;
        case ACL_DEFAULT | ACL_OTHER:
          *fmode   = (*fmode & 0177770) | (mode & i->perm);
          break;
      }
    }
  }

  return childAcl;
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
  // Avoid a memory leak coming from CRYPTO
  // See: http://www.mail-archive.com/openssl-dev@openssl.org/msg30097.html
  CRYPTO_cleanup_all_ex_data();
  
  return written;
}



std::string dmlite::generateToken(const std::string& id, const std::string& pfn, const std::string& passwd, time_t lifetime, bool write)
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



bool dmlite::validateToken(const std::string& token, const std::string& id, const std::string& pfn, const std::string& passwd, bool write)
{
  char     buffer1[1024];
  char     buffer2[1024];
  long     expires;
  unsigned tokenForWrite;
  unsigned inl, outl;

  // Search for '='
  size_t separator = token.find('@');
  if (separator == std::string::npos)
    return false;

  // Grab expiration and write mode
  sscanf(token.c_str() + separator + 1, "%ld@%d", &expires, &tokenForWrite);

  // Generate validation string
  inl = snprintf(buffer1, sizeof(buffer1),
                 "%s\035%s\035%ld\035%d", pfn.c_str(), id.c_str(), expires, tokenForWrite);
  HMAC(EVP_sha1(), passwd.c_str(), passwd.length(),
       (unsigned char*)buffer1, inl,
       (unsigned char*)buffer2, &outl);
  outl = base64_encode(buffer2, outl, buffer1);

  // Compare validation strings
  if (strncmp(buffer1, token.c_str(), outl) != 0)
    return false;

  // Expiration and mode
  if ((expires < time(NULL)) || (write && !tokenForWrite)) {
    return false;
  }

  // We are good!
  return true;
}