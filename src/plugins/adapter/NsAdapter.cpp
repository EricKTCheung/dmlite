/// @file   NsAdapter.cpp
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <serrno.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/checksums.h>
#include <dmlite/cpp/utils/urls.h>
#include <vector>
#include <dlfcn.h>

#include "Adapter.h"
#include "FunctionWrapper.h"
#include "NsAdapter.h"

using namespace dmlite;



inline ExtendedStat& fillChecksumInXattr(ExtendedStat& xstat)
{
  if (!xstat.csumtype.empty()) {
    std::string csumXattr("checksum.");
    csumXattr += checksums::fullChecksumName(xstat.csumtype);
    if (!xstat.hasField(csumXattr))
      xstat[csumXattr] = xstat.csumvalue;
  }
  return xstat;
}

static void ns_init_routine()
{
  typedef int (*set_selectsrvr_t)(int);
  set_selectsrvr_t ns_set_srvr;

  ns_set_srvr = reinterpret_cast<set_selectsrvr_t>(reinterpret_cast<size_t>
    (dlsym(RTLD_DEFAULT,"dpns_set_selectsrvr")));

  if (ns_set_srvr) {
    /* disable nameserver hostname selection based on path */
    ns_set_srvr(1);
  }
}

NsAdapterCatalog::NsAdapterCatalog(unsigned retryLimit, bool hostDnIsRoot, std::string hostDn)
  throw (DmException): Catalog(), si_(NULL), retryLimit_(retryLimit),
                       fqans_(NULL), nFqans_(0), hostDnIsRoot_(hostDnIsRoot), hostDn_(hostDn), secCtx_(NULL)
{
  static pthread_once_t once_control = PTHREAD_ONCE_INIT;
  pthread_once(&once_control, ns_init_routine);
}



NsAdapterCatalog::~NsAdapterCatalog()
{
  if (this->fqans_ != NULL) {
    for (unsigned i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
}


// setDpnsApiIdentity should be called by any public methods of the catalog
// which use the dpns api; this method makes sure that the dpns' api per-thread
// identity is set according to the content of the catalog's security context
//
void NsAdapterCatalog::setDpnsApiIdentity()
{
  FunctionWrapper<int> reset(dpns_client_resetAuthorizationId);
  reset();

  // can not do any more if there is no security context
  if (!secCtx_) { return; }

  uid_t uid = secCtx_->user.getUnsigned("uid");

  // nothing more to do for root
  if (uid == 0) { return; }

  FunctionWrapper<int, uid_t, gid_t, const char*, char*>(
      dpns_client_setAuthorizationId,
        uid, secCtx_->groups[0].getUnsigned("gid"), "GSI",
        (char*)secCtx_->user.name.c_str())();

  if (fqans_ && nFqans_) {
    FunctionWrapper<int, char*, char**, int>(
        dpns_client_setVOMS_data,
          fqans_[0], fqans_, nFqans_)();
  }
}


std::string NsAdapterCatalog::getImplId() const throw ()
{
  return std::string("NsAdapterCatalog");
}



void NsAdapterCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}



SecurityContext* NsAdapterCatalog::createSecurityContext(const SecurityCredentials& cred) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.clientName, cred.fqans, &user, &groups);
  return new SecurityContext(cred, user, groups);
}



SecurityContext* NsAdapterCatalog::createSecurityContext(void) throw (DmException)
{
  UserInfo user;
  std::vector<GroupInfo> groups;
  GroupInfo group;

  user.name    = "root";
  user["uid"]  = 0;
  group.name   = "root";
  group["gid"] = 0;
  groups.push_back(group);

  return new SecurityContext(SecurityCredentials(), user, groups);
}



void NsAdapterCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  // String => const char*
  if (this->fqans_ != NULL) {
    for (unsigned i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  this->fqans_ = NULL;
  this->nFqans_ = 0;

  this->secCtx_ = ctx;

  if (!ctx) { return; }

  // Root is a special case
  if (ctx->user.getUnsigned("uid") != 0 && ctx->groups.empty())
      throw DmException(DMLITE_SYSERR(DMLITE_NO_SUCH_GROUP),
                        "Need at least one group");

  this->nFqans_ = ctx->groups.size();
  this->fqans_  = new char* [this->nFqans_];
  for (unsigned i = 0; i < this->nFqans_; ++i) {
    this->fqans_[i] = new char [ctx->groups[i].name.length() + 1];
    strcpy(this->fqans_[i], ctx->groups[i].name.c_str());
  }
}


void NsAdapterCatalog::changeDir(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();

  FunctionWrapper<int, const char*>(dpns_chdir, path.c_str())();
  this->cwdPath_ = path;
}



std::string NsAdapterCatalog::getWorkingDir(void) throw (DmException)
{
  setDpnsApiIdentity();

  char buffer[1024];
  return std::string(FunctionWrapper<char*, char*, int>(dpns_getcwd, buffer, sizeof(buffer))());
}



ExtendedStat NsAdapterCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  setDpnsApiIdentity();

  ExtendedStat xStat;

  if (follow) {
    struct dpns_filestatg dpnsStat;
    FunctionWrapper<int, const char*, const char*, dpns_filestatg*>(dpns_statg, path.c_str(), NULL, &dpnsStat)();
    
    xStat.stat.st_atim.tv_sec = dpnsStat.atime;
    xStat.stat.st_ctim.tv_sec = dpnsStat.ctime;
    xStat.stat.st_mtim.tv_sec = dpnsStat.mtime;
    xStat.stat.st_gid   = dpnsStat.gid;
    xStat.stat.st_uid   = dpnsStat.uid;
    xStat.stat.st_nlink = dpnsStat.nlink;
    xStat.stat.st_ino   = dpnsStat.fileid;
    xStat.stat.st_mode  = dpnsStat.filemode;
    xStat.stat.st_size  = dpnsStat.filesize;
    xStat.csumtype      = dpnsStat.csumtype;
    xStat.csumvalue     = dpnsStat.csumvalue;
    xStat.status  = static_cast<ExtendedStat::FileStatus>(dpnsStat.status);
    xStat["type"] = dpnsStat.fileclass;
  }
  else {
    struct dpns_filestat dpnsStat;
    FunctionWrapper<int, const char*, dpns_filestat*>(dpns_lstat, path.c_str(), &dpnsStat)();
    
    xStat.stat.st_atim.tv_sec = dpnsStat.atime;
    xStat.stat.st_ctim.tv_sec = dpnsStat.ctime;
    xStat.stat.st_mtim.tv_sec = dpnsStat.mtime;
    xStat.stat.st_gid   = dpnsStat.gid;
    xStat.stat.st_uid   = dpnsStat.uid;
    xStat.stat.st_nlink = dpnsStat.nlink;
    xStat.stat.st_ino   = dpnsStat.fileid;
    xStat.stat.st_mode  = dpnsStat.filemode;
    xStat.stat.st_size  = dpnsStat.filesize;
    xStat.status  = static_cast<ExtendedStat::FileStatus>(dpnsStat.status);
    xStat["type"] = dpnsStat.fileclass;
  }

  // Get the ACL if the file is not a symlink
  if (!S_ISLNK(xStat.stat.st_mode)) {
    struct dpns_acl dpnsAcls[kAclEntriesMax];
    int n = FunctionWrapper<int, const char*, int, dpns_acl*>(dpns_getacl, path.c_str(), kAclEntriesMax, dpnsAcls)();

    for (int i = 0; i < n; ++i) {
      AclEntry e;

      e.id   = dpnsAcls[i].a_id;
      e.perm = dpnsAcls[i].a_perm;
      e.type = dpnsAcls[i].a_type;
      xStat.acl.push_back(e);
    }
  }

  // Missing bits
  xStat.parent = 0;
  
  std::vector<std::string> components = Url::splitPath(path);
  xStat.name = components.back();

  return fillChecksumInXattr(xStat);
}



ExtendedStat NsAdapterCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  setDpnsApiIdentity();

  struct dpns_filestatg dpnsStat;
  ExtendedStat xStat;

  FunctionWrapper<int, const char*, dpns_filestatg*>(dpns_statr, rfn.c_str(), &dpnsStat)();

  xStat.stat.st_atim.tv_sec = dpnsStat.atime;
  xStat.stat.st_ctim.tv_sec = dpnsStat.ctime;
  xStat.stat.st_mtim.tv_sec = dpnsStat.mtime;
  xStat.stat.st_gid   = dpnsStat.gid;
  xStat.stat.st_uid   = dpnsStat.uid;
  xStat.stat.st_nlink = dpnsStat.nlink;
  xStat.stat.st_ino   = dpnsStat.fileid;
  xStat.stat.st_mode  = dpnsStat.filemode;
  xStat.stat.st_size  = dpnsStat.filesize;
  xStat.status  = static_cast<ExtendedStat::FileStatus>(dpnsStat.status);
  xStat["type"] = dpnsStat.fileclass;

  xStat.parent = 0;
  xStat.name   = "";

  return fillChecksumInXattr(xStat);
}



bool NsAdapterCatalog::access(const std::string& sfn, int mode) throw (DmException)
{
  setDpnsApiIdentity();

  try {
    FunctionWrapper<int, const char*, int>(dpns_access, sfn.c_str(), mode)();
    return true;
  }
  catch (DmException& e) {
    if (e.code() != EACCES) throw;
  }
  return false;
}



bool NsAdapterCatalog::accessReplica(const std::string& rfn, int mode) throw (DmException)
{
  setDpnsApiIdentity();

  try {
    FunctionWrapper<int, const char*, int>(dpns_accessr, rfn.c_str(), mode)();
    return true;
  }
  catch (DmException& e) {
    if (e.code() == ENOENT) throw DmException(DMLITE_NO_SUCH_REPLICA, e.what());
    if (e.code() != EACCES) throw;
  }
  return false;
}



void NsAdapterCatalog::addReplica(const Replica& replica) throw (DmException)
{
  setDpnsApiIdentity();

  struct dpns_fileid uniqueId;

  // If server is empty, parse the surl
  std::string host;
  if (replica.server.empty()) {
    Url u(replica.rfn);
    host = u.domain;
    if (host.empty())
      throw DmException(EINVAL,
                        "Empty server specified, and SFN does not include it: %s",
                        replica.rfn.c_str());
  }
  else {
    host = replica.server;
  }

  uniqueId.fileid = replica.fileid;
  strncpy(uniqueId.server, getenv("DPNS_HOST"), sizeof(uniqueId.server));
  
  std::string pool       = replica.getString("pool");
  std::string filesystem = replica.getString("filesystem");

  FunctionWrapper<int, const char*, dpns_fileid*, const char*, const char*, char, char, const char*, const char*>
    (dpns_addreplica, NULL, &uniqueId, host.c_str(),
     replica.rfn.c_str(), replica.status, replica.type,
     pool.c_str(), filesystem.c_str())();
}



void NsAdapterCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  setDpnsApiIdentity();

  struct dpns_fileid uniqueId;

  uniqueId.fileid = replica.fileid;
  strncpy(uniqueId.server, getenv("DPNS_HOST"), sizeof(uniqueId.server));

  FunctionWrapper<int, const char*, dpns_fileid*, const char*>(dpns_delreplica, NULL, &uniqueId, replica.rfn.c_str())();
}



std::vector<Replica> NsAdapterCatalog::getReplicas(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();

  struct dpns_filereplicax *entries;
  int                       nEntries;
  std::vector<Replica>      replicas;

  if (dpns_getreplicax (path.c_str(), NULL, NULL, &nEntries, &entries) != 0)
    ThrowExceptionFromSerrno(serrno);
  if (nEntries == 0)
    return replicas;

  replicas.reserve(nEntries);

  for (int i = 0; i < nEntries; ++i) {
    Replica replica;

    replica.replicaid = i;
    replica.atime      = entries[i].atime;
    replica.fileid     = entries[i].fileid;
    replica.nbaccesses = entries[i].nbaccesses;
    replica.ptime      = entries[i].ptime;
    replica.ltime      = entries[i].ltime;
    replica.type       = static_cast<Replica::ReplicaType>(entries[i].f_type);
    replica.status     = static_cast<Replica::ReplicaStatus>(entries[i].status);


    replica.server = entries[i].host;
    replica.rfn    = entries[i].sfn;
    
    replica["filesystem"] = std::string(entries[i].fs);
    replica["setname"]    = std::string(entries[i].setname);
    replica["pool"]       = std::string(entries[i].poolname);

    replicas.push_back(replica);
  }
  
  free(entries);

  return replicas;
}



void NsAdapterCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, const char*>(dpns_symlink, oldpath.c_str(), newpath.c_str())();
}



std::string NsAdapterCatalog::readLink(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();

  char buf[PATH_MAX];
  FunctionWrapper<int, const char*, char*, size_t>(dpns_readlink, path.c_str(), buf, sizeof(buf))();
  return buf;
}



void NsAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*>(dpns_unlink, path.c_str())();
}



void NsAdapterCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  FunctionWrapper<int, const char*, mode_t>(dpns_creat, path.c_str(), mode)();
}



mode_t NsAdapterCatalog::umask(mode_t mask) throw ()
{
  setDpnsApiIdentity();
  return dpns_umask(mask);
}



void NsAdapterCatalog::setMode(const std::string& path, mode_t mode) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, mode_t>(dpns_chmod, path.c_str(), mode)();
}



void NsAdapterCatalog::setOwner(const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink) throw (DmException)
{
  setDpnsApiIdentity();
  if (followSymLink)
    FunctionWrapper<int, const char*, uid_t, gid_t>(dpns_chown, path.c_str(), newUid, newGid)();
  else
    FunctionWrapper<int, const char*, uid_t, gid_t>(dpns_lchown, path.c_str(), newUid, newGid)();
}



void NsAdapterCatalog::setSize(const std::string& path, size_t newSize) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, dpns_fileid*, u_signed64>(dpns_setfsize, path.c_str(), NULL, newSize)();
}



void NsAdapterCatalog::setChecksum(const std::string& path,
                                   const std::string& csumtype,
                                   const std::string& csumvalue) throw (DmException)
{
  setDpnsApiIdentity();

  ExtendedStat stat = this->extendedStat(path, false);
  
  FunctionWrapper<int, const char*, dpns_fileid*, u_signed64, const char*, char*>
    (dpns_setfsizec, path.c_str(), NULL, stat.stat.st_size,
     csumtype.c_str(), (char*)csumvalue.c_str())();
}



void NsAdapterCatalog::setAcl(const std::string& path, const Acl& acl) throw (DmException)
{
  setDpnsApiIdentity();

  struct dpns_acl *aclp;
  int    nAcl;
  size_t i;

  nAcl = acl.size();
  aclp = new dpns_acl[nAcl];

  for (i = 0; i < acl.size(); ++i) {
    aclp[i].a_id   = acl[i].id;
    aclp[i].a_perm = acl[i].perm;
    aclp[i].a_type = acl[i].type;
  }

  try {
    FunctionWrapper<int, const char*, int, dpns_acl*>(dpns_setacl, path.c_str(), nAcl, aclp)();
    delete [] aclp;
  }
  catch (...) {
    delete [] aclp;
    throw;
  }
}



void NsAdapterCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, utimbuf*>(dpns_utime, path.c_str(), (struct utimbuf*)buf)();
}



std::string NsAdapterCatalog::getComment(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();

  char comment[kCommentMax];
  FunctionWrapper<int, const char*, char*>(dpns_getcomment, path.c_str(), comment)();
  return std::string(comment);
}



void NsAdapterCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, char*>(dpns_setcomment, path.c_str(), (char*)comment.c_str())();
}



void NsAdapterCatalog::setGuid(const std::string&, const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "Adapter does not support setting the GUID");
}



void NsAdapterCatalog::updateExtendedAttributes(const std::string& path,
                                                const Extensible& attr) throw (DmException)
{
  setDpnsApiIdentity();

  // At least one checksum.* attribute must be supported, but only those
  // of 2 bytes (legacy implementation!)
  ExtendedStat xstat = this->extendedStat(path, true);

  std::vector<std::string> keys = attr.getKeys();
  std::string csumXattr;

  for (unsigned i = 0; i < keys.size(); ++i) {
    if (keys[i].compare("type") == 0)
      continue;
    else if (keys[i].compare(0, 9, "checksum.") != 0)
      throw DmException(EINVAL, "Adapter does not support custom extended attributes");
    else if (!csumXattr.empty())
      throw DmException(EINVAL, "Adapter only supports one single checksum type in the extended attributes");
    else
      csumXattr = keys[i];
  }

  std::string shortCsumType = checksums::shortChecksumName(csumXattr.substr(9));
  std::string csumValue     = attr.getString(csumXattr);

  if (shortCsumType.length() > 2)
    throw DmException(EINVAL, "'%s' is an invalid checksum type",
                      shortCsumType.c_str());

  this->setChecksum(path, shortCsumType, csumValue);
}



GroupInfo NsAdapterCatalog::getGroup(gid_t gid) throw (DmException)
{
  setDpnsApiIdentity();

  GroupInfo group;
  char      buffer[512];

  FunctionWrapper<int, gid_t, char*>(dpns_getgrpbygid, gid, buffer)();
  group.name      = buffer;
  group["gid"]    = gid;
  group["banned"] = 0;

  return group;
}



GroupInfo NsAdapterCatalog::newGroup(const std::string& gname) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, gid_t, char*>(dpns_entergrpmap, -1, (char*)gname.c_str())();
  return this->getGroup(gname);
}



GroupInfo NsAdapterCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  setDpnsApiIdentity();

  GroupInfo group;
  gid_t     gid;

  try {
    FunctionWrapper<int, char*, gid_t*>(dpns_getgrpbynam, (char*)groupName.c_str(), &gid)();
    group.name      = groupName;
    group["gid"]    = gid;
    group["banned"] = 0;
  }
  catch (DmException& e) {
    if (e.code() != EINVAL) throw;
    throw DmException(DMLITE_NO_SUCH_GROUP, e.what());
  }

  return group;
}



GroupInfo NsAdapterCatalog::getGroup(const std::string& key,
                                     const boost::any& value) throw (DmException)
{
  setDpnsApiIdentity();

  if (key != "gid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "AdapterCatalog does not support querying by %s",
                      key.c_str());
  
  gid_t gid = Extensible::anyToUnsigned(value);
  return this->getGroup(gid);
}



void NsAdapterCatalog::updateGroup(const GroupInfo& group) throw (DmException)
{
  setDpnsApiIdentity();

  // gid may not be initialized
  GroupInfo g = this->getGroup(group.name);
  FunctionWrapper<int, gid_t, char*, int>(dpns_modifygrpmap,
                                          g.getUnsigned("gid"),
                                          (char*)group.name.c_str(),
                                          group.getLong("banned"))();
}



void NsAdapterCatalog::deleteGroup(const std::string& groupName) throw (DmException)
{
  setDpnsApiIdentity();

  GroupInfo g = this->getGroup(groupName);
  try {
    FunctionWrapper<int, gid_t, char*>(dpns_rmgrpmap, g.getUnsigned("gid"), (char*)g.name.c_str())();
  }
  catch (DmException& e) {
    if (e.code() != EINVAL) throw;
    throw DmException(DMLITE_NO_SUCH_GROUP, e.what());
  }
}



UserInfo NsAdapterCatalog::getUser(const std::string& userName) throw (DmException)
{
  setDpnsApiIdentity();

  UserInfo user;
  uid_t    uid;

  if (hostDnIsRoot_ && userName == hostDn_) {
    user.name      = userName;
    user["uid"]    = 0u;
    user["banned"] = 0;
  }
  else {
    try {
      FunctionWrapper<int, char*, uid_t*>(dpns_getusrbynam, (char*)userName.c_str(), &uid)();
      user.name      = userName;
      user["uid"]    = uid;
      user["banned"] = 0;
    }
    catch (DmException& e) {
      if (e.code() != EINVAL) throw;
      throw DmException(DMLITE_NO_SUCH_USER, e.what());
    }
  }

  return user;
}



UserInfo NsAdapterCatalog::getUser(const std::string& key,
                                   const boost::any& value) throw (DmException)
{
  setDpnsApiIdentity();

  if (key != "uid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "NsAdapterCatalog does not support querying by %s",
                      key.c_str());
  
  uid_t uid = Extensible::anyToUnsigned(value);
  char  uname[256];
  
  FunctionWrapper<int, uid_t, char*>(dpns_getusrbyuid, uid, uname)();
  
  UserInfo user;
  user.name      = uname;
  user["uid"]    = uid;
  user["banned"] = 0;
  return user;
}



UserInfo NsAdapterCatalog::newUser(const std::string& uname) throw (DmException)
{
  setDpnsApiIdentity();

  FunctionWrapper<int, uid_t, char*>(dpns_enterusrmap, -1, (char*)uname.c_str())();
  return this->getUser(uname);
}



void NsAdapterCatalog::updateUser(const UserInfo& user) throw (DmException)
{
  setDpnsApiIdentity();

  // uid may not be initialized
  UserInfo u = this->getUser(user.name);
  FunctionWrapper<int, uid_t, char*, int>(dpns_modifyusrmap,
                                          u.getUnsigned("uid"),
                                          (char*)user.name.c_str(),
                                          user.getLong("banned"))();
}



void NsAdapterCatalog::deleteUser(const std::string& userName) throw (DmException)
{
  setDpnsApiIdentity();

  UserInfo u = this->getUser(userName);
  try {
    FunctionWrapper<int, uid_t, char*>(dpns_rmusrmap, u.getUnsigned("uid"), (char*)u.name.c_str())();
  }
  catch (DmException& e) {
    if (e.code() != EINVAL) throw;
    throw DmException(DMLITE_NO_SUCH_USER, e.what());
  }
}



std::vector<GroupInfo> NsAdapterCatalog::getGroups(void) throw (DmException)
{
  setDpnsApiIdentity();

  std::vector<GroupInfo> groups;
  struct dpns_groupinfo* dpnsGroups;
  GroupInfo group;
  int       nGroups;
  
  FunctionWrapper<int, int*, dpns_groupinfo**>(dpns_getgrpmap, &nGroups, &dpnsGroups)();
  for (int i = 0; i < nGroups; ++i) {
    group.clear();
    group.name      = dpnsGroups[i].groupname;
    group["gid"]    = dpnsGroups[i].gid;
    group["banned"] = dpnsGroups[i].banned;
    groups.push_back(group);
  }
  free(dpnsGroups);
  
  return groups;
}



std::vector<UserInfo> NsAdapterCatalog::getUsers(void) throw (DmException)
{
  setDpnsApiIdentity();

  std::vector<UserInfo> users;
  struct dpns_userinfo* dpnsUsers;
  UserInfo user;
  int      nUsers;
  
  FunctionWrapper<int, int*, dpns_userinfo**>(dpns_getusrmap, &nUsers, &dpnsUsers)();
  for (int i = 0; i < nUsers; ++i) {
    user.clear();
    user.name      = dpnsUsers[i].username;
    user["uid"]    = dpnsUsers[i].userid;
    user["banned"] = dpnsUsers[i].banned;
    user["ca"]     = std::string(dpnsUsers[i].user_ca);
    users.push_back(user);
  }
  free(dpnsUsers);
  
  return users;
}



void NsAdapterCatalog::getIdMap(const std::string& userName,
                                const std::vector<std::string>& groupNames,
                                UserInfo* user, std::vector<GroupInfo>* groups) throw (DmException)
{
  setDpnsApiIdentity();

  unsigned    ngroups = groupNames.size();
  const char *gnames[ngroups];
  gid_t       gids[ngroups + 1];
  uid_t       uid;
  
  if (hostDnIsRoot_ && userName == hostDn_) {
    (*user).name = userName;
    (*user)["uid"] = 0u;
    (*user)["banned"] = 0;
    GroupInfo grp;
    grp.name = "root";
    grp["gid"] = 0u;
    grp["banned"] = 0u;
    groups->push_back(grp);
  }
  else {
    for (unsigned i = 0; i < ngroups; ++i)
      gnames[i] = groupNames[i].c_str();

    FunctionWrapper<int, const char*, int, const char**, uid_t*, gid_t*>
      (dpns_getidmap, userName.c_str(), ngroups, gnames, &uid, gids)();

    (*user).name      = userName.c_str();
    (*user)["uid"]    = uid;
    (*user)["banned"] = 0;

    if (ngroups > 0) {
      for (unsigned i = 0; i < ngroups; ++i)
        groups->push_back(this->getGroup(gids[i]));
    }
    // If we didn't ask for any specific group, at least one gid will be available
    else {
      groups->push_back(this->getGroup(gids[0]));
    }
  }
}



Directory* NsAdapterCatalog::openDir(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();

  PrivateDir *privateDir;

  privateDir = new PrivateDir();
  dpns_startsess((char*)getenv("DPM_HOST"), (char*)"dmlite::adapter::opendir");
  privateDir->dpnsDir = dpns_opendir(path.c_str());
  if (privateDir->dpnsDir == NULL) {
    delete privateDir;
    ThrowExceptionFromSerrno(serrno);
    return NULL;
  }

  return privateDir;
}



void NsAdapterCatalog::closeDir(Directory* dir) throw (DmException)
{
  setDpnsApiIdentity();

  PrivateDir *privateDir = dynamic_cast<PrivateDir*>(dir);

  if (privateDir == NULL)
    throw DmException(EFAULT, "Tried to close a null directory");

  try {
    FunctionWrapper<int, dpns_DIR*>(dpns_closedir, privateDir->dpnsDir)();
    dpns_endsess();
    delete privateDir;
  }
  catch (...) {
    dpns_endsess();
    delete privateDir;
    throw;
  }
}



struct dirent* NsAdapterCatalog::readDir(Directory* dir) throw (DmException)
{
  setDpnsApiIdentity();

  PrivateDir *privateDir = dynamic_cast<PrivateDir*>(dir);

  if (privateDir == NULL)
    throw DmException(EFAULT, "Tried to read a null directory");

  return FunctionWrapper<dirent*, dpns_DIR*>(dpns_readdir, privateDir->dpnsDir)();
}



ExtendedStat* NsAdapterCatalog::readDirx(Directory* dir) throw (DmException)
{
  setDpnsApiIdentity();

  PrivateDir            *privateDir = static_cast<PrivateDir*>(dir);
  struct dpns_direnstat *direnstat;

  direnstat = dpns_readdirx(privateDir->dpnsDir);
  if (direnstat == NULL)
    return NULL;

  privateDir->stat.stat.st_ino = direnstat->fileid;
  privateDir->stat.name        = direnstat->d_name;

  privateDir->stat.status = static_cast<ExtendedStat::FileStatus>(direnstat->status);

  privateDir->stat.stat.st_atime = direnstat->atime;
  privateDir->stat.stat.st_ctime = direnstat->ctime;
  privateDir->stat.stat.st_mtime = direnstat->mtime;
  privateDir->stat.stat.st_mode  = direnstat->filemode;
  privateDir->stat.stat.st_size  = direnstat->filesize;
  privateDir->stat.stat.st_uid   = direnstat->uid;
  privateDir->stat.stat.st_gid   = direnstat->gid;
  privateDir->stat.stat.st_nlink = direnstat->nlink;
  
  return &(privateDir->stat);
}



void NsAdapterCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, mode_t>(dpns_mkdir, path.c_str(), mode)();
}



void NsAdapterCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, const char*>(dpns_rename, oldPath.c_str(), newPath.c_str())();
}



void NsAdapterCatalog::removeDir(const std::string& path) throw (DmException)
{
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*>(dpns_rmdir, path.c_str())();
}



Replica NsAdapterCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "NsAdapterCatalog: Get replica by RFN not implemented");
}



void NsAdapterCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "NsAdapterCatalog: updateReplica not implemented");
}
