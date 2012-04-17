/// @file   plugins/adapter/NsAdapter.cpp
/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdlib>
#include <cstring>
#include <dmlite/dm_errno.h>
#include <dmlite/common/Security.h>
#include <dmlite/common/Uris.h>
#include <serrno.h>

#include "Adapter.h"
#include "NsAdapter.h"

using namespace dmlite;



NsAdapterCatalog::NsAdapterCatalog(unsigned retryLimit)
  throw (DmException): Catalog(), retryLimit_(retryLimit)
{
  // These need to be set to 0
  this->fqans_  = 0x00;
  this->nFqans_ = 0;
  this->vo_     = 0x00;

  dpns_client_resetAuthorizationId();
}



NsAdapterCatalog::~NsAdapterCatalog()
{
  if (this->fqans_ != 0x00) {
    for (int i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  if (this->vo_ != 0x00)
    delete [] this->vo_;
}



std::string NsAdapterCatalog::getImplId() throw ()
{
  return std::string("NsAdapterCatalog");
}



void NsAdapterCatalog::set(const std::string& key, ...) throw (DmException)
{
  va_list vargs;

  va_start(vargs, key);
  this->set(key, vargs);
  va_end(vargs);
}



void NsAdapterCatalog::set(const std::string& key, va_list) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, "Option not recognised");
}



void NsAdapterCatalog::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  uid_t uid;
  gid_t gids[cred.nfqans + 1];

  // Get the ID mapping
  wrapCall(dpns_getidmap(cred.client_name, cred.nfqans, (const char**)cred.fqans,
                         &uid, gids));

  // Initialize context
  this->secCtx_.setCredentials(cred);
  this->secCtx_.getUser().uid    = uid;
  this->secCtx_.getUser().banned = 0;
  strncpy(this->secCtx_.getUser().name, cred.client_name, 255);

  // If groups were specified, copy
  if (cred.nfqans > 0) {
    this->secCtx_.resizeGroup(cred.nfqans);
    for (unsigned i = 0; i < cred.nfqans; ++i) {
      this->secCtx_.getGroup(i).gid    = gids[i];
      this->secCtx_.getGroup(i).banned = 0;
      strncpy(this->secCtx_.getGroup(i).name, cred.fqans[i], 255);
    }
  }
  // Else, there will be at least one default
  else {
    this->secCtx_.resizeGroup(1);
    this->secCtx_.getGroup(0) = this->getGroup(gids[0]);
  }

  // Set context
  this->setSecurityContext(this->secCtx_);
}



const SecurityContext& NsAdapterCatalog::getSecurityContext() throw (DmException)
{
  return this->secCtx_;
}



void NsAdapterCatalog::setSecurityContext(const SecurityContext& ctx)
{
  if (&this->secCtx_ != &ctx)
    this->secCtx_ = ctx;
  // Call DPNS API
  wrapCall(dpns_client_setAuthorizationId(ctx.getUser().uid,
                                          ctx.getGroup(0).gid,
                                          "GSI",
                                          (char*)ctx.getUser().name));

  if (ctx.groupCount() > 0)
    wrapCall(dpns_client_setVOMS_data((char*)ctx.getGroup(0).name,
                                      (char**)ctx.getCredentials().fqans,
                                      ctx.groupCount()));
}



void NsAdapterCatalog::changeDir(const std::string& path) throw (DmException)
{
  wrapCall(dpns_chdir(path.c_str()));
  this->cwdPath_ = path;
}



std::string NsAdapterCatalog::getWorkingDir(void) throw (DmException)
{
  char buffer[PATH_MAX];
  return std::string((const char*)wrapCall(dpns_getcwd(buffer, sizeof(buffer))));
}



ino_t NsAdapterCatalog::getWorkingDirI() throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Access by inode not implemented");
}



ExtendedStat NsAdapterCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  struct dpns_filestat dpnsStat;
  struct xstat         xStat;

  if (follow)
    wrapCall(dpns_stat(path.c_str(), &dpnsStat));
  else
    wrapCall(dpns_lstat(path.c_str(), &dpnsStat));

  xStat.stat.st_atim.tv_sec = dpnsStat.atime;
  xStat.stat.st_ctim.tv_sec = dpnsStat.ctime;
  xStat.stat.st_mtim.tv_sec = dpnsStat.mtime;
  xStat.stat.st_gid   = dpnsStat.gid;
  xStat.stat.st_uid   = dpnsStat.uid;
  xStat.stat.st_nlink = dpnsStat.nlink;
  xStat.stat.st_ino   = dpnsStat.fileid;
  xStat.stat.st_mode  = dpnsStat.filemode;
  xStat.stat.st_size  = dpnsStat.filesize;

  xStat.acl[0]       = '\0';
  xStat.csumtype[0]  = '\0';
  xStat.csumvalue[0] = '\0';
  xStat.guid[0]      = '\0';

  xStat.parent = 0;

  xStat.status = dpnsStat.status;
  xStat.type   = dpnsStat.fileclass;

  std::list<std::string> components = splitPath(path);
  if (!components.empty())
    strncpy(xStat.name, components.back().c_str(), sizeof(xStat.name));
  else
    strcpy(xStat.name, "/");

  // Get the ACL
  struct dpns_acl dpnsAcls[ACL_ENTRIES_MAX];
  int n = wrapCall(dpns_getacl(path.c_str(), ACL_ENTRIES_MAX, dpnsAcls));

  std::vector<Acl> dmAcls;
  for (int i = 0; i < n; ++i) {
    Acl acl;
    acl.id   = dpnsAcls[i].a_id;
    acl.perm = dpnsAcls[i].a_perm;
    acl.type = dpnsAcls[i].a_type;
    dmAcls.push_back(acl);
  }

  strncpy(xStat.acl, dmlite::serializeAcl(dmAcls).c_str(), sizeof(xStat.acl));

  return xStat;
}



ExtendedStat NsAdapterCatalog::extendedStat(ino_t) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Access by inode not supported");
}



ExtendedStat NsAdapterCatalog::extendedStat(ino_t, const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Access by inode not supported");
}



SymLink NsAdapterCatalog::readLink(ino_t inode) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Access by inode not supported");
}



void NsAdapterCatalog::addReplica(const std::string& guid, int64_t id,
                                  const std::string& server, const std::string& sfn,
                                  char status, char fileType,
                                  const std::string& poolName,
                                  const std::string& fileSystem) throw (DmException)
{
  struct dpns_fileid uniqueId;

  // If server is empty, parse the surl
  std::string host;
  if (server.empty()) {
    Uri u = splitUri(sfn);
    host = u.host;
    if (host.empty())
      throw DmException(DM_INVALID_VALUE, "Empty server specified, and SFN does not include it: " + sfn);
  }
  else {
    host = server;
  }

  uniqueId.fileid = id;
  strncpy(uniqueId.server, getenv("DPM_HOST"), sizeof(uniqueId.server));

  wrapCall(dpns_addreplica(guid.c_str(), &uniqueId, host.c_str(),
                           sfn.c_str(), status, fileType,
                           poolName.c_str(), fileSystem.c_str()));
}



void NsAdapterCatalog::deleteReplica(const std::string& guid, int64_t id,
                                     const std::string& sfn) throw (DmException)
{
  struct dpns_fileid uniqueId;

  uniqueId.fileid = id;
  strncpy(uniqueId.server, getenv("DPM_HOST"), sizeof(uniqueId.server));

  if (guid.empty())
    wrapCall(dpns_delreplica(NULL, &uniqueId, sfn.c_str()));
  else
    wrapCall(dpns_delreplica(guid.c_str(), &uniqueId, sfn.c_str()));
}



std::vector<FileReplica> NsAdapterCatalog::getReplicas(const std::string& path) throw (DmException)
{
  struct dpns_filereplicax *entries;
  int                       nEntries;
  std::vector<FileReplica>  replicas;

  if (dpns_getreplicax (path.c_str(), NULL, NULL, &nEntries, &entries) != 0)
    ThrowExceptionFromSerrno(serrno);
  if (nEntries == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

  replicas.reserve(nEntries);

  for (int i = 0; i < nEntries; ++i) {
    FileReplica replica;

    replica.replicaid = i;
    replica.atime      = entries[i].atime;
    replica.fileid     = entries[i].fileid;
    replica.type       = entries[i].f_type;
    replica.nbaccesses = entries[i].nbaccesses;
    replica.ptime      = entries[i].ptime;
    replica.ltime      = entries[i].ltime;
    replica.status     = entries[i].status;


    strncpy(replica.filesystem, entries[i].fs,       sizeof(replica.filesystem));
    strncpy(replica.pool,       entries[i].poolname, sizeof(replica.pool));
    strncpy(replica.server,     entries[i].host,     sizeof(replica.server));
    strncpy(replica.url,        entries[i].sfn,      sizeof(replica.url));

    replicas.push_back(replica);
  }
  
  free(entries);

  return replicas;
}



Uri NsAdapterCatalog::get(const std::string& path) throw (DmException)
{
  // Naive implementation: first occurrence
  // Better implementations are left for other plugins
  std::vector<FileReplica> replicas = this->getReplicas(path);

  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

  return dmlite::splitUri(replicas[0].url);
}



void NsAdapterCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  wrapCall(dpns_symlink(oldpath.c_str(), newpath.c_str()));
}



void NsAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
  wrapCall(dpns_unlink(path.c_str()));
}



void NsAdapterCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  wrapCall(dpns_creat(path.c_str(), mode));
}



std::string NsAdapterCatalog::put(const std::string& path, Uri* uri) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "put not implemented for NsAdapterCatalog");
}



std::string NsAdapterCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "put not implemented for NsAdapterCatalog");
}



void NsAdapterCatalog::putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "putStatus not implemented for NsAdapterCatalog");
}



void NsAdapterCatalog::putDone(const std::string& path, const std::string& token) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "putDone not implemented for NsAdapterCatalog");
}



mode_t NsAdapterCatalog::umask(mode_t mask) throw ()
{
  return dpns_umask(mask);
}



void NsAdapterCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  wrapCall(dpns_chmod(path.c_str(), mode));
}



void NsAdapterCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  wrapCall(dpns_chown(path.c_str(), newUid, newGid));
}



void NsAdapterCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  wrapCall(dpns_lchown(path.c_str(), newUid, newGid));
}



void NsAdapterCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  struct dpns_acl *aclp;
  int    nAcls;
  size_t i;

  nAcls = acls.size();
  aclp = new dpns_acl[nAcls];

  for (i = 0; i < acls.size(); ++i) {
    aclp[i].a_id   = acls[i].id;
    aclp[i].a_perm = acls[i].perm;
    aclp[i].a_type = acls[i].type;
  }

  int r = dpns_setacl(path.c_str(), nAcls, aclp);
  delete [] aclp;
  wrapCall(r);
}



void NsAdapterCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  wrapCall(dpns_utime(path.c_str(), (struct utimbuf*)buf));
}



void NsAdapterCatalog::utime(ino_t inode, const struct utimbuf* buf) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Access by inode not supported");
}



std::string NsAdapterCatalog::getComment(const std::string& path) throw (DmException)
{
  char comment[COMMENT_MAX];

  wrapCall(dpns_getcomment(path.c_str(), comment));
  
  return std::string(comment);
}



void NsAdapterCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  wrapCall(dpns_setcomment(path.c_str(), (char*)comment.c_str()));
}



GroupInfo NsAdapterCatalog::getGroup(gid_t gid) throw (DmException)
{
  GroupInfo group;

  wrapCall(dpns_getgrpbygid(gid, group.name));
  group.gid    = gid;
  group.banned = 0;

  return group;
}



void NsAdapterCatalog::setGuid(const std::string&, const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "Adapter does not support setting the GUID");
}



GroupInfo NsAdapterCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  GroupInfo group;

  wrapCall(dpns_getgrpbynam((char*)groupName.c_str(), &group.gid));
  strcpy(group.name, groupName.c_str());
  group.banned = 0;

  return group;
}



UserInfo NsAdapterCatalog::getUser(const std::string& userName) throw (DmException)
{
  UserInfo user;

  wrapCall(dpns_getusrbynam((char*)userName.c_str(), &user.uid));
  strcpy(user.name, userName.c_str());
  user.banned = 0;

  return user;
}



UserInfo NsAdapterCatalog::getUser(uid_t uid) throw (DmException)
{
  UserInfo user;

  wrapCall(dpns_getusrbyuid(uid, user.name));
  user.uid    = uid;
  user.banned = 0;
  user.ca[0] = '\0';
  
  return user;
}



Directory* NsAdapterCatalog::openDir(const std::string& path) throw (DmException)
{
  PrivateDir *privateDir;

  privateDir = new PrivateDir();
  dpns_startsess((char*)getenv("DPM_HOST"), (char*)"dmlite::adapter::opendir");
  privateDir->dpnsDir = dpns_opendir(path.c_str());
  if (privateDir->dpnsDir == 0x00) {
    delete privateDir;
    ThrowExceptionFromSerrno(serrno);
    return 0x00;
  }

  return static_cast<void*>(privateDir);
}



void NsAdapterCatalog::closeDir(Directory* dir) throw (DmException)
{
  int         r;
  PrivateDir *privateDir = static_cast<PrivateDir*>(dir);

  if (privateDir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to close a null directory");

  r = dpns_closedir(privateDir->dpnsDir);
  dpns_endsess();
  delete privateDir;

  wrapCall(r);
}



struct dirent* NsAdapterCatalog::readDir(Directory* dir) throw (DmException)
{
  PrivateDir    *privateDir = static_cast<PrivateDir*>(dir);

  if (privateDir == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null directory");

  return static_cast<struct dirent*>(wrapCall(dpns_readdir(privateDir->dpnsDir)));
}



ExtendedStat* NsAdapterCatalog::readDirx(Directory* dir) throw (DmException)
{
  PrivateDir            *privateDir = static_cast<PrivateDir*>(dir);
  struct dpns_direnstat *direnstat;

  direnstat = dpns_readdirx(privateDir->dpnsDir);
  if (direnstat == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null directory");

  privateDir->stat.stat.st_ino = direnstat->fileid;
  strncpy(privateDir->stat.name, direnstat->d_name,
          sizeof(privateDir->stat.name));

  privateDir->stat.status = direnstat->status;
  privateDir->stat.type   = direnstat->fileclass;

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
  wrapCall(dpns_mkdir(path.c_str(), mode));
}



void NsAdapterCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  wrapCall(dpns_rename(oldPath.c_str(), newPath.c_str()));
}



void NsAdapterCatalog::removeDir(const std::string& path) throw (DmException)
{
  try {
    wrapCall(dpns_rmdir(path.c_str()));
  }
  catch (DmException e) {
    if (e.code() == DM_INVALID_VALUE)
      throw DmException(DM_IS_CWD, e.what());
    throw;
  }
}



void NsAdapterCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  wrapCall(dpns_setratime(replica.c_str()));
}



void NsAdapterCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  wrapCall(dpns_setrltime(replica.c_str(), ltime));
}



void NsAdapterCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  wrapCall(dpns_setrstatus(replica.c_str(), status));
}



void NsAdapterCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  wrapCall(dpns_setrtype(replica.c_str(), type));
}
