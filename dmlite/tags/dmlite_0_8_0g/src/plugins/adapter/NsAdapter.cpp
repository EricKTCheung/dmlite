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
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " hostDn: " << hostDn);
  
  static pthread_once_t once_control = PTHREAD_ONCE_INIT;
  pthread_once(&once_control, ns_init_routine);
}



NsAdapterCatalog::~NsAdapterCatalog()
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " hostDn: " << this->hostDn_);
  
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
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
  FunctionWrapper<int> reset(dpns_client_resetAuthorizationId);
  reset();

  // can not do any more if there is no security context
  if (!secCtx_) {
    Err(adapterlogname, "No security context. Exiting.");
    return;
  }

  uid_t uid = secCtx_->user.getUnsigned("uid");
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "uid=" << uid);

  // nothing more to do for root
  if (uid == 0) { return; }

  FunctionWrapper<int, uid_t, gid_t, const char*, char*>(
      dpns_client_setAuthorizationId,
        uid, secCtx_->groups[0].getUnsigned("gid"), "GSI",
        (char*)secCtx_->user.name.c_str())();

  if (fqans_ && nFqans_) {
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "fqan=" << fqans_[0]);
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
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Deleting previous fqans");
    for (unsigned i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  this->fqans_ = NULL;
  this->nFqans_ = 0;
  this->userId_.clear();
  
  this->secCtx_ = ctx;

  if (!ctx) {
    Log(Logger::Lvl3, adapterlogmask, adapterlogname, "No security context. Exiting.");
    return;
    
  }

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
  
  this->userId_ = ctx->credentials.clientName;
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, " fqan=" << ( (fqans_ && nFqans_) ? fqans_[0]:"none") );
}


void NsAdapterCatalog::changeDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " path=" << path );
  setDpnsApiIdentity();

  FunctionWrapper<int, const char*>(dpns_chdir, path.c_str())();
  this->cwdPath_ = path;
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, " Exiting. path=" << path );
}



std::string NsAdapterCatalog::getWorkingDir(void) throw (DmException)
{
  
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
  setDpnsApiIdentity();

  char buffer[1024];
  std::string res = FunctionWrapper<char*, char*, int>(dpns_getcwd, buffer, sizeof(buffer))();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, " Exiting. wd:" << res );
  return res;
}



ExtendedStat NsAdapterCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " follow:" << follow);
  
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

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " size:" << xStat.stat.st_size <<
      " gid:" << xStat.stat.st_gid << " uid:" << xStat.stat.st_uid << " mode:" << xStat.stat.st_mode <<
      " csumtype:" << xStat.csumtype << " csumvalue:" << xStat.csumvalue);
  
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

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " nacls:" << xStat.acl.size() );
  
  // Missing bits
  xStat.parent = 0;
  
  std::vector<std::string> components = Url::splitPath(path);
  xStat.name = components.back();

  checksums::fillChecksumInXattr(xStat);
  return xStat;
}



ExtendedStat NsAdapterCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "rfn: " << rfn);
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
  xStat.csumtype      = dpnsStat.csumtype;
  xStat.csumvalue     = dpnsStat.csumvalue;
  xStat.status  = static_cast<ExtendedStat::FileStatus>(dpnsStat.status);
  xStat["type"] = dpnsStat.fileclass;

  xStat.parent = 0;
  xStat.name   = "";

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "rfn: " << rfn << " size:" << xStat.stat.st_size <<
      " gid:" << xStat.stat.st_gid << " uid:" << xStat.stat.st_uid << " mode:" << xStat.stat.st_mode <<
      " csumtype:" << xStat.csumtype << " csumvalue:" << xStat.csumvalue);
  
  checksums::fillChecksumInXattr(xStat);
  return xStat;
}



bool NsAdapterCatalog::access(const std::string& sfn, int mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "sfn: " << sfn);
  setDpnsApiIdentity();

  try {
    FunctionWrapper<int, const char*, int>(dpns_access, sfn.c_str(), mode)();
    Log(Logger::Lvl3, adapterlogmask, adapterlogname, "sfn: " << sfn << " returns true");
    return true;
  }
  catch (DmException& e) {
    if (e.code() != EACCES) throw;
  }
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "sfn: " << sfn << " returns false");
  return false;
}



bool NsAdapterCatalog::accessReplica(const std::string& rfn, int mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "rfn: " << rfn << " mode:" << mode);
  setDpnsApiIdentity();

  try {
    FunctionWrapper<int, const char*, int>(dpns_accessr, rfn.c_str(), mode)();
    Log(Logger::Lvl3, adapterlogmask, adapterlogname, "rfn: " << rfn << " returns true");
    return true;
  }
  catch (DmException& e) {
    if (e.code() == ENOENT) throw DmException(DMLITE_NO_SUCH_REPLICA, e.what());
    if (e.code() != EACCES) throw;
  }
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "rfn: " << rfn << " returns false");
  return false;
}



void NsAdapterCatalog::addReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "replica: " << replica.rfn);
  
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
     
   Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. replica: " << replica.rfn);
}



void NsAdapterCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "replica: " << replica.rfn);
  setDpnsApiIdentity();

  struct dpns_fileid uniqueId;

  uniqueId.fileid = replica.fileid;
  strncpy(uniqueId.server, getenv("DPNS_HOST"), sizeof(uniqueId.server));

  FunctionWrapper<int, const char*, dpns_fileid*, const char*>(dpns_delreplica, NULL, &uniqueId, replica.rfn.c_str())();
  Log(Logger::Lvl2, adapterlogmask, adapterlogname, "replica: " << replica.rfn);
}



std::vector<Replica> NsAdapterCatalog::getReplicas(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path);
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

  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "path: " << path << " nreplicas:" << replicas.size());
  
  return replicas;
}



void NsAdapterCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "oldpath: " << oldpath << " newpath: " << newpath);
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, const char*>(dpns_symlink, oldpath.c_str(), newpath.c_str())();
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. oldpath: " << oldpath << " newpath: " << newpath);
}



std::string NsAdapterCatalog::readLink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();

  char buf[PATH_MAX];
  FunctionWrapper<int, const char*, char*, size_t>(dpns_readlink, path.c_str(), buf, sizeof(buf))();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "path: " << path << " res:" << buf);
  return buf;
}



void NsAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*>(dpns_unlink, path.c_str())();
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path );
}



void NsAdapterCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, mode_t>(dpns_creat, path.c_str(), mode)();
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path );
}



mode_t NsAdapterCatalog::umask(mode_t mask) throw ()
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "mask: " << mask );
  setDpnsApiIdentity();
  
  mode_t res = dpns_umask(mask);
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. mask: " << mask );
  return res;
}



void NsAdapterCatalog::setMode(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, mode_t>(dpns_chmod, path.c_str(), mode)();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path );
}



void NsAdapterCatalog::setOwner(const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();
  if (followSymLink)
    FunctionWrapper<int, const char*, uid_t, gid_t>(dpns_chown, path.c_str(), newUid, newGid)();
  else
    FunctionWrapper<int, const char*, uid_t, gid_t>(dpns_lchown, path.c_str(), newUid, newGid)();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path );
}



void NsAdapterCatalog::setSize(const std::string& path, size_t newSize) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " newsize:" << newSize);
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, dpns_fileid*, u_signed64>(dpns_setfsize, path.c_str(), NULL, newSize)();
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path << " newsize:" << newSize);
}





void NsAdapterCatalog::setAcl(const std::string& path, const Acl& acl) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " nacls:" << acl.size());
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path << " nacls:" << acl.size());
}



void NsAdapterCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, utimbuf*>(dpns_utime, path.c_str(), (struct utimbuf*)buf)();
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "path: " << path);
}



std::string NsAdapterCatalog::getComment(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path );
  setDpnsApiIdentity();

  char comment[kCommentMax];
  comment[0] = '\0';
  FunctionWrapperLE<int, const char*, char*>(dpns_getcomment, path.c_str(), comment)();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path << "comment:" << comment);
  
  return std::string(comment);
}



void NsAdapterCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " comment:" << comment);
  
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, char*>(dpns_setcomment, path.c_str(), (char*)comment.c_str())();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path << " comment:" << comment);
}



void NsAdapterCatalog::setGuid(const std::string&, const std::string&) throw (DmException)
{
  throw DmException(DMLITE_SYSERR(ENOSYS),
                    "Adapter does not support setting the GUID");
}



void NsAdapterCatalog::updateExtendedAttributes(const std::string& path,
                                                const Extensible& attr) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " nattrs:" << attr.size() );
  

  setDpnsApiIdentity();

  // At least one checksum.* attribute must be supported, but only those
  // of 2 bytes (legacy implementation!)
  ExtendedStat xstat = this->extendedStat(path, true);

  std::vector<std::string> keys = attr.getKeys();
  std::string csumXattr;
  std::string shortCsumType;

  for (unsigned i = 0; i < keys.size(); ++i) {
    
    if (!checksums::isChecksumFullName(keys[i])) {
      Log(Logger::Lvl2, adapterlogmask, adapterlogname,
          "Adapter does not support custom extended attributes. Ignoring key: " << keys[i]);
      continue;
    }
    
    // We are here if the entry is a checksum. Check if it's one that adapter can support
    shortCsumType = checksums::shortChecksumName(keys[i]);
    if (shortCsumType.length() > 2) {
      Log(Logger::Lvl2, adapterlogmask, adapterlogname,
          "Adapter does not support checksums of type " << shortCsumType << " Ignoring.");
      continue;
    }
    
    // Write a warning if we already had one to write
    if (!csumXattr.empty()) {
      Log(Logger::Lvl2, adapterlogmask, adapterlogname,
          "Adapter only supports one single checksum type in the extended attributes. Ignoring key: " << keys[i]);
      continue;
    }
    
    csumXattr = keys[i];
  }

  shortCsumType = checksums::shortChecksumName(csumXattr);
  std::string csumValue     = attr.getString(csumXattr);
    
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " csumtype:" << shortCsumType << " csumvalue:" << csumValue);
  setDpnsApiIdentity();


  off_t sz = 0;
  try {
      // dpm-dsi is very kind and may offer to us a way to avoid yet one more stat request
      // other plugins may follow one day the same suggestion by A.Kyrianov
      sz = attr.getU64("filesize");
      Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " filesize:" << sz << " got from the xattrs.");
  }
  catch (...) {  }
  
  if (sz == 0) {
      ExtendedStat xstat = this->extendedStat(path, false);
      sz = xstat.stat.st_size;
      Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path: " << path << " filesize:" << sz << " got from stat-ing the file.");
  }
      
  FunctionWrapper<int> reset(dpns_client_resetAuthorizationId);
  reset();
      
  FunctionWrapper<int, const char*, dpns_fileid*, u_signed64, const char*, char*>
    (dpns_setfsizec, path.c_str(), NULL, sz,
     shortCsumType.c_str(), (char*)csumValue.c_str())();
     
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. path: " << path );
}



GroupInfo NsAdapterCatalog::getGroup(gid_t gid) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "gid: " << gid );
  setDpnsApiIdentity();

  GroupInfo group;
  char      buffer[512];

  FunctionWrapper<int, gid_t, char*>(dpns_getgrpbygid, gid, buffer)();
  group.name      = buffer;
  group["gid"]    = gid;
  group["banned"] = 0;

  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. gid: " << gid << " group:" << group.name);
  return group;
}



GroupInfo NsAdapterCatalog::newGroup(const std::string& gname) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "gname: " << gname );
  
  setDpnsApiIdentity();
  FunctionWrapper<int, gid_t, char*>(dpns_entergrpmap, -1, (char*)gname.c_str())();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. gname: " << gname );
  return this->getGroup(gname);
}



GroupInfo NsAdapterCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "groupName: " << groupName );
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

  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Exiting. group: " << group.name );
  return group;
}



GroupInfo NsAdapterCatalog::getGroup(const std::string& key,
                                     const boost::any& value) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "key:" << key);
  setDpnsApiIdentity();

  if (key != "gid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "AdapterCatalog does not support querying by %s",
                      key.c_str());
  
  gid_t gid = Extensible::anyToUnsigned(value);
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. key:" << key);
  return this->getGroup(gid);
}



void NsAdapterCatalog::updateGroup(const GroupInfo& group) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "group:" << group.name);
  
  setDpnsApiIdentity();

  // gid may not be initialized
  GroupInfo g = this->getGroup(group.name);
  FunctionWrapper<int, gid_t, char*, int>(dpns_modifygrpmap,
                                          g.getUnsigned("gid"),
                                          (char*)group.name.c_str(),
                                          group.getLong("banned"))();
					  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. group:" << group.name);
}



void NsAdapterCatalog::deleteGroup(const std::string& groupName) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "groupname:" << groupName);
  
  setDpnsApiIdentity();

  GroupInfo g = this->getGroup(groupName);
  try {
    FunctionWrapper<int, gid_t, char*>(dpns_rmgrpmap, g.getUnsigned("gid"), (char*)g.name.c_str())();
  }
  catch (DmException& e) {
    if (e.code() != EINVAL) throw;
    throw DmException(DMLITE_NO_SUCH_GROUP, e.what());
  }
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. groupname:" << groupName);
}



UserInfo NsAdapterCatalog::getUser(const std::string& userName) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "userName:" << userName);
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

  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "userName:" << user.name);
  return user;
}



UserInfo NsAdapterCatalog::getUser(const std::string& key,
                                   const boost::any& value) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "key:" << key);
  
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. user:" << user.name);
  return user;
}



UserInfo NsAdapterCatalog::newUser(const std::string& uname) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "uname:" << uname);
  setDpnsApiIdentity();

  FunctionWrapper<int, uid_t, char*>(dpns_enterusrmap, -1, (char*)uname.c_str())();
  
  UserInfo u = this->getUser(uname);
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "uname:" << u.name);
  return u;
}



void NsAdapterCatalog::updateUser(const UserInfo& user) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "user:" << user.name);
  setDpnsApiIdentity();

  // uid may not be initialized
  UserInfo u = this->getUser(user.name);
  FunctionWrapper<int, uid_t, char*, int>(dpns_modifyusrmap,
                                          u.getUnsigned("uid"),
                                          (char*)user.name.c_str(),
                                          user.getLong("banned"))();
					  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. user:" << user.name);
}



void NsAdapterCatalog::deleteUser(const std::string& userName) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "userName:" << userName);
  
  setDpnsApiIdentity();

  UserInfo u = this->getUser(userName);
  try {
    FunctionWrapper<int, uid_t, char*>(dpns_rmusrmap, u.getUnsigned("uid"), (char*)u.name.c_str())();
  }
  catch (DmException& e) {
    if (e.code() != EINVAL) throw;
    throw DmException(DMLITE_NO_SUCH_USER, e.what());
  }
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. userName:" << userName);
}



std::vector<GroupInfo> NsAdapterCatalog::getGroups(void) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, " ngroups:" << groups.size());
  return groups;
}



std::vector<UserInfo> NsAdapterCatalog::getUsers(void) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. nusers:" << users.size());
  
  return users;
}



void NsAdapterCatalog::getIdMap(const std::string& userName,
                                const std::vector<std::string>& groupNames,
                                UserInfo* user, std::vector<GroupInfo>* groups) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "userName:" << userName);
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. userName:" << userName);
}



Directory* NsAdapterCatalog::openDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path:" << path);
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

  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "path:" << path);
  return privateDir;
}



void NsAdapterCatalog::closeDir(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting.");
}



struct dirent* NsAdapterCatalog::readDir(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  setDpnsApiIdentity();

  PrivateDir *privateDir = dynamic_cast<PrivateDir*>(dir);

  if (privateDir == NULL)
    throw DmException(EFAULT, "Tried to read a null directory");

  struct dirent* de = FunctionWrapper<dirent*, dpns_DIR*>(dpns_readdir, privateDir->dpnsDir)();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. de:" << ( (de != 0) ? de->d_name : "none") );
  return de;
}



ExtendedStat* NsAdapterCatalog::readDirx(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "");
  
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
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting. privateDir:" << direnstat->d_name );
  return &(privateDir->stat);
}



void NsAdapterCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, "path:" << path);
  
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, mode_t>(dpns_mkdir, path.c_str(), mode)();
  
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "Exiting.");
}



void NsAdapterCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "oldPath:" << oldPath << " newPath:" << newPath);
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*, const char*>(dpns_rename, oldPath.c_str(), newPath.c_str())();
  Log(Logger::Lvl2, adapterlogmask, adapterlogname, "oldPath:" << oldPath << " newPath:" << newPath);
}



void NsAdapterCatalog::removeDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl3, adapterlogmask, adapterlogname, "path:" << path);
  setDpnsApiIdentity();
  FunctionWrapper<int, const char*>(dpns_rmdir, path.c_str())();
  Log(Logger::Lvl2, adapterlogmask, adapterlogname, "path:" << path);
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
