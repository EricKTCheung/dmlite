/// @file   plugins/adapter/NsAdapter.cpp

#include <vector>

/// @brief  Adapter Plugin: Cns wrapper implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdlib>
#include <cstring>
#include <dmlite/dm_errno.h>
#include <serrno.h>

#include "Adapter.h"
#include "NsAdapter.h"
#include "../common/Uris.h"

using namespace dmlite;



NsAdapterCatalog::NsAdapterCatalog(const std::string& nsHost, unsigned retryLimit)
  throw (DmException): Catalog(), retryLimit_(retryLimit)
{
  const char *envDpns;

  if (nsHost.empty() || nsHost[0] == '\0') {
    envDpns = getenv("DPNS_HOST");
    if (envDpns)
      this->nsHost_ = std::string(envDpns);
    else
      this->nsHost_ = std::string("localhost");
  }
  else {
    this->nsHost_ = nsHost;
  }

  setenv("DPNS_HOST", this->nsHost_.c_str(), 1);
  setenv("CSEC_MECH", "ID", 1);

  // These need to be set to 0
  this->fqans_  = 0x00;
  this->nFqans_ = 0;
  this->vo_     = 0x00;
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



std::string NsAdapterCatalog::getImplId()
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



struct stat NsAdapterCatalog::stat(const std::string& path) throw (DmException)
{
  struct dpns_filestat dpnsStat;
  struct stat          stStat;

  if (dpns_stat(path.c_str(), &dpnsStat) != 0)
    ThrowExceptionFromSerrno(serrno);

  stStat.st_atim.tv_sec = dpnsStat.atime;
  stStat.st_ctim.tv_sec = dpnsStat.ctime;
  stStat.st_mtim.tv_sec = dpnsStat.mtime;
  stStat.st_gid   = dpnsStat.gid;
  stStat.st_uid   = dpnsStat.uid;
  stStat.st_nlink = dpnsStat.nlink;
  stStat.st_ino   = dpnsStat.fileid;
  stStat.st_mode  = dpnsStat.filemode;
  stStat.st_size  = dpnsStat.filesize;

  return stStat;
}



struct stat NsAdapterCatalog::linkStat(const std::string& path) throw (DmException)
{
  struct dpns_filestat dpnsStat;
  struct stat          stStat;

  if (dpns_lstat(path.c_str(), &dpnsStat) != 0)
    ThrowExceptionFromSerrno(serrno);

  stStat.st_atim.tv_sec = dpnsStat.atime;
  stStat.st_ctim.tv_sec = dpnsStat.ctime;
  stStat.st_mtim.tv_sec = dpnsStat.mtime;
  stStat.st_gid   = dpnsStat.gid;
  stStat.st_uid   = dpnsStat.uid;
  stStat.st_nlink = dpnsStat.nlink;
  stStat.st_ino   = dpnsStat.fileid;
  stStat.st_mode  = dpnsStat.filemode;
  stStat.st_size  = dpnsStat.filesize;

  return stStat;
}



void NsAdapterCatalog::addReplica(const std::string& guid, int64_t id,
                                  const std::string& server,
                                  const std::string& sfn, const char status,
                                  const char fileType,
                                  const std::string& poolName,
                                  const std::string& fileSystem) throw (DmException)
{
  struct dpns_fileid uniqueId;

  uniqueId.fileid = id;
  strncpy(uniqueId.server, this->nsHost_.c_str(), sizeof(uniqueId.server));

  wrapCall(dpns_addreplica(guid.c_str(), &uniqueId, server.c_str(),
                           sfn.c_str(), status, fileType,
                           poolName.c_str(), fileSystem.c_str()));
}



void NsAdapterCatalog::deleteReplica(const std::string& guid, int64_t id,
                                     const std::string& sfn) throw (DmException)
{
  struct dpns_fileid uniqueId;

  uniqueId.fileid = id;
  strncpy(uniqueId.server, this->nsHost_.c_str(), sizeof(uniqueId.server));

  wrapCall(dpns_delreplica(guid.c_str(), &uniqueId, sfn.c_str()));
}



std::vector<FileReplica> NsAdapterCatalog::getReplicas(const std::string& path) throw (DmException)
{
  struct dpns_filereplica *entries;
  int                      nEntries;
  std::vector<FileReplica> replicas;

  if (dpns_getreplica (path.c_str(), NULL, NULL, &nEntries, &entries) != 0)
    ThrowExceptionFromSerrno(serrno);
  if (nEntries == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

  replicas.reserve(nEntries);

  for (int i = 0; i < nEntries; ++i) {
    FileReplica replica;

    replica.replicaid = i; /* Assuming the order is kept */
    replica.fileid    = entries[i].fileid;
    replica.status    = entries[i].status;
    replica.location  = splitUri(std::string(entries[i].sfn));
    strncpy(replica.unparsed_location, entries[i].sfn, URI_MAX);

    replicas.push_back(replica);
  }
  
  free(entries);

  return replicas;
}



FileReplica NsAdapterCatalog::get(const std::string& path) throw (DmException)
{
  // Naive implementation: first occurrence
  // Better implementations are left for other plugins
  std::vector<FileReplica> replicas = this->getReplicas(path);

  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "No replicas found for " + path);

  return replicas[0];
}



void NsAdapterCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  wrapCall(dpns_symlink(oldpath.c_str(), newpath.c_str()));
}



void NsAdapterCatalog::unlink(const std::string& path) throw (DmException)
{
  wrapCall(dpns_unlink(path.c_str()));
}



std::string NsAdapterCatalog::put(const std::string& path, Uri* uri) throw (DmException)
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



GroupInfo NsAdapterCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  GroupInfo group;

  wrapCall(dpns_getgrpbynam((char*)groupName.c_str(), &group.gid));
  strcpy(group.name, groupName.c_str());
  group.banned = 0;

  return group;
}



void NsAdapterCatalog::getIdMap(const std::string& userName,
                                const std::vector<std::string>& groups,
                                uid_t* uid, std::vector<gid_t>* gids) throw (DmException)
{
  int         nGroups = groups.size();
  const char *groupNames[nGroups];
  gid_t       gidsp[nGroups + 1];

  for (int i = 0; i < nGroups; ++i)
    groupNames[i] = groups[i].c_str();

  wrapCall(dpns_getidmap(userName.c_str(), nGroups, groupNames, uid, gidsp));

  gids->reserve(nGroups);
  for(int i = 0; i < nGroups; ++i)
    gids->push_back(gidsp[i]);
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
  dpns_startsess((char*)this->nsHost_.c_str(), (char*)"dmlite::adapter::opendir");
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



struct direntstat* NsAdapterCatalog::readDirx(Directory* dir) throw (DmException)
{
  PrivateDir            *privateDir = static_cast<PrivateDir*>(dir);
  struct dpns_direnstat *direnstat;

  direnstat = dpns_readdirx(privateDir->dpnsDir);
  if (direnstat == 0x00)
    throw DmException(DM_NULL_POINTER, "Tried to read a null directory");

  privateDir->direntstat.dirent.d_ino = direnstat->fileid;
  strncpy(privateDir->direntstat.dirent.d_name, direnstat->d_name,
          sizeof(privateDir->direntstat.dirent.d_name));

  privateDir->direntstat.dirent.d_reclen = direnstat->d_reclen;
  privateDir->direntstat.dirent.d_type   = 0x00;
  privateDir->direntstat.dirent.d_off    = 0x00;

  privateDir->direntstat.stat.st_atim.tv_sec = direnstat->mtime;
  privateDir->direntstat.stat.st_ctim.tv_sec = direnstat->ctime;
  privateDir->direntstat.stat.st_mtim.tv_sec = direnstat->mtime;

  privateDir->direntstat.stat.st_gid  = direnstat->gid;
  privateDir->direntstat.stat.st_uid  = direnstat->uid;

  privateDir->direntstat.stat.st_mode  = direnstat->filemode;
  privateDir->direntstat.stat.st_size  = direnstat->filesize;
  privateDir->direntstat.stat.st_nlink = direnstat->nlink;

  privateDir->direntstat.stat.st_ino = privateDir->direntstat.dirent.d_ino;

  return &(privateDir->direntstat);
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
  wrapCall(dpns_rmdir(path.c_str()));
}



void NsAdapterCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
  wrapCall(dpns_client_setAuthorizationId(uid, gid, "GSI",
                                          (char*)dn.c_str()));
}



void NsAdapterCatalog::setVomsData(const std::string& vo,
                                   const std::vector<std::string>& fqans) throw (DmException)
{
  // Free any remaning
  if (this->fqans_ != 0x00) {
    for (int i = 0; i < this->nFqans_; ++i)
      delete [] this->fqans_[i];
    delete [] this->fqans_;
  }
  if (this->vo_ != 0x00)
    delete [] this->vo_;
  
  // Copy VO
  this->vo_ = new char[vo.length() + 1];
  strcpy(this->vo_, vo.c_str());

  // Allocate memory for the array
  this->nFqans_ = fqans.size();
  this->fqans_  = new char*[this->nFqans_];

  // Copy the fqans
  for (int i = 0; i < this->nFqans_; ++i) {
    this->fqans_[i] = new char [fqans[i].length() + 1];
    strcpy(this->fqans_[i], fqans[i].c_str());
  }  

  // Pass the data
  wrapCall(dpns_client_setVOMS_data((char*)this->vo_,
                                    this->fqans_, this->nFqans_));
}



std::vector<Pool> NsAdapterCatalog::getPools() throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "NsAdapterCatalog::getPools no implemented");
}



std::vector<FileSystem> NsAdapterCatalog::getPoolFilesystems(const std::string&) throw (DmException)
{
  throw DmException(DM_NOT_IMPLEMENTED, "NsAdapterCatalog::getPoolFilesystems no implemented");
}
