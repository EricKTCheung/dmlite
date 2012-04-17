/// @file    plugins/profiler/ProfilerCatalog.cpp
/// @brief   ProfilerCatalog implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include <string.h>

using namespace dmlite;


ProfilerCatalog::ProfilerCatalog(Catalog* decorates) throw(DmException)
{
  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());
}



ProfilerCatalog::~ProfilerCatalog()
{
  delete this->decorated_;
  delete [] this->decoratedId_;
}



std::string ProfilerCatalog::getImplId() throw ()
{
  return std::string("ProfilerCatalog");
}



void ProfilerCatalog::set(const std::string& key, ...) throw(DmException)
{
  va_list vargs;

  va_start(vargs, key);
  this->set(key, vargs);
  va_end(vargs);
}



void ProfilerCatalog::set(const std::string& key, va_list vargs) throw (DmException)
{
  PROFILE(set, key, vargs);
}



void ProfilerCatalog::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  PROFILE(setSecurityCredentials, cred);
}



void ProfilerCatalog::setSecurityContext(const SecurityContext& ctx)
{
  PROFILE(setSecurityContext, ctx);
}



const SecurityContext& ProfilerCatalog::getSecurityContext() throw (DmException)
{
  return this->decorated_->getSecurityContext();
}



void ProfilerCatalog::changeDir(const std::string& path) throw (DmException)
{
  PROFILE(changeDir, path);
}



std::string ProfilerCatalog::getWorkingDir(void) throw(DmException)
{
  PROFILE_RETURN(std::string, getWorkingDir);
}



ino_t ProfilerCatalog::getWorkingDirI(void) throw (DmException)
{
  PROFILE_RETURN(ino_t, getWorkingDirI);
}



struct stat ProfilerCatalog::stat(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(struct stat, stat, path);
}



struct stat ProfilerCatalog::stat(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(struct stat, stat, inode);
}



struct stat ProfilerCatalog::stat(ino_t parent, const std::string& name) throw (DmException)
{
  PROFILE_RETURN(struct stat, stat, parent, name);
}



struct stat ProfilerCatalog::linkStat(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(struct stat, linkStat, path);
}



ExtendedStat ProfilerCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  PROFILE_RETURN(ExtendedStat, extendedStat, path, follow);
}



ExtendedStat ProfilerCatalog::extendedStat(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(ExtendedStat, extendedStat, inode);
}



ExtendedStat ProfilerCatalog::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  PROFILE_RETURN(ExtendedStat, extendedStat, parent, name);
}



SymLink ProfilerCatalog::readLink(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(SymLink, readLink, inode);
}



void ProfilerCatalog::addReplica(const std::string& guid, int64_t id,
                              const std::string& server, const std::string& sfn,
                              char status, char fileType,
                              const std::string& poolName,
                              const std::string& fileSystem) throw (DmException)
{
  PROFILE(addReplica, guid, id, server, sfn, status, fileType,
           poolName, fileSystem);
}



void ProfilerCatalog::deleteReplica(const std::string& guid, int64_t id,
                                 const std::string& sfn) throw (DmException)
{
  PROFILE(deleteReplica, guid, id, sfn);
}



std::vector<FileReplica> ProfilerCatalog::getReplicas(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(std::vector<FileReplica>, getReplicas, path);
}



Uri ProfilerCatalog::get(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(Uri, get, path);
}



void ProfilerCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  PROFILE(symlink, oldpath, newpath);
}



void ProfilerCatalog::unlink(const std::string& path) throw (DmException)
{
  PROFILE(unlink, path);
}



void ProfilerCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  PROFILE(create, path, mode);
}



std::string ProfilerCatalog::put(const std::string& path, Uri* uri) throw (DmException)
{
  PROFILE_RETURN(std::string, put, path, uri);
}



std::string ProfilerCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
  PROFILE_RETURN(std::string, put, path, uri, guid);
}



void ProfilerCatalog::putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException)
{
  PROFILE(putStatus, path, token, uri);
}



void ProfilerCatalog::putDone(const std::string& path, const std::string& token) throw (DmException)
{
  PROFILE(putDone, path, token);
}



mode_t ProfilerCatalog::umask(mode_t mask) throw ()
{
  PROFILE_RETURN(mode_t, umask, mask);
}



void ProfilerCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  PROFILE(changeMode, path, mode);
}



void ProfilerCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  PROFILE(changeOwner, path, newUid, newGid);
}



void ProfilerCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  PROFILE(linkChangeOwner, path, newUid, newGid);
}



void ProfilerCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  PROFILE(setAcl, path, acls);
}



void ProfilerCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  PROFILE(utime, path, buf);
}



void ProfilerCatalog::utime(ino_t ino, const utimbuf* buf) throw (DmException)
{
  PROFILE(utime, ino, buf);
}



std::string ProfilerCatalog::getComment(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(std::string, getComment, path);
}



void ProfilerCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  PROFILE(setComment, path, comment);
}



void ProfilerCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  PROFILE(setGuid, path, guid);
}



GroupInfo ProfilerCatalog::getGroup(gid_t gid) throw (DmException)
{
  PROFILE_RETURN(GroupInfo, getGroup, gid);
}



GroupInfo ProfilerCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  PROFILE_RETURN(GroupInfo, getGroup, groupName);
}



UserInfo ProfilerCatalog::getUser(const std::string& userName) throw (DmException)
{
  PROFILE_RETURN(UserInfo, getUser, userName);
}



UserInfo ProfilerCatalog::getUser(uid_t uid) throw (DmException)
{
  PROFILE_RETURN(UserInfo, getUser, uid);
}



Directory* ProfilerCatalog::openDir(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(Directory*, openDir, path);
}



void ProfilerCatalog::closeDir(void* dir) throw (DmException)
{
  PROFILE(closeDir, dir);
}



struct dirent* ProfilerCatalog::readDir(Directory* dir) throw (DmException)
{
  PROFILE_RETURN(struct dirent*, readDir, dir);
}



ExtendedStat* ProfilerCatalog::readDirx(Directory* dir) throw (DmException)
{
  PROFILE_RETURN(ExtendedStat*, readDirx, dir);
}



void ProfilerCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  PROFILE(makeDir, path, mode);
}



void ProfilerCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  PROFILE(rename, oldPath, newPath);
}



void ProfilerCatalog::removeDir(const std::string& path) throw (DmException)
{
  PROFILE(removeDir, path);
}



void ProfilerCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  PROFILE(replicaSetAccessTime, replica);
}



void ProfilerCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  PROFILE(replicaSetLifeTime, replica, ltime);
}



void ProfilerCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  PROFILE(replicaSetStatus, replica, status);
}



void ProfilerCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  PROFILE(replicaSetType, replica, type);
}
