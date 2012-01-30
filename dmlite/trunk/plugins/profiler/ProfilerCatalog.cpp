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



void ProfilerCatalog::changeDir(const std::string& path) throw (DmException)
{
  PROFILE(changeDir, path);
}



std::string ProfilerCatalog::getWorkingDir(void) throw(DmException)
{
  PROFILE_RETURN(std::string, getWorkingDir);
}



struct stat ProfilerCatalog::stat(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(struct stat, stat, path);
}



struct stat ProfilerCatalog::stat(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(struct stat, stat, inode);
}



struct stat ProfilerCatalog::linkStat(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(struct stat, linkStat, path);
}



struct xstat ProfilerCatalog::extendedStat(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(struct xstat, extendedStat, path);
}



struct xstat ProfilerCatalog::extendedStat(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(struct xstat, extendedStat, inode);
}



void ProfilerCatalog::addReplica(const std::string& guid, int64_t id,
                              const std::string& server, const std::string& sfn,
                              const char status, const char fileType,
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



FileReplica ProfilerCatalog::get(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(FileReplica, get, path);
}



void ProfilerCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  PROFILE(symlink, oldpath, newpath);
}



void ProfilerCatalog::unlink(const std::string& path) throw (DmException)
{
  PROFILE(unlink, path);
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
  PROFILE(umask, mask);
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



std::string ProfilerCatalog::getComment(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(std::string, getComment, path);
}



void ProfilerCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  PROFILE(setComment, path, comment);
}



GroupInfo ProfilerCatalog::getGroup(gid_t gid) throw (DmException)
{
  PROFILE_RETURN(GroupInfo, getGroup, gid);
}



GroupInfo ProfilerCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  PROFILE_RETURN(GroupInfo, getGroup, groupName);
}



void ProfilerCatalog::getIdMap(const std::string& userName,
                            const std::vector<std::string>& groupNames,
                            uid_t* uid, std::vector<gid_t>* gids) throw (DmException)
{
  PROFILE(getIdMap, userName, groupNames, uid, gids);
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



struct direntstat* ProfilerCatalog::readDirx(Directory* dir) throw (DmException)
{
  PROFILE_RETURN(struct direntstat*, readDirx, dir);
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



void ProfilerCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
  PROFILE(setUserId, uid, gid, dn);
}



void ProfilerCatalog::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
{
  PROFILE(setVomsData, vo, fqans);
}
