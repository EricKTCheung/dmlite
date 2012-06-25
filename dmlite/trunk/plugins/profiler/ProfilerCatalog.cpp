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



void ProfilerCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  PROFILE(setStackInstance, si);
}



void ProfilerCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  PROFILE(setSecurityContext, ctx);
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



ExtendedStat ProfilerCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  PROFILE_RETURN(ExtendedStat, extendedStat, path, follow);
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



Location ProfilerCatalog::get(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(Location, get, path);
}



Location ProfilerCatalog::put(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(Location, put, path);
}



Location ProfilerCatalog::put(const std::string& path, const std::string& guid) throw (DmException)
{
  PROFILE_RETURN(Location, put, path, guid);
}



void ProfilerCatalog::putDone(const std::string& host, const std::string& rfn, 
                              const std::map<std::string,std::string>& params) throw (DmException)
{
  PROFILE(putDone, host, rfn, params);
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



mode_t ProfilerCatalog::umask(mode_t mask) throw ()
{
  PROFILE_RETURN(mode_t, umask, mask);
}



void ProfilerCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  PROFILE(changeMode, path, mode);
}



void ProfilerCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid, bool fs) throw (DmException)
{
  PROFILE(changeOwner, path, newUid, newGid, fs);
}



void ProfilerCatalog::changeSize(const std::string& path, size_t newSize) throw (DmException)
{
  PROFILE(changeSize, path, newSize);
}



void ProfilerCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  PROFILE(setAcl, path, acls);
}



void ProfilerCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  PROFILE(utime, path, buf);
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
