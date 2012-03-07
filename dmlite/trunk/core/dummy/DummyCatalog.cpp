/// @file    core/dummy/DummyCatalog.cpp
/// @brief   DummyCatalog implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <dmlite/dummy/Dummy.h>

using namespace dmlite;



/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
if (this->decorated_ == 0x00)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);



DummyCatalog::DummyCatalog(Catalog* decorates) throw (DmException)
{
  this->decorated_ = decorates;
}



DummyCatalog::~DummyCatalog()
{
  delete this->decorated_;
}



std::string DummyCatalog::getImplId(void) throw ()
{
  return std::string("Dummy");;
}



void DummyCatalog::set(const std::string& key, va_list vargs) throw (DmException)
{
  DELEGATE(set, key, vargs);
}



void DummyCatalog::changeDir(const std::string& path) throw (DmException)
{
  DELEGATE(changeDir, path);
}



std::string DummyCatalog::getWorkingDir(void) throw (DmException)
{
  DELEGATE_RETURN(getWorkingDir);
}



ino_t DummyCatalog::getWorkingDirI(void) throw (DmException)
{
  DELEGATE_RETURN(getWorkingDirI);
}



ExtendedStat DummyCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  DELEGATE_RETURN(extendedStat, path, follow);
}



ExtendedStat DummyCatalog::extendedStat(ino_t inode) throw (DmException)
{
  DELEGATE_RETURN(extendedStat, inode);
}



ExtendedStat DummyCatalog::extendedStat(ino_t parent, const std::string& name) throw (DmException)
{
  DELEGATE_RETURN(extendedStat, parent, name);
}



SymLink DummyCatalog::readLink(ino_t inode) throw (DmException)
{
  DELEGATE_RETURN(readLink, inode);
}



void DummyCatalog::addReplica(const std::string& guid, int64_t id,
                              const std::string& server, const std::string& sfn,
                              char status, char fileType,
                              const std::string& poolName,
                              const std::string& fileSystem) throw (DmException)
{
  DELEGATE(addReplica, guid, id, server, sfn, status, fileType,
           poolName, fileSystem);
}



void DummyCatalog::deleteReplica(const std::string& guid, int64_t id,
                                 const std::string& sfn) throw (DmException)
{
  DELEGATE(deleteReplica, guid, id, sfn);
}



std::vector<FileReplica> DummyCatalog::getReplicas(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(getReplicas, path);
}



FileReplica DummyCatalog::get(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(get, path);
}



void DummyCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  DELEGATE(symlink, oldpath, newpath);
}



void DummyCatalog::unlink(const std::string& path) throw (DmException)
{
  DELEGATE(unlink, path);
}



void DummyCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  DELEGATE(create, path, mode);
}



std::string DummyCatalog::put(const std::string& path, Uri* uri) throw (DmException)
{
  DELEGATE_RETURN(put, path, uri);
}



std::string DummyCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
  DELEGATE_RETURN(put, path, uri, guid);
}



void DummyCatalog::putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException)
{
  DELEGATE(putStatus, path, token, uri);
}



void DummyCatalog::putDone(const std::string& path, const std::string& token) throw (DmException)
{
  DELEGATE(putDone, path, token);
}



mode_t DummyCatalog::umask(mode_t mask) throw ()
{
  DELEGATE_RETURN(umask, mask);
}



void DummyCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  DELEGATE(changeMode, path, mode);
}



void DummyCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  DELEGATE(changeOwner, path, newUid, newGid);
}



void DummyCatalog::linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException)
{
  DELEGATE(linkChangeOwner, path, newUid, newGid);
}



void DummyCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  DELEGATE(utime, path, buf);
}



std::string DummyCatalog::getComment(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(getComment, path);
}



void DummyCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  DELEGATE(setComment, path, comment);
}



GroupInfo DummyCatalog::getGroup(gid_t gid) throw (DmException)
{
  DELEGATE_RETURN(getGroup, gid);
}



GroupInfo DummyCatalog::getGroup(const std::string& groupName) throw (DmException)
{
  DELEGATE_RETURN(getGroup, groupName);
}



void DummyCatalog::getIdMap(const std::string& userName,
                            const std::vector<std::string>& groupNames,
                            uid_t* uid, std::vector<gid_t>* gids) throw (DmException)
{
  DELEGATE(getIdMap, userName, groupNames, uid, gids);
}



UserInfo DummyCatalog::getUser(const std::string& userName) throw (DmException)
{
  DELEGATE_RETURN(getUser, userName);
}



UserInfo DummyCatalog::getUser(uid_t uid) throw (DmException)
{
  DELEGATE_RETURN(getUser, uid);
}



void* DummyCatalog::openDir(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(openDir, path);
}



void DummyCatalog::closeDir(void* dir) throw (DmException)
{
  DELEGATE(closeDir, dir);
}



struct dirent* DummyCatalog::readDir(Directory* dir) throw (DmException)
{
  DELEGATE_RETURN(readDir, dir);
}



struct direntstat* DummyCatalog::readDirx(Directory* dir) throw (DmException)
{
  DELEGATE_RETURN(readDirx, dir);
}



void DummyCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  DELEGATE(makeDir, path, mode);
}



void DummyCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  DELEGATE(rename, oldPath, newPath);
}



void DummyCatalog::removeDir(const std::string& path) throw (DmException)
{
  DELEGATE(removeDir, path);
}



void DummyCatalog::replicaSetAccessTime(const std::string& replica) throw (DmException)
{
  DELEGATE(replicaSetAccessTime, replica);
}



void DummyCatalog::replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException)
{
  DELEGATE(replicaSetLifeTime, replica, ltime);
}



void DummyCatalog::replicaSetStatus(const std::string& replica, char status) throw (DmException)
{
  DELEGATE(replicaSetStatus, replica, status);
}



void DummyCatalog::replicaSetType(const std::string& replica, char type) throw (DmException)
{
  DELEGATE(replicaSetType, replica, type);
}



void DummyCatalog::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
  DELEGATE(setUserId, uid, gid, dn);
}



void DummyCatalog::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
{
  DELEGATE(setVomsData, vo, fqans);
}
