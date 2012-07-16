/// @file    core/dummy/DummyCatalog.cpp
/// @brief   DummyCatalog implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <dmlite/cpp/dummy/Dummy.h>

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



void DummyCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  DELEGATE(setStackInstance, si);
}



void DummyCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  DELEGATE(setSecurityContext, ctx);
}



void DummyCatalog::changeDir(const std::string& path) throw (DmException)
{
  DELEGATE(changeDir, path);
}



std::string DummyCatalog::getWorkingDir(void) throw (DmException)
{
  DELEGATE_RETURN(getWorkingDir);
}



ExtendedStat DummyCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  DELEGATE_RETURN(extendedStat, path, follow);
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



Location DummyCatalog::get(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(get, path);
}



Location DummyCatalog::put(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(put, path);
}



Location DummyCatalog::put(const std::string& path, const std::string& guid) throw (DmException)
{
  DELEGATE_RETURN(put, path, guid);
}



void DummyCatalog::putDone(const std::string& path, const std::string& rfn, 
                           const std::map<std::string,std::string>& params) throw (DmException)
{
  DELEGATE(putDone, path, rfn, params);
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



mode_t DummyCatalog::umask(mode_t mask) throw ()
{
  DELEGATE_RETURN(umask, mask);
}



void DummyCatalog::changeMode(const std::string& path, mode_t mode) throw (DmException)
{
  DELEGATE(changeMode, path, mode);
}



void DummyCatalog::changeOwner(const std::string& path, uid_t newUid, gid_t newGid, bool fs) throw (DmException)
{
  DELEGATE(changeOwner, path, newUid, newGid, fs);
}



void DummyCatalog::changeSize(const std::string& path, size_t newSize) throw (DmException)
{
  DELEGATE(changeSize, path, newSize);
}



void DummyCatalog::changeChecksum(const std::string& path,
                                  const std::string& csumtype,
                                  const std::string& csumvalue) throw (DmException)
{
  DELEGATE(changeChecksum, path, csumtype, csumvalue);
}



void DummyCatalog::setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException)
{
  DELEGATE(setAcl, path, acls);
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



void DummyCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  DELEGATE(setGuid, path, guid);
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



struct xstat* DummyCatalog::readDirx(Directory* dir) throw (DmException)
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
