/// @file   ProfilerCatalog.cpp
/// @brief  ProfilerCatalog implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
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
  if (this->stack_->contains("dictid")) {
    this->stack_->erase("dictid");
  }

  delete this->decorated_;
  delete [] this->decoratedId_;
}



std::string ProfilerCatalog::getImplId() const throw ()
{
  std::string implId = "ProfilerCatalog";
  implId += " over ";
  implId += this->decoratedId_;

  return implId;
}



void ProfilerCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack_ = si;
  BaseInterface::setStackInstance(this->decorated_, si);
}



void ProfilerCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);

  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  kXR_char dictid = Extensible::anyToUnsigned(this->stack_->get("dictid"));

  XrdMonitor::sendShortUserIdent(dictid);

  //XrdMonitor::sendUserIdent(dictid,
  //    // protocol
  //    ctx->user.name, // user DN
  //    ctx->credentials.remoteAddress // user hostname
  //    // org
  //    // role
  //    // grp
  //    // info
  //);
}



void ProfilerCatalog::changeDir(const std::string& path) throw (DmException)
{
  PROFILE(changeDir, path);
}



std::string ProfilerCatalog::getWorkingDir(void) throw(DmException)
{
  PROFILE_RETURN(std::string, getWorkingDir);
}



ExtendedStat ProfilerCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  reportXrdRedirCmd(path, XROOTD_MON_STAT);
  PROFILE_RETURN(ExtendedStat, extendedStat, path, follow);
}



ExtendedStat ProfilerCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  reportXrdRedirCmd(rfn, XROOTD_MON_STAT);
  PROFILE_RETURN(ExtendedStat, extendedStatByRFN, rfn);
}



bool ProfilerCatalog::access(const std::string& path, int mode) throw (DmException)
{
  PROFILE_RETURN(bool, access, path, mode);
}



bool ProfilerCatalog::accessReplica(const std::string& replica, int mode) throw (DmException)
{
  PROFILE_RETURN(bool, accessReplica, replica, mode);
}



void ProfilerCatalog::addReplica(const Replica& replica) throw (DmException)
{
  PROFILE(addReplica, replica);
}



void ProfilerCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  PROFILE(deleteReplica, replica);
}



std::vector<Replica> ProfilerCatalog::getReplicas(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(std::vector<Replica>, getReplicas, path);
}



void ProfilerCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  PROFILE(symlink, oldpath, newpath);
}



std::string ProfilerCatalog::readLink(const std::string& path) throw (DmException)
{
  PROFILE_RETURN(std::string, readLink, path);
}



void ProfilerCatalog::unlink(const std::string& path) throw (DmException)
{
  reportXrdRedirCmd(path, XROOTD_MON_RM);
  PROFILE(unlink, path);
}



void ProfilerCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  reportXrdRedirCmd(path, XROOTD_MON_OPENC);
  PROFILE(create, path, mode);
}



mode_t ProfilerCatalog::umask(mode_t mask) throw ()
{
  PROFILE_RETURN(mode_t, umask, mask);
}



void ProfilerCatalog::setMode(const std::string& path, mode_t mode) throw (DmException)
{
  PROFILE(setMode, path, mode);
}



void ProfilerCatalog::setOwner(const std::string& path, uid_t newUid, gid_t newGid, bool fs) throw (DmException)
{
  PROFILE(setOwner, path, newUid, newGid, fs);
}



void ProfilerCatalog::setSize(const std::string& path, size_t newSize) throw (DmException)
{
  PROFILE(setSize, path, newSize);
}



void ProfilerCatalog::setChecksum(const std::string& path, const std::string& csumtype, const std::string& csumvalue) throw (DmException)
{
  PROFILE(setChecksum, path, csumtype, csumvalue);
}



void ProfilerCatalog::setAcl(const std::string& path, const Acl& acls) throw (DmException)
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



void ProfilerCatalog::updateExtendedAttributes(const std::string& path,
                                               const Extensible& attr) throw (DmException)
{
  PROFILE(updateExtendedAttributes, path, attr);  
}



Directory* ProfilerCatalog::openDir(const std::string& path) throw (DmException)
{
  reportXrdRedirCmd(path, XROOTD_MON_OPENDIR);
  PROFILE_RETURN(Directory*, openDir, path);
}



void ProfilerCatalog::closeDir(Directory* dir) throw (DmException)
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
  reportXrdRedirCmd(path, XROOTD_MON_MKDIR);
  PROFILE(makeDir, path, mode);
}



void ProfilerCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  reportXrdRedirCmd(oldPath, XROOTD_MON_MV);
  PROFILE(rename, oldPath, newPath);
}



void ProfilerCatalog::removeDir(const std::string& path) throw (DmException)
{
  reportXrdRedirCmd(path, XROOTD_MON_RMDIR);
  PROFILE(removeDir, path);
}



Replica ProfilerCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException)
{
  PROFILE_RETURN(Replica, getReplicaByRFN, rfn);
}



void ProfilerCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  PROFILE(updateReplica, replica);
}


void ProfilerCatalog::reportXrdRedirCmd(const std::string &path, const int cmd_id)
{
  if (!this->stack_->contains("dictid")) {
    this->stack_->set("dictid", XrdMonitor::getDictId());
  }
  boost::any dictid_any = this->stack_->get("dictid");
  kXR_unt32 dictid = Extensible::anyToUnsigned(dictid_any);

  XrdMonitor::reportXrdRedirCmd(dictid, path, cmd_id);
}
