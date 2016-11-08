/// @file   ProfilerCatalog.cpp
/// @brief  ProfilerCatalog implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include <string.h>

using namespace dmlite;


ProfilerCatalog::ProfilerCatalog(Catalog* decorates) throw(DmException)
{
  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Ctor");

}



ProfilerCatalog::~ProfilerCatalog()
{
  delete this->decorated_;
  free(this->decoratedId_);

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "");
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
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}



void ProfilerCatalog::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}



void ProfilerCatalog::changeDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  PROFILE(changeDir, path);
}



std::string ProfilerCatalog::getWorkingDir(void) throw(DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  //sendUserIdentOrNOP();
  PROFILE_RETURN(std::string, getWorkingDir);
}


DmStatus ProfilerCatalog::extendedStat(ExtendedStat &xstat, const std::string& path, bool follow) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", follow: " << follow);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_STAT);
  PROFILE_RETURN(DmStatus, extendedStat, xstat, path, follow);
}

ExtendedStat ProfilerCatalog::extendedStat(const std::string& path, bool follow) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", follow: " << follow);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_STAT);
  PROFILE_RETURN(ExtendedStat, extendedStat, path, follow);
}



ExtendedStat ProfilerCatalog::extendedStatByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "rfn: " << rfn);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(rfn, XROOTD_MON_STAT);
  PROFILE_RETURN(ExtendedStat, extendedStatByRFN, rfn);
}



bool ProfilerCatalog::access(const std::string& path, int mode) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", mode: " << mode);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(bool, access, path, mode);
}



bool ProfilerCatalog::accessReplica(const std::string& replica, int mode) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "replica: " << replica << ", mode: " << mode);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(bool, accessReplica, replica, mode);
}



void ProfilerCatalog::addReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "replica: " << replica.rfn);
  //sendUserIdentOrNOP();
  PROFILE(addReplica, replica);
}



void ProfilerCatalog::deleteReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "replica: " << replica.rfn);
  //sendUserIdentOrNOP();
  PROFILE(deleteReplica, replica);
}



std::vector<Replica> ProfilerCatalog::getReplicas(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(std::vector<Replica>, getReplicas, path);
}



void ProfilerCatalog::symlink(const std::string& oldpath, const std::string& newpath) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "oldpath: " << oldpath << ", newpath: " << newpath);
  //sendUserIdentOrNOP();
  PROFILE(symlink, oldpath, newpath);
}



std::string ProfilerCatalog::readLink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(std::string, readLink, path);
}



void ProfilerCatalog::unlink(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_RM);
  PROFILE(unlink, path);
}



void ProfilerCatalog::create(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", mode: " << mode);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_OPENC);
  PROFILE(create, path, mode);
}



mode_t ProfilerCatalog::umask(mode_t mask) throw ()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "mask: " << mask);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(mode_t, umask, mask);
}



void ProfilerCatalog::setMode(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", mode: " << mode);
  //sendUserIdentOrNOP();
  PROFILE(setMode, path, mode);
}



void ProfilerCatalog::setOwner(const std::string& path, uid_t newUid, gid_t newGid, bool fs) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname,
          "path: " << path <<
          ", newUid: " << newUid <<
          ", newGid: " << newGid <<
          ", fs: " << fs
        );
  //sendUserIdentOrNOP();
  PROFILE(setOwner, path, newUid, newGid, fs);
}



void ProfilerCatalog::setSize(const std::string& path, size_t newSize) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", newSize: " << newSize);
  //sendUserIdentOrNOP();
  PROFILE(setSize, path, newSize);
}



void ProfilerCatalog::setChecksum(const std::string& path, const std::string& csumtype, const std::string& csumvalue) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname,
          "path: " << path <<
          ", csumtype: " << csumtype <<
          ", csumvalue: " << csumvalue
        );
  //sendUserIdentOrNOP();
  PROFILE(setChecksum, path, csumtype, csumvalue);
}

void ProfilerCatalog::getChecksum(const std::string& path, const std::string& csumtype,
              std::string& csumvalue, const std::string &pfn, const bool forcerecalc,
              const int waitsecs) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname,
          "path: " << path <<
          ", csumtype: " << csumtype <<
          ", forcerecalc: " << forcerecalc <<
          ", waitsecs: " << waitsecs
        );
  //sendUserIdentOrNOP();
  PROFILE(getChecksum, path, csumtype, csumvalue, pfn, forcerecalc, waitsecs);
}



void ProfilerCatalog::setAcl(const std::string& path, const Acl& acls) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", acls: " << acls.serialize());
  //sendUserIdentOrNOP();
  PROFILE(setAcl, path, acls);
}



void ProfilerCatalog::utime(const std::string& path, const struct utimbuf* buf) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", buf: " << buf);
  //sendUserIdentOrNOP();
  PROFILE(utime, path, buf);
}



std::string ProfilerCatalog::getComment(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(std::string, getComment, path);
}



void ProfilerCatalog::setComment(const std::string& path, const std::string& comment) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", comment: " << comment);
  //sendUserIdentOrNOP();
  PROFILE(setComment, path, comment);
}



void ProfilerCatalog::setGuid(const std::string& path, const std::string& guid) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", guid: " << guid);
  //sendUserIdentOrNOP();
  PROFILE(setGuid, path, guid);
}



void ProfilerCatalog::updateExtendedAttributes(const std::string& path,
                                               const Extensible& attr) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", attr size: " << (attr.end() - attr.begin()));
  //sendUserIdentOrNOP();
  PROFILE(updateExtendedAttributes, path, attr);
}



Directory* ProfilerCatalog::openDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_OPENDIR);
  PROFILE_RETURN(Directory*, openDir, path);
}



void ProfilerCatalog::closeDir(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "dir: " << dir);
  //sendUserIdentOrNOP();
  PROFILE(closeDir, dir);
}



struct dirent* ProfilerCatalog::readDir(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "dir: " << dir);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(struct dirent*, readDir, dir);
}



ExtendedStat* ProfilerCatalog::readDirx(Directory* dir) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "dir: " << dir);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(ExtendedStat*, readDirx, dir);
}



void ProfilerCatalog::makeDir(const std::string& path, mode_t mode) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path << ", mode: " << mode);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_MKDIR);
  PROFILE(makeDir, path, mode);
}



void ProfilerCatalog::rename(const std::string& oldPath, const std::string& newPath) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "oldPath: " << oldPath << ", newPath: " << newPath);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(oldPath, XROOTD_MON_MV);
  PROFILE(rename, oldPath, newPath);
}



void ProfilerCatalog::removeDir(const std::string& path) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "path: " << path);
  //sendUserIdentOrNOP();
  //reportXrdRedirCmd(path, XROOTD_MON_RMDIR);
  PROFILE(removeDir, path);
}



Replica ProfilerCatalog::getReplicaByRFN(const std::string& rfn) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "rfn: " << rfn);
  //sendUserIdentOrNOP();
  PROFILE_RETURN(Replica, getReplicaByRFN, rfn);
}



void ProfilerCatalog::updateReplica(const Replica& replica) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "replica: " << replica.rfn);
  //sendUserIdentOrNOP();
  PROFILE(updateReplica, replica);
}
