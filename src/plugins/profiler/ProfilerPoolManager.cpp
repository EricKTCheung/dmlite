/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;



ProfilerPoolManager::ProfilerPoolManager(PoolManager* decorates) throw(DmException)
{
  this->decorated_   = decorates;

  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
}



ProfilerPoolManager::~ProfilerPoolManager()
{
  delete this->decorated_;
  delete this->decoratedId_;

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
      __func__);
}



std::string ProfilerPoolManager::getImplId() const throw ()
{
  std::string implId = "ProfilerPoolManager";
  implId += " over ";
  implId += this->decoratedId_;

  return implId;
}



void ProfilerPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}



void ProfilerPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}



std::vector<Pool> ProfilerPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  PROFILE_RETURN(std::vector<Pool>, getPools, availability);
}



Pool ProfilerPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  PROFILE_RETURN(Pool, getPool, poolname);
}



void ProfilerPoolManager::newPool(const Pool& pool) throw (DmException)
{
  PROFILE(newPool, pool);
}



void ProfilerPoolManager::updatePool(const Pool& pool) throw (DmException)
{
  PROFILE(updatePool, pool);
}



void ProfilerPoolManager::deletePool(const Pool& pool) throw (DmException)
{
  PROFILE(deletePool, pool);
}



Location ProfilerPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  PROFILE_ASSIGN(Location, whereToRead, path);
  //reportXrdRedirCmd(ret, XROOTD_MON_OPENR);

  return ret;
}



Location ProfilerPoolManager::whereToRead(ino_t inode) throw (DmException)
{
  PROFILE_RETURN(Location, whereToRead, inode);
}



Location ProfilerPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  PROFILE_ASSIGN(Location, whereToWrite, path);
  //reportXrdRedirCmd(ret, XROOTD_MON_OPENW);

  return ret;
}
