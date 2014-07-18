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
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );

  Log(Logger::INFO, profilerlogmask, profilerlogname, "");
}



ProfilerPoolManager::~ProfilerPoolManager()
{
  delete this->decorated_;
  free(this->decoratedId_);

  Log(Logger::INFO, profilerlogmask, profilerlogname, "");
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
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "availability: " << availability);
  PROFILE_RETURN(std::vector<Pool>, getPools, availability);
}



Pool ProfilerPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "poolname: " << poolname);
  PROFILE_RETURN(Pool, getPool, poolname);
}



void ProfilerPoolManager::newPool(const Pool& pool) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "pool: " << pool.name);
  PROFILE(newPool, pool);
}



void ProfilerPoolManager::updatePool(const Pool& pool) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "pool: " << pool.name);
  PROFILE(updatePool, pool);
}



void ProfilerPoolManager::deletePool(const Pool& pool) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "pool: " << pool.name);
  PROFILE(deletePool, pool);
}



Location ProfilerPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "path: " << path);
  PROFILE_ASSIGN(Location, whereToRead, path);
  //reportXrdRedirCmd(ret, XROOTD_MON_OPENR);
  Log(Logger::INFO, profilerlogmask, profilerlogname, "return: " << ret.toString());
  return ret;
}



Location ProfilerPoolManager::whereToRead(ino_t inode) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "inode: " << inode);
  PROFILE_RETURN(Location, whereToRead, inode);
}



Location ProfilerPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  Log(Logger::DEBUG, profilerlogmask, profilerlogname, "path: " << path);
  PROFILE_ASSIGN(Location, whereToWrite, path);
  //reportXrdRedirCmd(ret, XROOTD_MON_OPENW);
  Log(Logger::INFO, profilerlogmask, profilerlogname, "return: " << ret.toString());
  return ret;
}
