/// @file    plugins/profiler/ProfilerPoolManager.cpp
/// @brief   ProfilerPoolManager implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"

#include <string.h>

using namespace dmlite;



ProfilerPoolManager::ProfilerPoolManager(PoolManager* decorates) throw(DmException)
{
  this->decorated_   = decorates;
  this->decoratedId_ = new char [decorates->getImplId().size() + 1];
  strcpy(this->decoratedId_, decorates->getImplId().c_str());
}



ProfilerPoolManager::~ProfilerPoolManager()
{
  delete this->decorated_;
  delete this->decoratedId_;
}



std::string ProfilerPoolManager::getImplId() throw ()
{
  return std::string("ProfilerPoolManager");
}



void ProfilerPoolManager::setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException)
{
  PROFILE(setUserId, uid, gid, dn);
}



void ProfilerPoolManager::setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException)
{
  PROFILE(setVomsData, vo, fqans);
}



std::vector<Pool> ProfilerPoolManager::getPools(void) throw (DmException)
{
  PROFILE_RETURN(std::vector<Pool>, getPools);
}



std::vector<FileSystem> ProfilerPoolManager::getPoolFilesystems(const std::string& poolname) throw (DmException)
{
  PROFILE_RETURN(std::vector<FileSystem>, getPoolFilesystems, poolname);
}



FileSystem ProfilerPoolManager::getFilesystem(const std::string& pool,
                                              const std::string& server,
                                              const std::string& fs) throw (DmException)
{
  PROFILE_RETURN(FileSystem, getFilesystem, pool, server, fs);
}
