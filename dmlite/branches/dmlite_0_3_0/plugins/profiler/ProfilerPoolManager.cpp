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



void ProfilerPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  PROFILE(setSecurityContext, ctx);
}



PoolMetadata* ProfilerPoolManager::getPoolMetadata(const Pool& pool) throw (DmException)
{
  PROFILE_RETURN(PoolMetadata*, getPoolMetadata, pool);
}



std::vector<Pool> ProfilerPoolManager::getPools(void) throw (DmException)
{
  PROFILE_RETURN(std::vector<Pool>, getPools);
}



Pool ProfilerPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  PROFILE_RETURN(pool, getPool, poolname);
}
