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



void ProfilerPoolManager::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  PROFILE(setSecurityCredentials, cred);
}



void ProfilerPoolManager::setSecurityContext(const SecurityContext& ctx)
{
  PROFILE(setSecurityContext, ctx);
}



const SecurityContext& ProfilerPoolManager::getSecurityContext() throw (DmException)
{
  return this->decorated_->getSecurityContext();
}



std::vector<Pool> ProfilerPoolManager::getPools(void) throw (DmException)
{
  PROFILE_RETURN(std::vector<Pool>, getPools);
}
