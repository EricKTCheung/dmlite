/// @file    core/dummy/DummyCatalog.cpp
/// @brief   DummyPoolManager implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
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



void DummyPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  DELEGATE(setSecurityContext, ctx);
}



PoolMetadata* DummyPoolManager::getPoolMetadata(const std::string& pool) throw (DmException)
{
  DELEGATE_RETURN(getPoolMetadata, pool);
}



std::vector<Pool> DummyPoolManager::getPools(void) throw (DmException)
{
  DELEGATE_RETURN(getPools);
}



Pool DummyPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  DELEGATE_RETURN(getPool, poolname);
}



std::vector<Pool> DummyPoolManager::getAvailablePools(bool write) throw (DmException)
{
  DELEGATE_RETURN(getAvailablePools, write);
}
