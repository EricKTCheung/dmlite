/// @file    core/dummy/DummyCatalog.cpp
/// @brief   DummyPoolManager implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dummy/DummyPool.h>

using namespace dmlite;



/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
if (this->decorated_ == NULL)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
if (this->decorated_ == NULL)\
  throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);



void DummyPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  BaseInterface::setStackInstance(this->decorated_, si);
}



void DummyPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}



std::vector<Pool> DummyPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  DELEGATE_RETURN(getPools, availability);
}



Pool DummyPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  DELEGATE_RETURN(getPool, poolname);
}



Location DummyPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(whereToRead, path);
}



Location DummyPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(whereToWrite, path);
}



void DummyPoolManager::doneWriting(const std::string& host,
                                   const std::string& rfn,
                                   const Extensible& params) throw (DmException)
{
  DELEGATE(doneWriting, host, rfn, params);
}
