/// @file    core/dummy/DummyPool.cpp
/// @brief   DummyPoolManager implementation.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dummy/DummyPool.h>

using namespace dmlite;



/// Little of help here to avoid redundancy
#define DELEGATE(method, ...) \
if (this->decorated_ == NULL)\
  throw DmException(DMLITE_SYSERR(ENOSYS),\
                    "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);


/// Little of help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
if (this->decorated_ == NULL)\
  throw DmException(DMLITE_SYSERR(ENOSYS),\
                    "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);



DummyPoolManager::DummyPoolManager(PoolManager* decorated) throw (DmException)
{
  this->decorated_ = decorated;
}



DummyPoolManager::~DummyPoolManager()
{
  delete this->decorated_;
}



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



void DummyPoolManager::newPool(const Pool& pool) throw (DmException)
{
  DELEGATE(newPool, pool);
}



void DummyPoolManager::updatePool(const Pool& pool) throw (DmException)
{
  DELEGATE(updatePool, pool);
}



void DummyPoolManager::deletePool(const Pool& pool) throw (DmException)
{
  DELEGATE(deletePool, pool);
}



Location DummyPoolManager::whereToRead(ino_t inode) throw (DmException)
{
  DELEGATE_RETURN(whereToRead, inode);
}



Location DummyPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(whereToRead, path);
}



Location DummyPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  DELEGATE_RETURN(whereToWrite, path);
}
