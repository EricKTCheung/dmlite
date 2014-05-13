#include <dmlite/cpp/poolmanager.h>
#include "NotImplemented.h"


using namespace dmlite;



PoolManagerFactory::~PoolManagerFactory()
{
  // Nothing
}



FACTORY_NOT_IMPLEMENTED(PoolManager* PoolManagerFactory::createPoolManager(PluginManager* pm) throw (DmException));



PoolManager* PoolManagerFactory::createPoolManager(PoolManagerFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createPoolManager(pm);
}



PoolManager::~PoolManager()
{
  // Nothing
}



NOT_IMPLEMENTED(std::vector<Pool> PoolManager::getPools(PoolAvailability availability) throw (DmException));
NOT_IMPLEMENTED(Pool PoolManager::getPool(const std::string& poolname) throw (DmException));
NOT_IMPLEMENTED(void PoolManager::newPool(const Pool& pool) throw (DmException));
NOT_IMPLEMENTED(void PoolManager::updatePool(const Pool& pool) throw (DmException));
NOT_IMPLEMENTED(void PoolManager::deletePool(const Pool& pool) throw (DmException));
NOT_IMPLEMENTED(Location PoolManager::whereToRead(const std::string& path) throw (DmException));
NOT_IMPLEMENTED(Location PoolManager::whereToRead(ino_t inode) throw (DmException));
NOT_IMPLEMENTED(Location PoolManager::whereToWrite(const std::string& path) throw (DmException));
NOT_IMPLEMENTED(void PoolManager::cancelWrite(const Location& loc) throw (DmException));
