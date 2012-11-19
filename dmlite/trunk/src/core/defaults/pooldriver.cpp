#include <dmlite/cpp/pooldriver.h>
#include "NotImplemented.h"


using namespace dmlite;



PoolDriverFactory::~PoolDriverFactory()
{
  // Nothing
}



std::string PoolDriverFactory::implementedPool() throw ()
{
  return std::string();
}



FACTORY_NOT_IMPLEMENTED(PoolDriver* PoolDriverFactory::createPoolDriver(void) throw (DmException));



PoolDriver::~PoolDriver()
{
  // Nothing
}



NOT_IMPLEMENTED(PoolHandler* PoolDriver::createPoolHandler(const std::string&) throw (DmException));
NOT_IMPLEMENTED(void PoolDriver::toBeCreated(const Pool&) throw (DmException));
NOT_IMPLEMENTED(void PoolDriver::justCreated(const Pool&) throw (DmException));
NOT_IMPLEMENTED(void PoolDriver::update(const Pool&) throw (DmException));
NOT_IMPLEMENTED(void PoolDriver::toBeDeleted(const Pool&) throw (DmException));



PoolHandler::~PoolHandler()
{
  // Nothing
}



NOT_IMPLEMENTED_WITHOUT_ID(std::string PoolHandler::getPoolType(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(std::string PoolHandler::getPoolName(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(uint64_t PoolHandler::getTotalSpace(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(uint64_t PoolHandler::getFreeSpace(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(bool PoolHandler::poolIsAvailable(bool) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(bool PoolHandler::replicaIsAvailable(const Replica&) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(Location PoolHandler::whereToRead(const Replica&) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(void PoolHandler::removeReplica(const Replica&) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(Location PoolHandler::whereToWrite(const std::string&) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(void PoolHandler::cancelWrite(const std::string& path, const Location& loc) throw (DmException));
