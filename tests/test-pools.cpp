#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/poolmanager.h>
#include <iostream>
#include <vector>

const float GB = 1073741824;


static void printPools(dmlite::StackInstance* stack, const std::vector<dmlite::Pool>& pools)
{
  for (unsigned i = 0; i < pools.size(); ++i) {
    try {
      dmlite::PoolHandler *handler = stack->getPoolDriver(pools[i].type)->createPoolHandler(pools[i].name);

      std::cout << "Pool type:   " << handler->getPoolType()   << std::endl
                << "Pool name:   " << handler->getPoolName()   << std::endl
                << "Capacity:    " << handler->getTotalSpace() / GB << " GB" << std::endl
                << "Free:        " << handler->getFreeSpace() / GB  << " GB" << std::endl;

      delete handler;
    }
    catch (dmlite::DmException& e) {
      if (e.code() != DM_UNKNOWN_POOL_TYPE)
        throw;
      std::cout << "Pool type:   " << pools[i].type << std::endl
                << "Pool name:   " << pools[i].name << std::endl
                << "UNKNOWN POOL TYPE!!" << std::endl;
    }
    std::cout << std::endl;
  }
}



int main(int argn, char **argv)
{
  // Need a plugin to load
  if (argn < 2) {
    std::cerr << "Usage: " << argv[0] << " <config file>" << std::endl;
    return -1;
  }

  // Load plugin
  dmlite::PluginManager  manager;
  dmlite::StackInstance* stack;
  dmlite::PoolManager*   poolManager;
  dmlite::GroupInfo      group;
  
  dmlite::SecurityCredentials credentials;
  
  credentials.clientName = "/C=CH/O=CERN/OU=GD/CN=Test user 0";
  credentials.mech       = "NONE";

  try {
    manager.loadConfiguration(argv[1]);
    stack = new dmlite::StackInstance(&manager);
    
    stack->setSecurityCredentials(credentials);
    
    poolManager = stack->getPoolManager();
  }
  catch (dmlite::DmException& e) {
    std::cerr << e.what() << std::endl;
    return -1;
  }

  // All
  std::cout << "============= ALL POOLS ============="
            << std::endl << std::endl;
  printPools(stack, poolManager->getPools(dmlite::PoolManager::kAny));
  
  // Available for read
  std::cout << "======== AVAILABLE FOR READ ========="
            << std::endl << std::endl;
  printPools(stack, poolManager->getPools(dmlite::PoolManager::kForRead));

  // Available for write
  std::cout << "======== AVAILABLE FOR WRITE ========"
            << std::endl << std::endl;
  printPools(stack, poolManager->getPools(dmlite::PoolManager::kForWrite));
  
  // Show not available
  std::cout << "=========== NOT AVAILABLE ==========="
            << std::endl << std::endl;
  printPools(stack, poolManager->getPools(dmlite::PoolManager::kNone));

  // Free
  delete stack;

  // Done
  return 0;
}
