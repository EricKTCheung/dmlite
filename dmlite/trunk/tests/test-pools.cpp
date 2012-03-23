#include <dmlite/dmlite++.h>
#include <iostream>
#include <vector>

int main(int argn, char **argv)
{
  std::vector<Pool> pools;

  // Need a plugin to load
  if (argn < 2) {
    std::cerr << "Usage: " << argv[0] << " <config file>" << std::endl;
    return -1;
  }

  // Load plugin
  dmlite::PluginManager manager;
  dmlite::PoolManager  *poolManager;

  try {
    manager.loadConfiguration(argv[1]);
    poolManager = manager.getPoolManagerFactory()->createPoolManager();
    // Ask for the pools
    pools = poolManager->getPools();
  }
  catch (dmlite::DmException exc) {
    std::cerr << exc.what() << std::endl;
    return -1;
  }

  // Print info
  for (unsigned i = 0; i < pools.size(); ++i) {
    std::cout << "Pool type:   " << pools[i].pool_type << std::endl
              << "Pool name:   " << pools[i].pool_name << std::endl
              << "Capacity:    " << pools[i].capacity << " bytes" << std::endl
              << "Free:        " << pools[i].free     << " bytes" << std::endl;

    std::cout << std::endl;
  }


  // Free
  std::vector<Pool>::iterator i;
  for (i = pools.begin(); i != pools.end(); ++i) {
    if (i->gids)
      delete [] i->gids;
  }
  delete poolManager;

  // Done
  return 0;
}
