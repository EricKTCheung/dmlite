#include <dmlite/dmlite++.h>
#include <iostream>
#include <vector>

int main(int argn, char **argv)
{
  std::vector<Pool> pools;

  // Need a plugin to load
  if (argn < 3)
  {
    std::cerr << "Usage: " << argv[0] << " <so file> <plugin>" << std::endl;
    return -1;
  }

  // Load plugin
  dmlite::PluginManager manager;
  dmlite::Catalog      *catalog;

  try {
    manager.loadPlugin(argv[1], argv[2]);
    catalog = manager.getCatalogFactory()->create();
    // Ask for the pools
    pools = catalog->getPools();
  }
  catch (dmlite::DmException exc) {
    std::cerr << exc.what() << std::endl;
    return -1;
  }

  // Print info
  for (unsigned i = 0; i < pools.size(); ++i) {
    std::cout << "Pool name:   " << pools[i].poolname << std::endl
              << "Capacity:    " << pools[i].capacity << std::endl
              << "Free:        " << pools[i].free     << std::endl
              << "Filesystems: " << pools[i].nbelem   << std::endl;

    for (int j = 0; j < pools[i].nbelem; ++j) {
      std::cout << "\tServer:     " << pools[i].elemp[j].server << std::endl
                << "\tFilesystem: " << pools[i].elemp[j].fs     << std::endl
                << "\tCapacity:   " << pools[i].elemp[j].capacity << " bytes" << std::endl
                << "\tFree:       " << pools[i].elemp[j].free     << " bytes" << std::endl;
    }
    
    std::cout << std::endl;
  }


  // Free
  std::vector<Pool>::iterator i;
  for (i = pools.begin(); i != pools.end(); ++i) {
    delete [] i->gids;
    delete [] i->elemp;
  }
  delete catalog;

  // Done
  return 0;
}
