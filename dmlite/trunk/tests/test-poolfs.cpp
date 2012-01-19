#include <dmlite/dmlite++.h>
#include <iostream>
#include <vector>

int main(int argn, char **argv)
{
  std::vector<FileSystem> filesystems;

  // Need a plugin to load
  if (argn < 4)
  {
    std::cerr << "Usage: " << argv[0] << " <so file> <plugin> <pool>" << std::endl;
    return -1;
  }

  // Load plugin
  dmlite::PluginManager manager;
  dmlite::Catalog      *catalog;

  try {
    manager.loadPlugin(argv[1], argv[2]);
    catalog = manager.getCatalogFactory()->create();
  
    // Ask for the pool
    filesystems = catalog->getPoolFilesystems(argv[3]);
  }
  catch (dmlite::DmException exc) {
    std::cerr << exc.what() << std::endl;
    return -1;
  }

  // Print info
  std::cout << "Pool name:             " << argv[3] << std::endl
            << "Number of filesystems: " << filesystems.size() << std::endl << std::endl;

  for (unsigned i = 0; i < filesystems.size(); ++i) {
    std::cout << "Filesystem " << i << std::endl
              << "Server:   " << filesystems[i].server << std::endl
              << "Path:     " << filesystems[i].fs << std::endl
              << "Status:   " << filesystems[i].status << std::endl
              << "Free:     " << filesystems[i].free     << " bytes" << std::endl
              << "Capacity: " << filesystems[i].capacity << " bytes" << std::endl << std::endl;
  }

  // Free
  delete catalog;

  // Done
  return 0;
}
