/// @file    plugins/librarian/Librarian.cpp
/// @brief   Filter replicas.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Librarian.h"

using namespace dmlite;


LibrarianFactory::LibrarianFactory(CatalogFactory* catalogFactory) throw(DmException):
     DummyFactory(catalogFactory)
{
  // Nothing
}



LibrarianFactory::~LibrarianFactory() throw(DmException)
{
  // Nothing
}



void LibrarianFactory::set(const std::string& key, const std::string& value) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* LibrarianFactory::create() throw(DmException)
{
  if (this->nested_factory_ != 0x00)
    return new LibrarianCatalog(this->nested_factory_->create());
  else
    return new LibrarianCatalog(0x00);
}



static void registerPluginLibrarian(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerCatalogFactory(new LibrarianFactory(pm->getCatalogFactory()));
  }
  catch (DmException e) {
    if (e.code() == DM_NO_FACTORY)
      throw DmException(DM_NO_FACTORY, std::string("Librarian can not be loaded first"));
    throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_librarian = {
  API_VERSION,
  registerPluginLibrarian
};
