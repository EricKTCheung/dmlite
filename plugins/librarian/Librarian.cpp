/// @file    plugins/librarian/Librarian.cpp
/// @brief   Filter replicas.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Librarian.h"

using namespace dmlite;


LibrarianFactory::LibrarianFactory(CatalogFactory* catalogFactory) throw(DmException):
  nestedFactory_(catalogFactory)
{
  // Nothing
}



LibrarianFactory::~LibrarianFactory() throw(DmException)
{
  // Nothing
}



void LibrarianFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



Catalog* LibrarianFactory::createCatalog(PluginManager* pm) throw(DmException)
{
  if (this->nestedFactory_ != 0x00)
    return new LibrarianCatalog(CatalogFactory::createCatalog(this->nestedFactory_, pm));
  else
    throw DmException(DM_NO_CATALOG, "Librarian need an underlying plug-in");
}



static void registerPluginLibrarian(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerFactory(new LibrarianFactory(pm->getCatalogFactory()));
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
