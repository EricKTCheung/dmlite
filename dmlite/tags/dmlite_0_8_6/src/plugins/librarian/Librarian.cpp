/// @file    Librarian.cpp
/// @brief   Filter replicas.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Librarian.h"

using namespace dmlite;


LibrarianFactory::LibrarianFactory(CatalogFactory* catalogFactory) throw(DmException):
  nestedFactory_(catalogFactory)
{
  // Nothing
}



LibrarianFactory::~LibrarianFactory()
{
  // Nothing
}



void LibrarianFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
                    std::string("Unknown option ") + key);
}



Catalog* LibrarianFactory::createCatalog(PluginManager* pm) throw(DmException)
{
  return new LibrarianCatalog(CatalogFactory::createCatalog(this->nestedFactory_, pm));
}



static void registerPluginLibrarian(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nested = pm->getCatalogFactory();
  if (nested == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_CATALOG),
                      std::string("Librarian can not be loaded first"));
    
  pm->registerCatalogFactory(new LibrarianFactory(pm->getCatalogFactory()));
}



/// This is what the PluginManager looks for
PluginIdCard plugin_librarian = {
  PLUGIN_ID_HEADER,
  registerPluginLibrarian
};
