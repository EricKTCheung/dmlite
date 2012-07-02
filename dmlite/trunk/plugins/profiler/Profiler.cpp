/// @file    plugins/profiler/Profiler.cpp
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <fstream>
#include <iostream>

#include "Profiler.h"

using namespace dmlite;

ProfilerFactory::ProfilerFactory(CatalogFactory* catalogFactory,
                                 PoolManagerFactory* poolManagerFactory) throw (DmException)
{
  this->nestedCatalogFactory_     = catalogFactory;
  this->nestedPoolManagerFactory_ = poolManagerFactory;
}



ProfilerFactory::~ProfilerFactory()
{
  // Nothing
}



void ProfilerFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + value);
}



Catalog* ProfilerFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  
  return new ProfilerCatalog(CatalogFactory::createCatalog(this->nestedCatalogFactory_, pm));
}



PoolManager* ProfilerFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  return new ProfilerPoolManager(PoolManagerFactory::createPoolManager(this->nestedPoolManagerFactory_, pm));
}



static void registerProfilerPlugin(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerFactory(static_cast<CatalogFactory*>(new ProfilerFactory(pm->getCatalogFactory(), 0x00)));
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }

  try {
    pm->registerFactory(static_cast<PoolManagerFactory*>(new ProfilerFactory(0x00, pm->getPoolManagerFactory())));
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_profiler = {
  PLUGIN_ID_HEADER,
  registerProfilerPlugin
};
