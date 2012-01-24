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



Catalog* ProfilerFactory::createCatalog() throw (DmException)
{
  return new ProfilerCatalog(this->nestedCatalogFactory_->createCatalog());
}



PoolManager* ProfilerFactory::createPoolManager() throw (DmException)
{
  return new ProfilerPoolManager(this->nestedPoolManagerFactory_->createPoolManager());
}



static void registerProfilerPlugin(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerCatalogFactory(new ProfilerFactory(pm->getCatalogFactory(), 0x00));
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }

  try {
    pm->registerPoolFactory(new ProfilerFactory(0x00, pm->getPoolManagerFactory()));
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_profiler = {
  API_VERSION,
  registerProfilerPlugin
};
