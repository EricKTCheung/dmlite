/// @file    plugins/profiler/Profiler.cpp
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <fstream>
#include <iostream>

#include "Profiler.h"

using namespace dmlite;

ProfilerFactory::ProfilerFactory(CatalogFactory* catalogFactory) throw (DmException)
{
  this->nestedFactory_ = catalogFactory;
}



ProfilerFactory::~ProfilerFactory()
{
  // Nothing
}



void ProfilerFactory::set(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + value);
}



Catalog* ProfilerFactory::create() throw (DmException)
{
  return new ProfilerCatalog(this->nestedFactory_->create());
}



static void registerProfilerPlugin(PluginManager* pm) throw(DmException)
{
  try {
    pm->registerCatalogFactory(new ProfilerFactory(pm->getCatalogFactory()));
  }
  catch (DmException e) {
    if (e.code() == DM_NO_FACTORY)
      throw DmException(DM_NO_FACTORY, "Profiler can not be loaded first");
    else
      throw;
  }
}



/// This is what the PluginManager looks for
PluginIdCard plugin_profiler = {
  API_VERSION,
  registerProfilerPlugin
};
