/// @file   Profiler.cpp
/// @brief  Profiler plugin.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <fstream>
#include <iostream>

#include "Profiler.h"

using namespace dmlite;

ProfilerFactory::ProfilerFactory(CatalogFactory* catalogFactory,
                                 PoolManagerFactory* poolManagerFactory,
                                 IODriverFactory *ioFactory) throw (DmException)
{
  this->nestedCatalogFactory_     = catalogFactory;
  this->nestedPoolManagerFactory_ = poolManagerFactory;
  this->nestedIODriverFactory_    = ioFactory;

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s",
      "Profiler",
      "ProfilerFactory started");
}



ProfilerFactory::~ProfilerFactory()
{
  // Nothing
}



void ProfilerFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "Collector") {
    this->mon.collector_addr = value;
  } else if (key == "ProfileFunctions") {
    profile_functions_ = value == "yes" ? true : false;
  } else if (key == "MsgBufferSize") {
    this->mon.redir_max_buffer_size = atoi(value.c_str());
  } else {
    throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
        std::string("Unknown option ") + key);
  }
}



Catalog* ProfilerFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  initMonitor();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerCatalog nesting", this->nestedCatalogFactory_);
  return new ProfilerCatalog(CatalogFactory::createCatalog(this->nestedCatalogFactory_, pm), &mon);
}



PoolManager* ProfilerFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  initMonitor();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerPoolManager nesting", this->nestedPoolManagerFactory_);
  return new ProfilerPoolManager(PoolManagerFactory::createPoolManager(this->nestedPoolManagerFactory_, pm));
}


void ProfilerFactory::initMonitor() throw (DmException)
{
  if (!this->mon.isInitialized()) {
    if(this->mon.init() != 0) {
    throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
        std::string("Could not connect to the monitoring collector"));
    }
    char info[1024+256];

    snprintf(info, 1024+256, "%s.%d:%d@%s\n&pgm=%s&ver=%s",
             "dpmmgr", 1, 16, "localhost", "dpm", "1.8.8");

    this->mon.sendMonMap('=', 0, info);
  }
}




IODriver*   ProfilerFactory::createIODriver(PluginManager* pm)   throw (DmException)
{
  initMonitor();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerIODriver nesting", this->nestedIODriverFactory_);
  return new ProfilerIODriver(IODriverFactory::createIODriver(this->nestedIODriverFactory_, pm));
}




static void registerProfilerPlugin(PluginManager* pm) throw(DmException)
{
  ProfilerFactory *pf = new ProfilerFactory(pm->getCatalogFactory(),
                                            pm->getPoolManagerFactory(),
                                            pm->getIODriverFactory());
  pm->registerCatalogFactory(pf);
  pm->registerPoolManagerFactory(pf);
  pm->registerIODriverFactory(pf);
}



/// This is what the PluginManager looks for
PluginIdCard plugin_profiler = {
  PLUGIN_ID_HEADER,
  registerProfilerPlugin
};
