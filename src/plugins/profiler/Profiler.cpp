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
    XrdMonitor::collector_addr = value;
  } else if (key == "MsgBufferSize") {
    XrdMonitor::redir_max_buffer_size_ = atoi(value.c_str());
  } else {
    throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
        std::string("Unknown option ") + key);
  }
}



Catalog* ProfilerFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  initXrdMonitorIfNotInitialized();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerCatalog nesting", this->nestedCatalogFactory_);
  return new ProfilerCatalog(CatalogFactory::createCatalog(this->nestedCatalogFactory_, pm));
}



PoolManager* ProfilerFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  initXrdMonitorIfNotInitialized();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerPoolManager nesting", this->nestedPoolManagerFactory_);
  return new ProfilerPoolManager(PoolManagerFactory::createPoolManager(this->nestedPoolManagerFactory_, pm));
}


IODriver*   ProfilerFactory::createIODriver(PluginManager* pm)   throw (DmException)
{
  initXrdMonitorIfNotInitialized();
  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s %ld",
      "Profiler",
      "Creating ProfilerIODriver nesting", this->nestedIODriverFactory_);
  return new ProfilerIODriver(IODriverFactory::createIODriver(this->nestedIODriverFactory_, pm));
}


void ProfilerFactory::initXrdMonitorIfNotInitialized() throw (DmException)
{
  int ret;
  if(ret = XrdMonitor::initOrNOP() != 0) {
    throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
        std::string("Could not connect to the monitoring collector"));
  } else if (ret != XRDMON_FUNC_IS_NOP) {
    XrdMonitor::sendServerIdent();
  }
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
