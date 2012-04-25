/// @file   plugins/adapter/Adapter.cpp
/// @brief  Adapter Plugin entry point.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/dmlite++.h>
#include <pthread.h>

#include "Adapter.h"
#include "FilesystemHandler.h"
#include "NsAdapter.h"
#include "DpmAdapter.h"

#include <stdlib.h>

using namespace dmlite;

// Prototypes of Cthread methods (not in the headers)
extern "C" {
int Cthread_init(void);
int _Cthread_addcid(char *, int, char *, int, pthread_t, unsigned, void *(*)(void *), int);
}

NsAdapterFactory::NsAdapterFactory() throw (DmException): retryLimit_(3)
{
  Cthread_init();
  _Cthread_addcid(NULL, 0, NULL, 0, pthread_self(), 0, NULL, 0);
  setenv("CSEC_MECH", "ID", 1);
}



NsAdapterFactory::~NsAdapterFactory()
{
  // Nothing
}



void NsAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "Host") {
    setenv("DPNS_HOST", value.c_str(), 1);
  }
  else if (key == "RetryLimit") {
    unsigned v = (unsigned)atoi(value.c_str());
    if (v > 0)
      this->retryLimit_ = v;
    else
      throw DmException(DM_INVALID_VALUE, "RetryLimit must be equal or greater than 1");
  }
  else
    throw DmException(DM_UNKNOWN_OPTION, "Unrecognised option " + key);
}



Catalog* NsAdapterFactory::createCatalog(StackInstance* si) throw (DmException)
{
  return new NsAdapterCatalog(this->retryLimit_);
}



DpmAdapterFactory::DpmAdapterFactory() throw (DmException): NsAdapterFactory()
{
  // Nothing
}



DpmAdapterFactory::~DpmAdapterFactory()
{
  // Nothing
}



void DpmAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "Host") {
    setenv("DPM_HOST", value.c_str(), 1);
    setenv("DPNS_HOST", value.c_str(), 1);
  }
  else
    NsAdapterFactory::configure(key, value);
}



Catalog* DpmAdapterFactory::createCatalog(StackInstance* si) throw (DmException)
{
  return new DpmAdapterCatalog(this->retryLimit_);
}



PoolManager* DpmAdapterFactory::createPoolManager(StackInstance* si) throw (DmException)
{
  return new DpmAdapterPoolManager(this->retryLimit_);
}



std::string DpmAdapterFactory::implementedPool() throw ()
{
  return "filesystem";
}



PoolHandler* DpmAdapterFactory::createPoolHandler(StackInstance* si, Pool* pool) throw (DmException)
{
  if (std::string(pool->pool_type) != this->implementedPool())
    throw DmException(DM_UNKNOWN_POOL_TYPE, "DpmAdapter does not recognise the pool type %s", pool->pool_type);
  return new FilesystemPoolHandler(si->getPoolManager(), pool);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new NsAdapterFactory());
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new DpmAdapterFactory());
  pm->registerPoolFactory(new DpmAdapterFactory());
  pm->registerPoolHandlerFactory(new DpmAdapterFactory());
}



static void registerPluginHandler(PluginManager* pm) throw (DmException)
{
  pm->registerPoolHandlerFactory(new DpmAdapterFactory());
}


/// This is what the PluginManager looks for
PluginIdCard plugin_adapter_ns = {
  API_VERSION,
  registerPluginNs
};

PluginIdCard plugin_adapter_dpm = {
  API_VERSION,
  registerPluginDpm
};

PluginIdCard plugin_handler_fs = {
  API_VERSION,
  registerPluginHandler
};
