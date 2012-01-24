/// @file   plugins/adapter/Adapter.cpp
/// @brief  Adapter Plugin entry point.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/dmlite++.h>
#include <pthread.h>

#include "Adapter.h"
#include "NsAdapter.h"
#include "DpmAdapter.h"

using namespace dmlite;

// Prototypes of Cthread methods (not in the headers)
extern "C" {
int Cthread_init(void);
int _Cthread_addcid(char *, int, char *, int, pthread_t, unsigned, void *(*)(void *), int);
}

NsAdapterFactory::NsAdapterFactory() throw (DmException)
{
  this->nsHost_[0]  = '\0';
  this->retryLimit_ = 3;
  
  Cthread_init();
  _Cthread_addcid(NULL, 0, NULL, 0, pthread_self(), 0, NULL, 0);
}



NsAdapterFactory::~NsAdapterFactory()
{
  // Nothing
}



void NsAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "Host")
    this->nsHost_ = value;
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



Catalog* NsAdapterFactory::createCatalog() throw (DmException)
{
  return new NsAdapterCatalog(this->nsHost_, this->retryLimit_);
}



DpmAdapterFactory::DpmAdapterFactory() throw (DmException): NsAdapterFactory()
{
  this->dpmHost_[0] = '\0';
}



DpmAdapterFactory::~DpmAdapterFactory()
{
  // Nothing
}



void DpmAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "Host")
    this->dpmHost_ = value;
  else
    NsAdapterFactory::configure(key, value);
}



Catalog* DpmAdapterFactory::createCatalog() throw (DmException)
{
  return new DpmAdapter(this->dpmHost_, this->retryLimit_);
}



PoolManager* DpmAdapterFactory::createPoolManager() throw (DmException)
{
  return new DpmAdapter(this->dpmHost_, this->retryLimit_);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new NsAdapterFactory());
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new DpmAdapterFactory());
  pm->registerPoolFactory(new DpmAdapterFactory());
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
