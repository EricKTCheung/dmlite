/// @file   plugins/adapter/Adapter.cpp
/// @brief  Adapter Plugin entry point.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/dmlite++.h>
#include <pthread.h>

#include "Adapter.h"
#include "DpmAdapter.h"
#include "FilesystemDriver.h"
#include "IO.h"
#include "NsAdapter.h"


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
    setenv("LFC_HOST", value.c_str(), 1);
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



Catalog* NsAdapterFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new NsAdapterCatalog(this->retryLimit_);
}



UserGroupDb* NsAdapterFactory::createUserGroupDb(PluginManager*) throw (DmException)
{
  return new NsAdapterCatalog(this->retryLimit_);
}



DpmAdapterFactory::DpmAdapterFactory() throw (DmException):
  retryLimit_(3), tokenPasswd_("default"), tokenUseIp_(true), tokenLife_(600)
{
  Cthread_init();
  _Cthread_addcid(NULL, 0, NULL, 0, pthread_self(), 0, NULL, 0);
  setenv("CSEC_MECH", "ID", 1);
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
  else if (key == "RetryLimit") {
    unsigned v = (unsigned)atoi(value.c_str());
    if (v > 0)
      this->retryLimit_ = v;
    else
      throw DmException(DM_INVALID_VALUE, "RetryLimit must be equal or greater than 1"); 
  }
  else if (key == "TokenPassword") {
    this->tokenPasswd_ = value;
  }
  else if (key == "TokenId") {
    if (strcasecmp(value.c_str(), "ip") == 0)
      this->tokenUseIp_ = true;
    else
      this->tokenUseIp_ = false;
  }
  else if (key == "TokenLife") {
    this->tokenLife_ = (unsigned)atoi(value.c_str());
  }
  else
    throw DmException(DM_UNKNOWN_OPTION, "Unrecognised option " + key);
}



Catalog* DpmAdapterFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new DpmAdapterCatalog(this->retryLimit_,
                               this->tokenPasswd_, this->tokenUseIp_, this->tokenLife_);
}



PoolManager* DpmAdapterFactory::createPoolManager(PluginManager*) throw (DmException)
{
  return new DpmAdapterPoolManager(this->retryLimit_);
}



std::string DpmAdapterFactory::implementedPool() throw ()
{
  return "filesystem";
}



PoolDriver* DpmAdapterFactory::createPoolDriver() throw (DmException)
{
  return new FilesystemPoolDriver(tokenPasswd_, tokenUseIp_, tokenLife_);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerFactory((CatalogFactory*)new NsAdapterFactory());
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  pm->registerFactory(static_cast<CatalogFactory*>(new DpmAdapterFactory()));
  pm->registerFactory(static_cast<UserGroupDbFactory*>(new NsAdapterFactory()));
  pm->registerFactory(static_cast<PoolManagerFactory*>(new DpmAdapterFactory()));
  pm->registerFactory(static_cast<PoolDriverFactory*>(new DpmAdapterFactory()));
}



static void registerPluginDriver(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<PoolDriverFactory*>(new DpmAdapterFactory()));
}



static void registerIOPlugin(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(new StdIOFactory());
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

PluginIdCard plugin_fs_pooldriver = {
  API_VERSION,
  registerPluginDriver
};

PluginIdCard plugin_fs_io = {
  API_VERSION,
  registerIOPlugin
};
