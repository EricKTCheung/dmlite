/// @file   Adapter.cpp
/// @brief  Adapter Plugin entry point.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <pthread.h>

#include "Adapter.h"
#include "DpmAdapter.h"
#include "FilesystemDriver.h"
#include "IO.h"
#include "NsAdapter.h"
#include "RFIO.h"


#include <Cthread_api.h>
#include <stdlib.h>

#include <utils/logger.h>

using namespace dmlite;



Logger::bitmask dmlite::adapterlogmask = 0;
Logger::component dmlite::adapterlogname = "Adapter";

int IntConnectionFactory::create() { return 1; }
void IntConnectionFactory::destroy(int) {};
bool IntConnectionFactory::isValid(int) { return true; }
IntConnectionFactory::~IntConnectionFactory()  {};



NsAdapterFactory::NsAdapterFactory() throw (DmException): retryLimit_(3), hostDnIsRoot_(false),
    hostDn_(), connectionPool_(&connectionFactory_, 10)
{
  adapterlogmask = Logger::get()->getMask(adapterlogname);
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " Hi.");
  
  Cthread_init();
  setenv("CSEC_MECH", "ID", 1);
  
}



NsAdapterFactory::~NsAdapterFactory()
{
  // Nothing
}



void NsAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  
  LogCfgParm(Logger::Lvl4, adapterlogmask, adapterlogname, key, value);
  
  if (key == "DpmHost" || key == "NsHost" || key == "Host") {
    setenv("DPNS_HOST", value.c_str(), 1);
    setenv("LFC_HOST", value.c_str(), 1);
    this->dpnsHost_ = value;
  }
  else if (key == "RetryLimit") {
    unsigned v = (unsigned)atoi(value.c_str());
    if (v > 0)
      this->retryLimit_ = v;
    else
      throw DmException(DMLITE_CFGERR(EINVAL), "RetryLimit must be equal or greater than 1");

    setenv("DPM_CONRETRY", value.c_str(), 1);
    setenv("DPNS_CONRETRY", value.c_str(), 1);
    setenv("LFC_CONRETRY", value.c_str(), 1);
  }
  else if (key == "ConnectionTimeout") {
    setenv("DPM_CONNTIMEOUT", value.c_str(), 1);
    setenv("DPNS_CONNTIMEOUT", value.c_str(), 1);
    setenv("LFC_CONNTIMEOUT", value.c_str(), 1);
  }
  else if (key == "RetryInterval") {
    setenv("DPM_CONRETRYINT", value.c_str(), 1);
    setenv("DPNS_CONRETRYINT", value.c_str(), 1);
    setenv("LFC_CONRETRYINT", value.c_str(), 1);
  }
  else if (key == "HostDNIsRoot")
    this->hostDnIsRoot_ = (value != "no");
  else if (key == "HostCertificate")
    this->hostDn_ = getCertificateSubject(value);
  else if (key == "ConnPoolSize")
    this->connectionPool_.resize(atoi(value.c_str()));
  else gotit = false;
  
  if (gotit)
    LogCfgParm(Logger::Lvl4, adapterlogmask, adapterlogname, key, value);
    
  
    
}

INode* NsAdapterFactory::createINode(PluginManager*) throw(DmException)
{
  return new NsAdapterINode(this->retryLimit_, this->hostDnIsRoot_,
                            this->hostDn_, this->dpnsHost_);
}


Catalog* NsAdapterFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new NsAdapterCatalog(this->retryLimit_, this->hostDnIsRoot_, this->hostDn_);
}



Authn* NsAdapterFactory::createAuthn(PluginManager*) throw (DmException)
{
  return new NsAdapterCatalog(this->retryLimit_, this->hostDnIsRoot_, this->hostDn_);
}



DpmAdapterFactory::DpmAdapterFactory() throw (DmException):
  retryLimit_(3), tokenPasswd_("default"), tokenUseIp_(true), tokenLife_(28800),
  adminUsername_("root"), connectionPool_(&connectionFactory_, 10)
{
  adapterlogmask = Logger::get()->getMask(adapterlogname);
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " Ctor");
  
  Cthread_init();
  setenv("CSEC_MECH", "ID", 1);
  
  dirspacereportdepth = 6;
}



DpmAdapterFactory::~DpmAdapterFactory()
{
  // Nothing
}



void DpmAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  Log(Logger::Lvl4, adapterlogmask, adapterlogname, " Key: " << key << " Value: " << value);
  
  if (key == "DpmHost" || key == "NsHost" || key == "Host") {
    setenv("DPM_HOST", value.c_str(), 1);
    setenv("DPNS_HOST", value.c_str(), 1);
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
  else if (key == "AdminUsername") {
    this->adminUsername_ = value;
  }
  else if (key == "ConnPoolSize")
    this->connectionPool_.resize(atoi(value.c_str()));
  else if (key == "AdapterDirectorySpaceReportDepth")
    this->dirspacereportdepth = atoi(value.c_str());
  else {
    gotit = false;
    NsAdapterFactory::configure(key, value);
  }
  
  if (gotit)
    Log(Logger::Lvl4, adapterlogmask, adapterlogname, "Setting parms. Key: " << key << " Value: " << value);
  
}



Catalog* DpmAdapterFactory::createCatalog(PluginManager*) throw (DmException)
{
  return new DpmAdapterCatalog(this, this->retryLimit_, this->hostDnIsRoot_, this->hostDn_);
}



PoolManager* DpmAdapterFactory::createPoolManager(PluginManager*) throw (DmException)
{
  return new DpmAdapterPoolManager(this, this->retryLimit_,
                                   this->tokenPasswd_, this->tokenUseIp_,
                                   this->tokenLife_);
}



std::string DpmAdapterFactory::implementedPool() throw ()
{
  return "filesystem";
}



PoolDriver* DpmAdapterFactory::createPoolDriver() throw (DmException)
{
  return new FilesystemPoolDriver(tokenPasswd_, tokenUseIp_, tokenLife_,
                                  retryLimit_, adminUsername_, dirspacereportdepth);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  NsAdapterFactory* nsFactory = new NsAdapterFactory();
  pm->registerAuthnFactory(nsFactory);
  pm->registerCatalogFactory(nsFactory);
  pm->registerINodeFactory(nsFactory);
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  DpmAdapterFactory* dpmFactory = new DpmAdapterFactory();
  
  pm->registerAuthnFactory(dpmFactory);
  pm->registerCatalogFactory(dpmFactory);
  pm->registerINodeFactory(dpmFactory);
  pm->registerPoolManagerFactory(dpmFactory);
  pm->registerPoolDriverFactory(dpmFactory);
}



static void registerPluginDriver(PluginManager* pm) throw (DmException)
{
   
  pm->registerPoolDriverFactory(new DpmAdapterFactory());
}



static void registerIOPlugin(PluginManager* pm) throw (DmException)
{
  
  pm->registerIODriverFactory(new StdIOFactory());
}



static void registerRFIOPlugin(PluginManager* pm) throw (DmException)
{
  
  pm->registerIODriverFactory(new StdRFIOFactory());
}


/// This is what the PluginManager looks for
PluginIdCard plugin_adapter_ns = {
  PLUGIN_ID_HEADER,
  registerPluginNs
};

PluginIdCard plugin_adapter_dpm = {
  PLUGIN_ID_HEADER,
  registerPluginDpm
};

PluginIdCard plugin_fs_pooldriver = {
  PLUGIN_ID_HEADER,
  registerPluginDriver
};

PluginIdCard plugin_fs_io = {
  PLUGIN_ID_HEADER,
  registerIOPlugin
};

PluginIdCard plugin_fs_rfio = {
  PLUGIN_ID_HEADER,
  registerRFIOPlugin
};
