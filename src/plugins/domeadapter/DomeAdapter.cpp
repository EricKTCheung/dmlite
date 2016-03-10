/// @file   DomeAdapter.cpp
/// @brief  Dome adapter plugin entry point.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <iostream>

#include "DomeAdapter.h"
#include "DomeAdapterCatalog.h"
#include "DomeAdapterIO.h"
#include "DomeAdapterPools.h"
#include "DomeAdapterDriver.h"

using namespace dmlite;

Logger::bitmask dmlite::domeadapterlogmask = ~0;
Logger::component dmlite::domeadapterlogname = "DomeAdapter";

DomeAdapterFactory::DomeAdapterFactory() throw (DmException) : davixPool_(&davixFactory_, 10) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);

}

DomeAdapterFactory::~DomeAdapterFactory() {
  
}

Catalog* DomeAdapterFactory::createCatalog(PluginManager*) throw (DmException) {
  return new DomeAdapterCatalog();
}

Authn* DomeAdapterFactory::createAuthn(PluginManager*) throw (DmException)
{
  return new DomeAdapterCatalog();
}

void DomeAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException) 
{
  std::cout << "in DomeAdapterFactory::configure with " << key << " = " << value << std::endl;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);

  if(key == "DomeHead") {
    domehead_ = value;
  }
  else if(key == "TokenPassword") {
    tokenPasswd_ = value;
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
  else if( key.find("Davix") != std::string::npos) {
    std::cout << "sending param to davix" << std::endl;
    davixFactory_.configure(key, value);
  }
}

std::string DomeAdapterFactory::implementedPool() throw() {
  return "filesystem";
}

PoolManager* DomeAdapterFactory::createPoolManager(PluginManager*) throw (DmException) {
  return new DomeAdapterPoolManager(this);
}

PoolDriver* DomeAdapterFactory::createPoolDriver() throw (DmException) {
  std::cout << "in create pool driver" << std::endl;
  return new DomeAdapterPoolDriver(this);
}

std::string DomeAdapterPoolManager::getImplId() const throw () {
  return "DomeAdapterPoolManager";
}

void DomeAdapterPoolManager::setStackInstance(StackInstance* si) throw (DmException) {
  si_ = si;
}

void DomeAdapterPoolManager::setSecurityContext(const SecurityContext* secCtx) throw (DmException) {
  secCtx_ = secCtx;
}

static void registerPluginDomeAdapter(PluginManager* pm) throw(DmException) {
  DomeAdapterFactory *dmFactory = new DomeAdapterFactory();
  pm->registerCatalogFactory(dmFactory);
  pm->registerAuthnFactory(dmFactory);
  // pm->registerINodeFactory(nsFactory);
}

static void registerIOPlugin(PluginManager* pm) throw (DmException) {
  pm->registerIODriverFactory(new DomeIOFactory());
}

static void registerDomeAdapterPools(PluginManager* pm) throw (DmException) {
  DomeAdapterFactory *dmFactory = new DomeAdapterFactory();
  pm->registerPoolManagerFactory(dmFactory);
  pm->registerPoolDriverFactory(dmFactory);
}


PluginIdCard plugin_domeadapter_ns = {
  PLUGIN_ID_HEADER,
  registerPluginDomeAdapter
};

PluginIdCard plugin_domeadapter_io = {
  PLUGIN_ID_HEADER,
  registerIOPlugin
};

PluginIdCard plugin_domeadapter_pools = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterPools
};
