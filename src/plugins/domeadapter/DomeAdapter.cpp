/// @file   DomeAdapter.cpp
/// @brief  Dome adapter plugin entry point.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <iostream>

#include "DomeAdapter.h"
#include "DomeAdapterDiskCatalog.h"
#include "DomeAdapterHeadCatalog.h"
#include "DomeAdapterIO.h"
#include "DomeAdapterPools.h"
#include "DomeAdapterDriver.h"

using namespace dmlite;

Logger::bitmask dmlite::domeadapterlogmask = ~0;
Logger::component dmlite::domeadapterlogname = "DomeAdapter";

DomeAdapterFactory::DomeAdapterFactory() throw (DmException) : davixPool_(&davixFactory_, 64) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);

}

DomeAdapterFactory::~DomeAdapterFactory() {

}

void DomeAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
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
  return new DomeAdapterPoolDriver(this);
}

Catalog* DomeAdapterFactory::createCatalog(PluginManager*) throw (DmException) {
  return new DomeAdapterDiskCatalog(this);
}

Authn* DomeAdapterFactory::createAuthn(PluginManager*) throw (DmException) {
  return new DomeAdapterDiskCatalog(this);
}

static void registerDomeAdapterDiskCatalog(PluginManager* pm) throw(DmException) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "registerDomeAdapterDiskCatalog");

  DomeAdapterFactory *dmFactory = new DomeAdapterFactory();
  pm->registerCatalogFactory(dmFactory);
  pm->registerAuthnFactory(dmFactory);
}

static void registerIOPlugin(PluginManager* pm) throw (DmException) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "registerIOPlugin");

  pm->registerIODriverFactory(new DomeIOFactory());
}

static void registerDomeAdapterPools(PluginManager* pm) throw (DmException) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "registerDomeAdapterPools");

  DomeAdapterFactory *dmFactory = new DomeAdapterFactory();
  pm->registerPoolManagerFactory(dmFactory);
  pm->registerPoolDriverFactory(dmFactory);
}

static void registerDomeAdapterHeadCatalog(PluginManager* pm) throw (DmException) {
  domeadapterlogmask = Logger::get()->getMask(domeadapterlogname);
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "registerDomeAdapterHeadCatalog");

  CatalogFactory* nestedCAT = pm->getCatalogFactory();

  if (nestedCAT == NULL)
    throw DmException(DMLITE_SYSERR(DMLITE_NO_CATALOG),
        std::string("Head catalog wraps another catalog (usually the mysql one) and cannot be loaded first. "
                     "You probably need to load the head catalog after the mysql plugin - remember the config files are "
                     "processed by lexicographical order. "));

  DomeAdapterHeadCatalogFactory *factory = new DomeAdapterHeadCatalogFactory(nestedCAT);
  pm->registerCatalogFactory(factory);
}

PluginIdCard plugin_domeadapter_headcatalog = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterHeadCatalog
};

PluginIdCard plugin_domeadapter_diskcatalog = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterDiskCatalog
};

PluginIdCard plugin_domeadapter_io = {
  PLUGIN_ID_HEADER,
  registerIOPlugin
};

PluginIdCard plugin_domeadapter_pools = {
  PLUGIN_ID_HEADER,
  registerDomeAdapterPools
};
