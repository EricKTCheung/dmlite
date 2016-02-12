/// @file   DomeAdapter.cpp
/// @brief  Dome adapter plugin entry point.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <iostream>

#include "DomeAdapter.h"
#include "DomeAdapterCatalog.h"
#include "DomeAdapterIO.h"

using namespace dmlite;

Logger::bitmask dmlite::domeadapterlogmask = 0;
Logger::component dmlite::domeadapterlogname = "DomeAdapter";


void DomeAdapterFactory::configure(const std::string& key, const std::string& value) throw (DmException) 
{
  std::cout << "in DomeAdapterFactory::configure with " << key << " = " << value << std::endl;
  LogCfgParm(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, key, value);
}

Catalog* DomeAdapterFactory::createCatalog(PluginManager*) throw (DmException) 
{
  return new DomeAdapterCatalog();
}

Authn* DomeAdapterFactory::createAuthn(PluginManager*) throw (DmException)
{
  return new DomeAdapterCatalog();
}


DomeAdapterFactory::DomeAdapterFactory() throw (DmException) 
{

}

DomeAdapterFactory::~DomeAdapterFactory() {
	
}

static void registerPluginDomeAdapter(PluginManager* pm) throw(DmException) 
{
  DomeAdapterFactory *dmFactory = new DomeAdapterFactory();
  pm->registerCatalogFactory(dmFactory);
  pm->registerAuthnFactory(dmFactory);

  // pm->registerINodeFactory(nsFactory);
}

static void registerIOPlugin(PluginManager* pm) throw (DmException)
{
  pm->registerIODriverFactory(new DomeIOFactory());
}


PluginIdCard plugin_domeadapter = {
  PLUGIN_ID_HEADER,
  registerPluginDomeAdapter
};

PluginIdCard plugin_domeadapter_io = {
  PLUGIN_ID_HEADER,
  registerIOPlugin
};
