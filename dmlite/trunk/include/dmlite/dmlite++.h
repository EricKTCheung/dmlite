/// @file   include/dm/dm.h
/// @brief  Interface between libdm and client code.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_H
#define	DMLITEPP_H

#include <list>
#include <string>
#include "dm_catalog.h"
#include "dm_errno.h"
#include "dm_exceptions.h"
#include "dm_pool.h"
#include "dm_types.h"

/// Namespace for the libdm C++ API
namespace dmlite {

const unsigned API_VERSION = 20120209;

/// CatalogInterface can only be instantiated through this class.
class PluginManager {
public:
  /// Constructor
  PluginManager() throw ();

  /// Destructor
  ~PluginManager() throw ();

  /// Load a plugin. Previously instantiated interfaces won't be affected.
  /// @param lib     The .so file. Usually, (path)/plugin_name.so.
  /// @param id      The plugin ID. Usually, plugin_name.
  void loadPlugin(const std::string& lib, const std::string& id) throw (DmException);

  /// Set a configuration parameter. It will be passed to the loaded plugins.
  /// @param key   The configuration parameter.
  /// @param value The value for the configuration parameter.
  void configure(const std::string& key, const std::string& value) throw (DmException);

  /// Load a configuration file, with plugins and parameters.
  /// @param file The configuration file.
  void loadConfiguration(const std::string& file) throw (DmException);

  /// Register a catalog factory. To be used by concrete implementations (i.e. Plugins)
  /// @param factory The catalog concrete factory.
  void registerCatalogFactory(CatalogFactory* factory) throw (DmException);

  // Register a pool factory.
  /// @param factory The pool concrete factory.
  void registerPoolFactory(PoolManagerFactory* factory) throw (DmException);

  /// Get the CatalogFactory implementation on top of the plugin stack.
  CatalogFactory* getCatalogFactory() throw (DmException);

  /// Get the PoolFactory implementation on top of the plugin stack.
  PoolManagerFactory* getPoolManagerFactory() throw (DmException);
  
protected:
private:
  /// Internal list of loaded plug-ins.
  std::list<CatalogFactory*>     catalog_plugins_;
  std::list<PoolManagerFactory*> pool_plugins_;

  /// Keep pointers returned by dlopen at hand to free on destruction
  std::list<void*> dlHandles_;
  
  /// Can not be copied
  PluginManager(const PluginManager&);
};

// Joint between plugins and plugin-manager
struct PluginIdCard {
  /// Used to make sure API is consistent.
  unsigned const  ApiVersion;
  /// Let the plug-in register itself and its concrete factories
  void (*registerPlugin)(PluginManager* pm) throw (DmException);
};

};

#endif	// DMLITEPP_H

