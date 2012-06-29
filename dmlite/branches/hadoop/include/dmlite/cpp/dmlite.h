/// @file   include/dmlite/dmlite++.h
/// @brief  Interface between libdm and client code.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_H
#define	DMLITEPP_H

#include <list>
#include <map>
#include <string>
#include "dm_auth.h"
#include "dm_catalog.h"
#include "dm_exceptions.h"
#include "dm_inode.h"
#include "dm_io.h"
#include "dm_pool.h"
#include "dm_pooldriver.h"
#include "../common/dm_errno.h"
#include "../common/dm_types.h"

/// Namespace for the libdm C++ API
namespace dmlite {

const unsigned API_VERSION = 20120627;

class StackInstance;

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
  
  /// Register a UserGroupDb factory. To be used by concrete implementations (i.e. Plugins)
  /// @param factory The UserDbGroup concrete factory.
  void registerFactory(UserGroupDbFactory* factory) throw (DmException);
  
  /// Register a INode factory. To be used by concrete implementations (i.e. Plugins)
  /// @param factory The INode concrete factory.
  void registerFactory(INodeFactory* factory) throw (DmException);

  /// Register a catalog factory. To be used by concrete implementations (i.e. Plugins)
  /// @param factory The catalog concrete factory.
  void registerFactory(CatalogFactory* factory) throw (DmException);

  /// Register a pool factory.
  /// @param factory The pool concrete factory.
  void registerFactory(PoolManagerFactory* factory) throw (DmException);

  /// Register a IO factory.
  /// @param factory The IO concrete factory.
  void registerFactory(IOFactory* factory) throw (DmException);

  /// Register a PoolDriver factory.
  /// @param factory The PoolDriver factory.
  void registerFactory(PoolDriverFactory* factory) throw (DmException);

  /// Get the UserGroupDbFactory implementation on top of the plugin stack.
  UserGroupDbFactory* getUserGroupDbFactory() throw (DmException);
  
  // Get the INodeFactory implementation on top of the plugin stack.
  INodeFactory* getINodeFactory() throw (DmException);
  
  /// Get the CatalogFactory implementation on top of the plugin stack.
  CatalogFactory* getCatalogFactory() throw (DmException);

  /// Get the PoolFactory implementation on top of the plugin stack.
  PoolManagerFactory* getPoolManagerFactory() throw (DmException);

  /// Get the appropiate pool driver factory for the pool.
  PoolDriverFactory* getPoolDriverFactory(const std::string& pooltype) throw (DmException);

  /// Get the IOFactory implementation on top of the plugin stack.
  IOFactory* getIOFactory() throw (DmException);
  
protected:
private:
  /// Internal list of loaded plug-ins.
  std::list<UserGroupDbFactory*> usergroup_plugins_;
  std::list<INodeFactory*>       inode_plugins_;
  std::list<CatalogFactory*>     catalog_plugins_;
  std::list<PoolManagerFactory*> pool_plugins_;
  std::list<IOFactory*>          io_plugins_;
  std::list<PoolDriverFactory*>  pool_driver_plugins_;

  /// Keep pointers returned by dlopen at hand to free on destruction
  std::list<void*> dlHandles_;
  
  /// Can not be copied
  PluginManager(const PluginManager&);
};

/// We need to have something that allows one plugin stack to access
/// another plugin stack, so this represents a instantiation
/// of each plugin stack.
/// It also keeps common state: user credentials, security context,
/// and run-time parameters (see set)
class StackInstance {
public:
  
  /// Constructor.
  StackInstance(PluginManager* pm) throw(DmException);
  
  /// Destructor.
  ~StackInstance() throw ();
  
  /// Set a key-value pair associated with this context.
  /// This can be used to pass advanced parameters to and from the plugins.
  /// @param key   The key.
  /// @param value The value.
  void set(const std::string& key, Value& value) throw (DmException);
  
  /// Get a value associated to a key.
  /// This can be used to pass advanced parameters to and from the plugins.
  /// @param key The key parameter.
  Value get(const std::string& key) throw (DmException);
  
  /// Erase a key,value pair from.
  /// @param key The key of the pair to be erased.
  void erase(const std::string& key) throw (DmException);
  
  /// Get the plugin manager.
  PluginManager* getPluginManager() throw (DmException);
  
  /// Set the security credentials.
  void setSecurityCredentials(const SecurityCredentials& cred) throw (DmException);
  
  /// Set the security context.
  void setSecurityContext(const SecurityContext& ctx) throw (DmException);
  
  /// Return the security context.
  const SecurityContext* getSecurityContext(void) const throw ();
  
  /// Get the UsersDb interface.
  UserGroupDb* getUserGroupDb() throw (DmException);
  
  /// Get the INode.
  INode* getINode() throw (DmException);
  
  /// Get the catalog.
  Catalog* getCatalog() throw (DmException);
  
  // Check if there is a PoolManager available
  bool isTherePoolManager() throw ();
  
  /// Get the PoolManager.
  PoolManager* getPoolManager() throw (DmException);
  
  /// Get a pool driver.
  PoolDriver* getPoolDriver(const std::string& poolType) throw (DmException);
  
  /// Get the IO driver.
  IODriver* getIODriver() throw (DmException);
  
private:
  PluginManager* pluginManager_;
  
  UserGroupDb*   ugDb_;
  INode*         inode_;
  Catalog*       catalog_;
  PoolManager*   poolManager_;
  IODriver*      ioDriver_;
  
  const SecurityContext* secCtx_;
  
  std::map<std::string, PoolDriver*> poolDrivers_;
  
  std::map<std::string, Value> stackMsg_;
};

/// Joint between plugins and plugin-manager
struct PluginIdCard {
  /// Used to make sure API is consistent.
  unsigned const  ApiVersion;
  /// Let the plug-in register itself and its concrete factories
  void (*registerPlugin)(PluginManager* pm) throw (DmException);
};

/// Macro intended to allow future expansions of the PluginIdCard header
/// easily.
#define PLUGIN_ID_HEADER API_VERSION

};

#endif	// DMLITEPP_H
