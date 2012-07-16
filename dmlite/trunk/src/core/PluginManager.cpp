/// @file   core/PluginManager.cpp
/// @brief  Implementation of dm::PluginManager
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dlfcn.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/dm_basefactory.h>
#include <fstream>
#include <sstream>
#include <set>
#include "builtin/Catalog.h"
#include "builtin/UserGroupDb.h"

using namespace dmlite;

/// Helper to populate unique pointer set
template <class T>
static void populateUnique(std::set<BaseFactory*>& unique,
                           const std::list<T*>& l)
{
  typename std::list<T*>::const_iterator i;
  
  for (i = l.begin(); i != l.end(); ++i) {
    if (unique.count(static_cast<BaseFactory*>(*i)) == 0)
      unique.insert(static_cast<BaseFactory*>(*i));
  }
}


/// Helper for configuration
template <class T>
static bool configureFactories(std::list<T*>& l,
                               const std::string& key, const std::string& value)
{
  typename std::list<T*>::iterator i;
  bool recognized=  false;
  
  for (i = l.begin(); i != l.end(); ++i) {
    try {
      (*i)->configure(key, value);
      recognized = true;
    }
    catch (DmException& e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }
  
  return recognized;
}


PluginManager::PluginManager() throw()
{
  // Register built-in plugins
  this->registerFactory(new BuiltInCatalogFactory());
  this->registerFactory(new BuiltInUserGroupDbFactory());
}



PluginManager::~PluginManager() throw()
{
  // Set with unique pointers
  std::set<BaseFactory*> uniqueFactories;
  
  populateUnique(uniqueFactories, this->usergroup_plugins_);
  populateUnique(uniqueFactories, this->inode_plugins_);
  populateUnique(uniqueFactories, this->catalog_plugins_);
  populateUnique(uniqueFactories, this->pool_plugins_);
  populateUnique(uniqueFactories, this->io_plugins_);
  populateUnique(uniqueFactories, this->pool_driver_plugins_);

  // Free
  for (std::set<BaseFactory*>::iterator i = uniqueFactories.begin();
       i != uniqueFactories.end(); ++i)
    delete *i;

  // dlclose
  std::list<void*>::iterator j;
  for (j = this->dlHandles_.begin();
       j != this->dlHandles_.end();
       ++j)
    dlclose(*j);
}



void PluginManager::loadPlugin(const std::string& lib, const std::string& id) throw(DmException)
{
  void         *dl;
  PluginIdCard *idCard;

  dl = dlopen(lib.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (dl == 0x00)
    throw DmException(DM_NO_SUCH_FILE, std::string(dlerror()));
  this->dlHandles_.push_back(dl);

  idCard = static_cast<PluginIdCard*>(dlsym(dl, id.c_str()));
  if (idCard == 0x00)
    throw DmException(DM_NO_SUCH_SYMBOL, std::string(dlerror()));

  if (idCard->ApiVersion < API_VERSION)
    throw DmException(DM_API_VERSION_MISMATCH, "Plugin version (%d) < API version (%d) - Consider upgrading the plug-in %s",
                      idCard->ApiVersion, API_VERSION, lib.c_str());
  else if (idCard->ApiVersion > API_VERSION)
    throw DmException(DM_API_VERSION_MISMATCH, "Plugin version (%d) > API version (%d) - Consider upgrading dmlite or downgrading the plugin %s",
                      idCard->ApiVersion, API_VERSION, lib.c_str());

  // Call the registerer
  idCard->registerPlugin(this);
}



void PluginManager::configure(const std::string& key, const std::string& value) throw(DmException)
{
  bool recognized = false;
  
  recognized |= configureFactories(this->usergroup_plugins_, key, value);
  recognized |= configureFactories(this->inode_plugins_, key, value);
  recognized |= configureFactories(this->catalog_plugins_, key, value);
  recognized |= configureFactories(this->pool_plugins_, key, value);
  recognized |= configureFactories(this->pool_driver_plugins_, key, value);
  recognized |= configureFactories(this->io_plugins_, key, value);

  if (!recognized)
    throw DmException(DM_UNKNOWN_OPTION, "Unknown option " + key);
}



void PluginManager::loadConfiguration(const std::string& file) throw(DmException)
{
  std::ifstream in(file.c_str(), std::ios_base::in);
  std::string   buffer;
  int           line;

  if (in.fail())
    throw DmException(DM_NO_SUCH_FILE, std::string("Could not open ") + file);

  line  = 1;

  while (!in.eof()) {
    getline(in, buffer);

    // Skip comments
    if (buffer[0] != '#') {
      std::istringstream stream(buffer);
      std::string parameter;

      stream >> parameter;

      // Skip comments and empty lines
      if (!parameter.empty() && parameter[0] != '#') {
        // Load a plugin
        if (parameter == "LoadPlugin") {
          std::string plugin, lib;
          // Get plugin
          if (stream.eof())
            throw DmException(DM_MALFORMED_CONF,
                              "Plugin field not specified at line %d",
                              line);
          stream >> plugin;
          // Get lib
          if (stream.eof())
            throw DmException(DM_MALFORMED_CONF,
                              "Library field not specified at line %d",
                              line);
          stream >> lib;
          this->loadPlugin(lib, plugin);
        }
        // Something that must be passed to loaded plugins
        else {
          std::string value;
          stream >> value;
          try {
            this->configure(parameter, value);
          }
          catch (DmException& e) {
            // Error code is good, but error message can be better here.
            std::ostringstream out;
            out << "Invalid configuration parameter " << parameter <<
                   " at line " << line;
            throw DmException(e.code(), out.str());
          }
        }
      }
    }
    ++line;
  }

  in.close();
}



void PluginManager::registerFactory(UserGroupDbFactory* factory) throw (DmException)
{
  this->usergroup_plugins_.push_front(factory);
}



UserGroupDbFactory* PluginManager::getUserGroupDbFactory() throw (DmException)
{
  if (this->usergroup_plugins_.empty())
    throw DmException(DM_NO_FACTORY, "There is no plugin at the top of the stack");
  else
    return this->usergroup_plugins_.front();
}



void PluginManager::registerFactory(INodeFactory* factory) throw (DmException)
{
  this->inode_plugins_.push_front(factory);
}



INodeFactory* PluginManager::getINodeFactory() throw (DmException)
{
  if (this->inode_plugins_.empty())
    throw DmException(DM_NO_FACTORY, "There is no plugin at the top of the stack");
  else
    return this->inode_plugins_.front();
}



void PluginManager::registerFactory(CatalogFactory* factory) throw(DmException)
{
  this->catalog_plugins_.push_front(factory);
}



CatalogFactory* PluginManager::getCatalogFactory() throw(DmException)
{
  if (this->catalog_plugins_.empty())
    throw DmException(DM_NO_FACTORY, "There is no plugin at the top of the stack");
  else
    return this->catalog_plugins_.front();
}



void PluginManager::registerFactory(PoolManagerFactory* factory) throw (DmException)
{
  this->pool_plugins_.push_front(factory);
}



PoolManagerFactory* PluginManager::getPoolManagerFactory() throw (DmException)
{
  if (this->pool_plugins_.empty())
    throw DmException(DM_NO_FACTORY, "There is no plugin at the top of the stack");
  else
    return this->pool_plugins_.front();
}



void PluginManager::registerFactory(IOFactory* factory) throw (DmException)
{
  this->io_plugins_.push_front(factory);
}



IOFactory* PluginManager::getIOFactory() throw (DmException)
{
  if (this->io_plugins_.empty())
    throw DmException(DM_NO_FACTORY, "There is no plugin at the top of the stack");
  else
    return this->io_plugins_.front();
}



void PluginManager::registerFactory(PoolDriverFactory* factory) throw (DmException)
{
  this->pool_driver_plugins_.push_front(factory);
}



PoolDriverFactory* PluginManager::getPoolDriverFactory(const std::string& pooltype) throw (DmException)
{
  std::list<PoolDriverFactory*>::iterator i;

  for (i = this->pool_driver_plugins_.begin();
       i != this->pool_driver_plugins_.end();
       ++i)
  {
    if ((*i)->implementedPool() == pooltype) {
      return *i;
    }
  }

  throw DmException(DM_UNKNOWN_POOL_TYPE, "No plugin recognises the pool type " + pooltype);
}
