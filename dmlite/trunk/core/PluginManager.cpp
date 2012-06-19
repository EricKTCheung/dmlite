/// @file   core/PluginManager.cpp
/// @brief  Implementation of dm::PluginManager
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dlfcn.h>
#include <dmlite/dmlite++.h>
#include <fstream>
#include <sstream>

#include "Private.h"
#include "builtin/Catalog.h"

using namespace dmlite;



PluginManager::PluginManager() throw()
{
  // Register built-in plugins
  this->registerFactory(new BuiltInCatalogFactory());
}



PluginManager::~PluginManager() throw()
{
  // Delete the instantiated factories
  for (std::list<UserGroupDbFactory*>::iterator i = this->usergroup_plugins_.begin();
       i != this->usergroup_plugins_.end();
       ++i) {
    delete *i;
  }
  
  for (std::list<INodeFactory*>::iterator i = this->inode_plugins_.begin();
       i != this->inode_plugins_.end();
       ++i) {
    delete *i;
  }
  
  for (std::list<CatalogFactory*>::iterator i = this->catalog_plugins_.begin();
       i != this->catalog_plugins_.end();
       ++i) {
    delete *i;
  }

  for (std::list<PoolManagerFactory*>::iterator i = this->pool_plugins_.begin();
       i != this->pool_plugins_.end();
       ++i) {
    delete *i;
  }

  for (std::list<IOFactory*>::iterator i = this->io_plugins_.begin();
       i != this->io_plugins_.end();
       ++i) {
    delete *i;
  }

  for (std::list<PoolDriverFactory*>::iterator i = this->pool_driver_plugins_.begin();
       i != this->pool_driver_plugins_.end();
       ++i) {
    delete *i;
  }

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
  // Bad option by default
  int r = -1;
  
  // UserGroupDb plugins
  for (std::list<UserGroupDbFactory*>::const_iterator i = this->usergroup_plugins_.begin();
       i != this->usergroup_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0; // At least one recognised this
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }  
  
  // INode plugins
  for (std::list<INodeFactory*>::const_iterator i = this->inode_plugins_.begin();
       i != this->inode_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0; // At least one recognised this
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }

  // Catalog plugins
  for (std::list<CatalogFactory*>::const_iterator i = this->catalog_plugins_.begin();
       i != this->catalog_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0; // At least one recognised this
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }

  // Pool manager plugins
  for (std::list<PoolManagerFactory*>::const_iterator i = this->pool_plugins_.begin();
       i != this->pool_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0; // At least one recognised this
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }

  // Pool handlers plugins
  for (std::list<PoolDriverFactory*>::const_iterator i = this->pool_driver_plugins_.begin();
       i != this->pool_driver_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0; // At least one recognised this
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }
  
  // IO Factories
  for (std::list<IOFactory*>::const_iterator i = this->io_plugins_.begin();
       i != this->io_plugins_.end(); ++i) {
    try {
      (*i)->configure(key, value);
      r = 0;
    }
    catch (DmException e) {
      if (e.code() != DM_UNKNOWN_OPTION)
        throw;
    }   
    catch (...) {
      throw DmException(DM_UNEXPECTED_EXCEPTION, "Unexpected exception catched");
    }
  }

  // Failed?
  if (r != 0)
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
          catch (DmException e) {
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
