/// @file   core/Catalog.cpp
/// @brief  Implementation of non abstract dm::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dmlite.h>

using namespace dmlite;

/* Need to provide default constructors */

UserGroupDbFactory::~UserGroupDbFactory()
{
  // Nothing
}



INodeFactory::~INodeFactory()
{
  // Nothing
}



CatalogFactory::~CatalogFactory()
{
  // Nothing
}



PoolManager::~PoolManager()
{
  // Nothing
}



PoolManagerFactory::~PoolManagerFactory()
{
  // Nothing
}



PoolDriverFactory::~PoolDriverFactory()
{
  // Nothing
}



PoolDriver::~PoolDriver()
{
  // Nothing
}



PoolHandler::~PoolHandler()
{
  // Nothing
}



PoolMetadata::~PoolMetadata()
{
  // Nothing
}



UserGroupDb::~UserGroupDb()
{
  // Nothing
}



INode::~INode()
{
  // Nothing
}



Catalog::~Catalog()
{
  // Nothing
}



IOHandler::~IOHandler()
{
  // Nothing
}



IOFactory::~IOFactory()
{
  // Nothing
}



IODriver::~IODriver()
{
  // Nothing
}

/* Common and default methods */
UserGroupDb* UserGroupDbFactory::createUserGroupDb(UserGroupDbFactory* f, PluginManager* pm) throw (DmException)
{
  return f->createUserGroupDb(pm);
}



INode* INodeFactory::createINode(INodeFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createINode(pm);
}



Catalog* CatalogFactory::createCatalog(CatalogFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createCatalog(pm);
}



PoolManager* PoolManagerFactory::createPoolManager(PoolManagerFactory* factory, PluginManager* pm) throw (DmException)
{
  return factory->createPoolManager(pm);
}
