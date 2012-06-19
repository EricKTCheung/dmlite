/// @file   core/Catalog.cpp
/// @brief  Implementation of non abstract dm::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>

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



Catalog::Catalog() throw (DmException): parent_(0x00)
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

/* Common and default methods */

struct stat Catalog::stat(const std::string& path, bool followSym) throw (DmException)
{
  return this->extendedStat(path, followSym).stat;
}



void Catalog::setParent(Catalog* parent)
{
  this->parent_ = parent;
}



Catalog* Catalog::getParent(void)
{
  return this->parent_;
}
