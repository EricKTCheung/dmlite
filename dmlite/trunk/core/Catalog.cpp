/// @file   core/Catalog.cpp
/// @brief  Implementation of non abstract dm::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <cstdio>
#include <dmlite/dm_catalog.h>

#include "dmlite/dm_pool.h"

using namespace dmlite;


PoolManager::~PoolManager()
{
  // Nothing
}



CatalogFactory::~CatalogFactory()
{
  // Nothing
}



AuthBase::~AuthBase()
{
  // Nothing
}



PoolManagerFactory::~PoolManagerFactory()
{
  // Nothing
}



void Catalog::set(const std::string& key, ...) throw (DmException)
{
  va_list vargs;

  va_start(vargs, key);
  this->set(key, vargs);
  va_end(vargs);
}



Catalog::Catalog() throw (DmException): parent_(0x00)
{
  // Nothing
}



Catalog::~Catalog()
{
  // Nothing
}

void Catalog::setParent(Catalog* parent)
{
  this->parent_ = parent;
}



Catalog* Catalog::getParent(void)
{
  return this->parent_;
}



struct stat Catalog::stat(const std::string& path) throw (DmException)
{
  return this->extendedStat(path, true).stat;
}



struct stat Catalog::stat(ino_t inode) throw (DmException)
{
  return this->extendedStat(inode).stat;
}



struct stat Catalog::stat(ino_t parent, const std::string& name) throw (DmException)
{
  return this->extendedStat(parent, name).stat;
}



struct stat Catalog::linkStat(const std::string& path) throw (DmException)
{
  return this->extendedStat(path, false).stat;
}
