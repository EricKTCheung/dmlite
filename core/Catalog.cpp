/// @file   core/Catalog.cpp
/// @brief  Implementation of non abstract dm::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <cstdio>
#include <dmlite/dm_catalog.h>

#include "dmlite/dm_pool.h"

using namespace dmlite;


Catalog::Catalog() throw (DmException): parent_(0x00)
{
  // Nothing
}



Catalog::~Catalog()
{
  // Nothing
}



PoolManager::~PoolManager()
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



void Catalog::setParent(Catalog* parent)
{
  this->parent_ = parent;
}



Catalog* Catalog::getParent(void)
{
  return this->parent_;
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
