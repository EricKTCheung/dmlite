/// @file   core/Catalog.cpp
/// @brief  Implementation of non abstract dm::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <cstdio>
#include <dmlite/dm_errno.h>
#include <dmlite/dm_interfaces.h>

using namespace dmlite;


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



CatalogFactory::~CatalogFactory()
{
  // Nothing
}
