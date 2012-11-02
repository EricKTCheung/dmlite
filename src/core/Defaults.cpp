/// @file   core/Defaults.cpp
/// @brief  Implementation of non abstract dmlite::Catalog methods.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/base.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/inode.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/pooldriver.h>
#include <dmlite/cpp/poolmanager.h>

using namespace dmlite;

/* Need to provide default constructors */
BaseInterface::~BaseInterface()
{
  // Nothing
}



BaseFactory::~BaseFactory()
{
  // Nothing
}



AuthnFactory::~AuthnFactory()
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



Authn::~Authn()
{
  // Nothing
}



IDirectory::~IDirectory()
{
  // Nothing
}



INode::~INode()
{
  // Nothing
}



Directory::~Directory()
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



struct stat IOHandler::fstat() throw (DmException)
{
  struct stat st;
  off_t where = this->tell();
  this->seek(0, kEnd);
  st.st_size = this->tell();
  this->seek(where, kSet);
  return st;
}



size_t IOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  size_t total = 0;
  for (size_t i = 0; i < count; ++i) {
    total += this->read(static_cast<char*>(vector[i].iov_base), vector[i].iov_len);
  }
  return total;
}



size_t IOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  size_t total = 0;
  for (size_t i = 0; i < count; ++i) {
    total += this->write(static_cast<char*>(vector[i].iov_base), vector[i].iov_len);
  }
  return total;
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
void BaseInterface::setStackInstance(BaseInterface* i, StackInstance* si) throw (DmException)
{
  if (i != NULL) i->setStackInstance(si);
}



void BaseInterface::setSecurityContext(BaseInterface* i, const SecurityContext* ctx) throw (DmException)
{
  if (i != NULL) i->setSecurityContext(ctx);
}



Authn* AuthnFactory::createAuthn(AuthnFactory* f, PluginManager* pm) throw (DmException)
{
  return f->createAuthn(pm);
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
