/// @file    plugins/librarian/LibrarianCatalog.cpp
/// @brief   Filter replicas. Catalog implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <set>
#include <vector>
#include <dmlite/common/Uris.h>

#include "Librarian.h"

using namespace dmlite;



LibrarianCatalog::LibrarianCatalog(StackInstance* si, Catalog* decorates) throw (DmException):
   DummyCatalog(decorates), stack_(si)
{
  // Nothing
}



LibrarianCatalog::~LibrarianCatalog() throw (DmException)
{
  // Nothing
}



std::string LibrarianCatalog::getImplId() throw ()
{
  return std::string("LibrarianCatalog");
}



void LibrarianCatalog::set(const std::string& key, va_list vargs) throw (DmException)
{
  if (key == "ExcludeReplicas") {
    int      n_list  = va_arg(vargs, int);
    int64_t* id_list = va_arg(vargs, int64_t*);

    for (int i = 0; i < n_list; ++i)
      this->exclude(id_list[i]);
  }
  else if (key == "ClearExcluded") {
    this->excluded_.clear();
  }
  else if (this->decorated_ != 0x00)
    this->decorated_->set(key, vargs);
  else
    throw DmException(DM_UNKNOWN_OPTION, "Unknown option " + key);
}



std::vector<FileReplica> LibrarianCatalog::getReplicas(const std::string& path) throw (DmException)
{
  std::vector<FileReplica> replicas;

  if (this->decorated_ == 0x00)
    throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin to serve get request");

  // Get all of them
  replicas = DummyCatalog::getReplicas(path);

  // Remove excluded
  std::vector<FileReplica>::iterator i;
  for (i = replicas.begin(); i != replicas.end();) {
    if (this->isExcluded(i->replicaid))
      i = replicas.erase(i);
    else
      ++i;
  }

  // Return
  if (replicas.size() == 0)
    throw DmException(DM_NO_REPLICAS, "There are no available replicas");
  return replicas;
}



Uri LibrarianCatalog::get(const std::string& path) throw (DmException)
{
  std::vector<FileReplica> replicas;

  // Get all the available
  replicas = this->getReplicas(path);

  // If there is no PoolManager, return the url field
  PoolManager *poolManager;
  try {
    poolManager = this->stack_->getPoolManager();
  }
  catch (DmException e) {
    if (e.code() != DM_NO_POOL_MANAGER)
      throw;
    return dmlite::splitUri(replicas[0].url);
  }
   
  Uri  uri;
  bool found = false;
  for (int i = 0; i < replicas.size() && !found; ++i) {
    Pool         pool    = this->stack_->getPoolManager()->getPool(replicas[0].pool);
    PoolHandler* handler = this->stack_->getPoolHandler(&pool);
    
    if (handler->replicaAvailable(replicas[i])) {
      uri = handler->getPhysicalLocation(replicas[i]);
      found = true;
    }
    
    delete handler;
  }
  
  if (found)
    return uri;
  else
    throw DmException(DM_NO_REPLICAS, "There is no replica in an active pool");
  
  
  return dmlite::splitUri(replicas[0].url);
}



void LibrarianCatalog::exclude(int64_t replicaId)
{
  this->excluded_.insert(replicaId);
}



bool LibrarianCatalog::isExcluded(int64_t replicaId)
{
  return this->excluded_.count(replicaId) != 0;
}
