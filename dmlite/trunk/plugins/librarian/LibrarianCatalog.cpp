/// @file    plugins/librarian/LibrarianCatalog.cpp
/// @brief   Filter replicas. Catalog implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <set>
#include <vector>
#include <dmlite/common/Uris.h>

#include "Librarian.h"

using namespace dmlite;

static bool isExcluded(uint64_t id, uint64_t* excluded, size_t size)
{
  for (int i = 0; i < size; ++i)
    if (excluded[i] == id)
      return true;
  return false;
}



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



std::vector<FileReplica> LibrarianCatalog::getReplicas(const std::string& path) throw (DmException)
{
  std::vector<FileReplica> replicas;
  Value excluded = this->stack_->get("ExcludeReplicas");

  if (this->decorated_ == 0x00)
    throw DmException(DM_NOT_IMPLEMENTED, "There is no plugin to serve get request");

  // Get all of them
  replicas = DummyCatalog::getReplicas(path);

  // Remove excluded
  std::vector<FileReplica>::iterator i;
  for (i = replicas.begin(); i != replicas.end();) {
    if (isExcluded(i->replicaid, excluded.array.u64v, excluded.array.size))
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
    PoolHandler* handler = this->stack_->getPoolHandler(pool);
    
    if (handler->replicaAvailable(path, replicas[i])) {
      uri = handler->getLocation(path, replicas[i]);
      found = true;
    }
  }
  
  if (found)
    return uri;
  else
    throw DmException(DM_NO_REPLICAS, "There is no replica in an active pool");
  
  
  return dmlite::splitUri(replicas[0].url);
}
