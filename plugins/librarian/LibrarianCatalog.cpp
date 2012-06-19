/// @file    plugins/librarian/LibrarianCatalog.cpp
/// @brief   Filter replicas. Catalog implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <set>
#include <vector>
#include <dmlite/common/Urls.h>

#include "Librarian.h"

using namespace dmlite;

static bool isExcluded(uint64_t id, uint64_t* excluded, size_t size)
{
  for (int i = 0; i < size; ++i)
    if (excluded[i] == id)
      return true;
  return false;
}



LibrarianCatalog::LibrarianCatalog(Catalog* decorates) throw (DmException):
   DummyCatalog(decorates), stack_(0x00)
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
