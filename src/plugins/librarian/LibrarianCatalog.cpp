/// @file    LibrarianCatalog.cpp
/// @brief   Filter replicas. Catalog implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <set>
#include <vector>
#include <dmlite/cpp/utils/urls.h>

#include "Librarian.h"

using namespace dmlite;

static bool isExcluded(uint64_t id, const std::vector<boost::any>& excluded)
{
  std::vector<boost::any>::const_iterator i;
  for (i = excluded.begin(); i != excluded.end(); ++i)
    if (Extensible::anyToU64(*i) == id)
      return true;
  return false;
}



LibrarianCatalog::LibrarianCatalog(Catalog* decorates) throw (DmException):
   DummyCatalog(decorates), stack_(0x00)
{
  // Nothing
}



LibrarianCatalog::~LibrarianCatalog() 
{
  // Nothing
}



std::string LibrarianCatalog::getImplId() const throw ()
{
  return std::string("LibrarianCatalog");
}



void LibrarianCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack_ = si;
  Catalog::setStackInstance(this->decorated_, si);
}



std::vector<Replica> LibrarianCatalog::getReplicas(const std::string& path) throw (DmException)
{
  std::vector<Replica> replicas;

  if (this->decorated_ == 0x00)
    throw DmException(DMLITE_SYSERR(ENOSYS),
                      "There is no plugin to serve get request");

  // Get all of them
  replicas = DummyCatalog::getReplicas(path);
  
  // Excluded must be a vector
  std::vector<boost::any> excluded;
  try {
    boost::any excludedAny = this->stack_->get("ExcludeReplicas");
    excluded = boost::any_cast<std::vector<boost::any> >(excludedAny);
  }
  catch (boost::bad_any_cast&) {
    throw DmException(EINVAL, "ExcludeReplicas is not an array");
  }
  catch (DmException& e) {
    if (e.code() != DMLITE_SYSERR(DMLITE_UNKNOWN_KEY)) throw;
  }

  // Remove excluded
  std::vector<Replica>::iterator i;
  for (i = replicas.begin(); i != replicas.end();) {
    if (isExcluded(i->replicaid, excluded))
      i = replicas.erase(i);
    else
      ++i;
  }

  // Return
  if (replicas.size() == 0)
    throw DmException(DMLITE_NO_REPLICAS, "There are no available replicas");
  return replicas;
}
