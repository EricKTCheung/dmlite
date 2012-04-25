/// @file    plugins/librarian/Librarian.h
/// @brief   Filter replicas.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef LIBRARIAN_H
#define	LIBRARIAN_H

#include <dmlite/dmlite++.h>
#include <dmlite/dummy/Dummy.h>
#include <set>

namespace dmlite {

/// Librarian plugin
class LibrarianCatalog: public DummyCatalog {
public:
  /// Constructor
  /// @param decorates The underlying decorated catalog.
  LibrarianCatalog(StackInstance* si, Catalog* decorates) throw (DmException);

  /// Destructor
  ~LibrarianCatalog() throw (DmException);

  // Overloading
  std::string getImplId(void) throw ();
  
  void set      (const std::string&, va_list) throw (DmException);

  virtual std::vector<FileReplica> getReplicas(const std::string&)               throw (DmException);
  virtual Uri                      get        (const std::string&)               throw (DmException);

protected:
  void exclude   (int64_t replicaId);
  bool isExcluded(int64_t replicaId);
  
private:
  std::set<int64_t>  excluded_;
  StackInstance*     stack_;
};

/// Concrete factory for the Librarian plugin.
class LibrarianFactory: public CatalogFactory {
public:
  /// Constructor
  LibrarianFactory(CatalogFactory* catalogFactory) throw (DmException);
  /// Destructor
  ~LibrarianFactory() throw (DmException);

  void configure(const std::string& key, const std::string& value) throw (DmException);
  Catalog* createCatalog(StackInstance* si) throw (DmException);
  
protected:
  CatalogFactory* nestedFactory_;
private:
};

};

#endif	// LIBRARIAN_H
