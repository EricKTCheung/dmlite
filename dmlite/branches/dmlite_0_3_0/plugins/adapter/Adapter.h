/// @file   plugins/adapter/Adapter.h
/// @brief  Dummy Plugin concrete factories.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef ADAPTER_H
#define	ADAPTER_H

#include <dmlite/dmlite++.h>

namespace dmlite {

/// Concrete factory for DPNS/LFC wrapper
class NsAdapterFactory: public CatalogFactory {
public:
  /// Constructor
  NsAdapterFactory() throw (DmException);
  /// Destructor
  ~NsAdapterFactory();

  void configure(const std::string& key, const std::string& value) throw (DmException);
  Catalog* createCatalog()                                         throw (DmException);

protected:
  unsigned retryLimit_;
};

/// Concrete factory for DPM wrapper
class DpmAdapterFactory: public NsAdapterFactory, public PoolManagerFactory, public PoolHandlerFactory {
public:
  /// Constructor
  DpmAdapterFactory() throw (DmException);
  /// Destructor
  ~DpmAdapterFactory();

  void configure(const std::string& key, const std::string& value) throw (DmException);

  Catalog*     createCatalog()     throw (DmException);
  PoolManager* createPoolManager() throw (DmException);

  std::string implementedPool() throw();
  PoolHandler* createPoolHandler(Pool* pool) throw (DmException);

protected:
};

void ThrowExceptionFromSerrno(int serr) throw(DmException);
int   wrapCall(int   ret) throw (DmException);
void* wrapCall(void* ret) throw (DmException);

};

#endif	// ADAPTER_H

