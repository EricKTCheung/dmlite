/// @file   plugins/adapter/Adapter.h
/// @brief  Dummy Plugin concrete factories.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef ADAPTER_H
#define	ADAPTER_H

#include <dmlite/dmlite++.h>

namespace dmlite {

/// Concrete factory for DPNS/LFC wrapper
class NsAdapterFactory: public CatalogFactory, public UserGroupDbFactory {
public:
  /// Constructor
  NsAdapterFactory() throw (DmException);
  /// Destructor
  ~NsAdapterFactory();

  void configure(const std::string& key, const std::string& value) throw (DmException);
  
  Catalog*     createCatalog(PluginManager*)     throw (DmException);  
  UserGroupDb* createUserGroupDb(PluginManager*) throw (DmException);

protected:
  unsigned retryLimit_;
};

/// Concrete factory for DPM wrapper
class DpmAdapterFactory: public NsAdapterFactory, public PoolManagerFactory, public PoolDriverFactory {
public:
  /// Constructor
  DpmAdapterFactory() throw (DmException);
  /// Destructor
  ~DpmAdapterFactory();

  void configure(const std::string& key, const std::string& value) throw (DmException);

  Catalog*     createCatalog(PluginManager*)     throw (DmException);
  PoolManager* createPoolManager(PluginManager*) throw (DmException);

  std::string implementedPool() throw();
  PoolDriver* createPoolDriver(StackInstance* si, const Pool& pool) throw (DmException);
  
protected:
  unsigned retryLimit_;
  
  std::string tokenPasswd_;
  bool        tokenUseIp_;
  unsigned    tokenLife_;
};

void ThrowExceptionFromSerrno(int serr, const char* extra = 0x00) throw(DmException);
int   wrapCall(int   ret) throw (DmException);
void* wrapCall(void* ret) throw (DmException);

};

#endif	// ADAPTER_H

