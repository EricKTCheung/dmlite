/// @file   DomeAdapter.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_H
#define	DOME_ADAPTER_H

#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/pooldriver.h>
#include "utils/DavixPool.h"

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterPoolManager;
  class DomeAdapterPoolDriver;
  class DomeAdapterPoolHandler;
  class DomeAdapterDiskCatalog;

  class DomeAdapterFactory : public CatalogFactory, public AuthnFactory,
                             public PoolManagerFactory, public PoolDriverFactory {
  public:
    /// Constructor
    DomeAdapterFactory() throw (DmException);
    /// Destructor
    virtual ~DomeAdapterFactory();

    void configure(const std::string &key, const std::string &value) throw (DmException);

    PoolManager* createPoolManager(PluginManager*) throw (DmException);
    PoolDriver* createPoolDriver() throw (DmException);
    Catalog* createCatalog(PluginManager*)  throw (DmException);
    Authn*   createAuthn  (PluginManager*)  throw (DmException);

    std::string implementedPool() throw();
  private:
    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;

    std::string domehead_;
    bool tokenUseIp_;
    std::string tokenPasswd_;
    unsigned tokenLife_;

  friend class DomeAdapterPoolManager;
  friend class DomeAdapterPoolDriver;
  friend class DomeAdapterPoolHandler;
  friend class DomeAdapterDiskCatalog;
  };

}


#endif
