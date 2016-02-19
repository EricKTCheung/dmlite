/// @file   DomeAdapter.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_H
#define	DOME_ADAPTER_H

#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterFactory: public CatalogFactory, public AuthnFactory {
  public:
	  /// Constructor
    DomeAdapterFactory() throw (DmException);
	  /// Destructor
    virtual ~DomeAdapterFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);

    Catalog* createCatalog(PluginManager*)  throw (DmException);
    Authn*   createAuthn  (PluginManager*)  throw (DmException);
    //PoolContainer<int> *getPool() {return &connectionPool_;}
  };

}


#endif
