/// @file   Adapter.h
/// @brief  Dummy Plugin concrete factories.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef ADAPTER_H
#define	ADAPTER_H

#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/inode.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include "utils/logger.h"

namespace dmlite {


  extern Logger::bitmask adapterlogmask;
  extern Logger::component adapterlogname;
  extern Logger::bitmask adapterRFIOlogmask;
  extern Logger::component adapterRFIOlogname;
  
  /// Fake Factory for Pool resources
  class IntConnectionFactory: public PoolElementFactory<int> {
  public:
    IntConnectionFactory() {};
    virtual ~IntConnectionFactory();

    virtual int create();
    virtual void   destroy(int);
    virtual bool   isValid(int);

  protected:
  private:
  };



  /// Concrete factory for DPNS/LFC wrapper
  class NsAdapterFactory: public CatalogFactory, public INodeFactory, public AuthnFactory {
  public:
    /// Constructor
    NsAdapterFactory() throw (DmException);
    /// Destructor
    virtual ~NsAdapterFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);

    INode*   createINode  (PluginManager*)  throw (DmException);
    Catalog* createCatalog(PluginManager*)  throw (DmException);
    Authn*   createAuthn  (PluginManager*)  throw (DmException);

    PoolContainer<int> *getPool() {return &connectionPool_;}
  protected:
    unsigned retryLimit_;
    bool hostDnIsRoot_;
    std::string hostDn_;
    std::string dpnsHost_;

    /// Connection pool.
    IntConnectionFactory connectionFactory_;
    PoolContainer<int> connectionPool_;
  };

  /// Concrete factory for DPM wrapper
  class DpmAdapterFactory: public NsAdapterFactory, public PoolManagerFactory, public PoolDriverFactory {
  public:
    /// Constructor
    DpmAdapterFactory() throw (DmException);
    /// Destructor
    virtual ~DpmAdapterFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);

    Catalog*     createCatalog(PluginManager*)     throw (DmException);
    PoolManager* createPoolManager(PluginManager*) throw (DmException);

    std::string implementedPool() throw();
    PoolDriver* createPoolDriver(void) throw (DmException);

    PoolContainer<int> *getPool() {return &connectionPool_;}
  protected:
    unsigned retryLimit_;

    std::string tokenPasswd_;
    bool        tokenUseIp_;
    unsigned    tokenLife_;

    /// Admin username for replication.
    std::string adminUsername_;

    /// Connection pool.
    IntConnectionFactory connectionFactory_;
    PoolContainer<int> connectionPool_;
    
    int dirspacereportdepth;
  };

};

#endif	// ADAPTER_H

