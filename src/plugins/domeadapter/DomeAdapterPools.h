/// @file   DomeAdapterPools.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_POOLS_H
#define DOME_ADAPTER_POOLS_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/pooldriver.h>
#include <fstream>
#include "utils/DavixPool.h"

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterPoolManager;

  class DomeAdapterPoolsFactory : public PoolManagerFactory, public PoolDriverFactory {
  public:
    /// Constructor
    DomeAdapterPoolsFactory() throw (DmException);
    /// Destructor
    virtual ~DomeAdapterPoolsFactory();

    void configure(const std::string &key, const std::string &value) throw (DmException);

    PoolManager* createPoolManager(PluginManager*) throw (DmException);
    PoolDriver* createPoolDriver(PluginManager*) throw (DmException);
  private:
    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;

    std::string domehead;
  
  friend class DomeAdapterPoolManager;
  };

  class DomeAdapterPoolManager : public PoolManager {
  public:
    DomeAdapterPoolManager(DomeAdapterPoolsFactory *factory);
    ~DomeAdapterPoolManager();

    std::string getImplId() const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException);
    Pool getPool(const std::string&) throw (DmException);

    void newPool(const Pool& pool) throw (DmException);
    // void updatePool(const Pool& pool) throw (DmException);
    void deletePool(const Pool& pool) throw (DmException);

    Location whereToRead (const std::string& path) throw (DmException);
    // Location whereToRead (ino_t inode)             throw (DmException);
    Location whereToWrite(const std::string& path) throw (DmException);

    // void cancelWrite(const Location& loc) throw (DmException);
  private:
    StackInstance* si_;
    const SecurityContext* secCtx_;

    /// The corresponding factory.
    DomeAdapterPoolsFactory* factory_;
  };
}

#endif