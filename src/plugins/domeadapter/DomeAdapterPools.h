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


namespace dmlite {

  class DomeAdapterFactory;

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterPoolManager : public PoolManager {
  public:
    DomeAdapterPoolManager(DomeAdapterFactory *factory);
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
    Location whereToWrite(const std::string& path) throw (DmException);

    void cancelWrite(const Location& loc) throw (DmException);
  private:
    StackInstance* si_;
    const SecurityCredentials* creds_;
    std::string userId_;

    /// The corresponding factory.
    DomeAdapterFactory* factory_;
  };
}

#endif