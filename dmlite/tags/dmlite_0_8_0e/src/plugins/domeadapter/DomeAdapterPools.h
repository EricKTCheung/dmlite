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
    virtual ~DomeAdapterPoolManager();

    virtual std::string getImplId() const throw ();

    virtual void setStackInstance(StackInstance* si) throw (DmException);
    virtual void setSecurityContext(const SecurityContext*) throw (DmException);

    virtual std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException);
    virtual Pool getPool(const std::string&) throw (DmException);

    virtual void newPool(const Pool& pool) throw (DmException);
    virtual void updatePool(const Pool& pool) throw (DmException);
    virtual void deletePool(const Pool& pool) throw (DmException);

    virtual Location whereToRead (const std::string& path) throw (DmException);
    virtual Location whereToWrite(const std::string& path) throw (DmException);

    virtual void cancelWrite(const Location& loc) throw (DmException);

    virtual void getDirSpaces(const std::string& path, int64_t &totalfree, int64_t &used) throw (DmException);
  private:
    StackInstance* si_;
    const SecurityContext* sec_;
    std::string userId_;

    /// The corresponding factory.
    DomeAdapterFactory* factory_;
  };
}

#endif
