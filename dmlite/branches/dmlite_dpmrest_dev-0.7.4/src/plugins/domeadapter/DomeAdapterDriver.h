/// @file   DomeAdapterDriver.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_DRIVER_H
#define DOME_ADAPTER_DRIVER_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/pooldriver.h>
#include <fstream>
#include "utils/DavixPool.h"

namespace dmlite {

  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterPoolsFactory;

  class DomeAdapterPoolDriver : public PoolDriver {
  public:
    DomeAdapterPoolDriver(DomeAdapterPoolsFactory* factory);
    ~DomeAdapterPoolDriver();

    std::string getImplId() const throw();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);

    // void toBeCreated(const Pool& pool) throw (DmException);
    // void justCreated(const Pool& pool) throw (DmException);
    // void update(const Pool& pool) throw (DmException);
    // void toBeDeleted(const Pool& pool) throw (DmException);
  private:
    StackInstance* si_;
    const SecurityContext* secCtx_;

    /// The corresponding factory.
    DomeAdapterPoolsFactory* factory_;
  };

  class DomeAdapterPoolHandler: public PoolHandler {
  public:
    DomeAdapterPoolHandler(DomeAdapterPoolDriver *driver, const std::string& poolname);
    ~DomeAdapterPoolHandler();

  private:
    std::string poolname_;
    DomeAdapterPoolDriver *driver_;

  };
}

#endif
