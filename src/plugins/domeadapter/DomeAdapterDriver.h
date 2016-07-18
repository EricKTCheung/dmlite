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

  class DomeAdapterFactory;
  class DomeAdapterPoolHandler;

  class DomeAdapterPoolDriver : public PoolDriver {
  public:
    DomeAdapterPoolDriver(DomeAdapterFactory* factory);
    ~DomeAdapterPoolDriver();

    std::string getImplId() const throw();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);

    void toBeCreated(const Pool& pool) throw (DmException);
    void justCreated(const Pool& pool) throw (DmException);
    void update(const Pool& pool) throw (DmException);
    void toBeDeleted(const Pool& pool) throw (DmException);
  private:
    StackInstance* si_;
    const SecurityContext *secCtx_;
    std::string userId_;

    /// The corresponding factory.
    DomeAdapterFactory* factory_;

    friend class DomeAdapterPoolHandler;
  };

  class DomeAdapterPoolHandler: public PoolHandler {
  public:
    DomeAdapterPoolHandler(DomeAdapterPoolDriver *driver, const std::string& poolname);
    ~DomeAdapterPoolHandler();

    std::string getPoolType    (void) throw (DmException);
    std::string getPoolName    (void) throw (DmException);
    uint64_t    getTotalSpace  (void) throw (DmException);
    uint64_t    getFreeSpace   (void) throw (DmException);
    bool        poolIsAvailable(bool) throw (DmException);

    bool     replicaIsAvailable(const Replica& replica) throw (DmException);
    Location whereToRead       (const Replica& replica) throw (DmException);

    void removeReplica(const Replica&) throw (DmException);
    Location whereToWrite(const std::string&) throw (DmException);
    void cancelWrite(const Location& loc) throw (DmException);

  private:
    enum DomeFsStatus {
      FsStaticActive = 0,
      FsStaticDisabled,
      FsStaticReadOnly
    };

    std::string poolname_;
    DomeAdapterPoolDriver *driver_;

    uint64_t getPoolField(const std::string &field, uint64_t def) throw (DmException);
  };
}

#endif
