/// @file   include/dmlite/cpp/poolmanager.h
/// @brief  Pool API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_POOLMANAGER_H
#define DMLITE_CPP_POOLMANAGER_H

#include <string>
#include <vector>
#include "base.h"
#include "exceptions.h"
#include "pooldriver.h"
#include "utils/extensible.h"

namespace dmlite {
  
  // Forward declarations.
  class StackInstance;
  
  /// Internal interface for handling pool metadata.
  struct Pool: public Extensible {
    std::string name;
    std::string type;
  };

  /// Interface for pool types.
  class PoolManager: public virtual BaseInterface {
   public:
    enum PoolAvailability { kAny, kNone, kForRead, kForWrite, kForBoth};

    /// Destructor.
    virtual ~PoolManager();

    /// Get the list of pools.
    /// @param availability Filter by availability.
    virtual std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException) = 0;

    /// Get a specific pool.
    virtual Pool getPool(const std::string& poolname) throw (DmException) = 0;

    /// Get a location for a logical name.
    /// @param path     The path to get.
    virtual Location whereToRead(const std::string& path) throw (DmException) = 0;

    /// Start the PUT of a file.
    /// @param path  The path of the file to create.
    /// @return      The physical location where to write.
    virtual Location whereToWrite(const std::string& path) throw (DmException) = 0;

    /// Finish a PUT
    /// @param host   The host where the replica is hosted.
    /// @param rfn    The replica file name.
    /// @param params The extra parameters as was returned by whereToWrite
    virtual void doneWriting(const std::string& host, const std::string& rfn,
                             const Extensible& params) throw (DmException) = 0;
  };

  /// Plug-ins must implement a concrete factory to be instantiated.
  class PoolManagerFactory: public virtual BaseFactory {
   public:
    /// Virtual destructor
    virtual ~PoolManagerFactory();

   protected:
    // Stack instance is allowed to instantiate PoolManager
    friend class StackInstance;  

    /// Children of PoolManagerFactory are allowed to instantiate too (decorator)
    static PoolManager* createPoolManager(PoolManagerFactory* factory,
                                          PluginManager* pm) throw (DmException);

    /// Instantiate a implementation of Pool
    virtual PoolManager* createPoolManager(PluginManager* pm) throw (DmException) = 0;
  };

};

#endif // DMLITE_CPP_POOLMANAGER_H
