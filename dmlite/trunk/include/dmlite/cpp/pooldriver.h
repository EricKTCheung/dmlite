/// @file   include/dmlite/cpp/pooldriver.h
/// @brief  Pool handling API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_POOLDRIVER_H
#define DMLITE_CPP_POOLDRIVER_H

#include <map>
#include <vector>
#include "base.h"
#include "exceptions.h"
#include "inode.h"
#include "utils/extensible.h"

namespace dmlite {
  
  // Forward declarations.
  class StackInstance;
  
  /// Represents a chunk of a file.
  struct Chunk: public Extensible {
    std::string host;
    std::string path;
    off_t       offset;
    size_t      size;
  };
  
  /// Represent the complete location of a file.
  typedef std::vector<Chunk> Location;

  /// Handler for a pool. Works similary to a file handler.
  class PoolHandler {
   public:
    /// Destructor
    virtual ~PoolHandler();

    /// Get the pool type of this pool.
    virtual std::string getPoolType(void) throw (DmException) = 0;

    /// Get the pool name of this pool.
    virtual std::string getPoolName(void) throw (DmException) = 0;

    /// Get the total space of this pool.
    virtual uint64_t getTotalSpace(void) throw (DmException) = 0;

    /// Get the free space of this pool.
    virtual uint64_t getFreeSpace(void) throw (DmException) = 0;

    /// Check if the pool is actually available
    virtual bool poolIsAvailable(bool write = true) throw (DmException) = 0;
    
    /// Check if a replica is available
    virtual bool replicaIsAvailable(const Replica& replica) throw (DmException) = 0;

    /// Get the actual location of the file replica. This is pool-specific.
    virtual Location whereToRead(const Replica& replica) throw (DmException) = 0;

    /// Remove a replica from the pool.
    virtual void removeReplica(const Replica& replica) throw (DmException) = 0;

    /// Get where to put a file
    virtual Location whereToWrite(const std::string& path) throw (DmException) = 0;
  };

  /// Interface for a pool driver
  class PoolDriver: public virtual BaseInterface {
   public:
    /// Destructor
    virtual ~PoolDriver();

    /// Create a handler.
    virtual PoolHandler* createPoolHandler(const std::string& poolName) throw (DmException) = 0;
  };

  /// PoolDriver factory
  class PoolDriverFactory: public virtual BaseFactory {
   public:
    /// Destructor.
    virtual ~PoolDriverFactory();

    /// Supported pool type
    virtual std::string implementedPool() throw () = 0;

   protected:
    friend class StackInstance;

    /// Instantiate the implemented pool driver.
    virtual PoolDriver* createPoolDriver(void) throw (DmException) = 0;
  };

};

#endif	// DMLITE_CPP_POOLDRIVER_H
