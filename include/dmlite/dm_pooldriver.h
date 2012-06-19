/// @file   include/dmlite/dm_pooldriver.h
/// @brief  Pool handling API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_POOLDRIVER_H
#define	DMLITE_POOLDRIVER_H

#include <map>
#include <vector>
#include "dm_auth.h"
#include "dm_exceptions.h"
#include "dm_types.h"

namespace dmlite {

// Advanced declarations
class PoolManager;
class StackInstance;
  
/// Interface for a pool driver
class PoolDriver {
public:
  /// Destructor
  virtual ~PoolDriver();
  
  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException) = 0;
  
  /// Get the pool type of this pool.
  virtual std::string getPoolType(void) throw (DmException) = 0;

  /// Get the pool name of this pool.
  virtual std::string getPoolName(void) throw (DmException) = 0;

  /// Get the total space of this pool.
  virtual uint64_t getTotalSpace(void) throw (DmException) = 0;

  /// Get the free space of this pool.
  virtual uint64_t getFreeSpace(void) throw (DmException) = 0;
  
  /// Check if the pool is actually available
  virtual bool isAvailable(bool write = true) throw (DmException) = 0;

  /// Get the actual location of the file replica. This is pool-dependant.
  virtual Location getLocation(const std::string &fn, const FileReplica& replica) throw (DmException) = 0;
  
  /// Remove a replica from the pool.
  virtual void remove(const std::string& fn, const FileReplica& replica) throw (DmException) = 0;

  /// Get where to put a file
  virtual Location putLocation(const std::string& fn) throw (DmException) = 0;
  
  /// Finish a put
  virtual void putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException) = 0;
};



/// PoolDriver factory
class PoolDriverFactory {
public:
  /// Destructor.
  virtual ~PoolDriverFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Supported pool type
  virtual std::string implementedPool() throw () = 0;

  /// Instantiate a driver wrapping the passed pool.
  virtual PoolDriver* createPoolDriver(StackInstance* si, const Pool& pool) throw (DmException) = 0;
};

};

#endif	// DMLITE_POOLDRIVER_H
