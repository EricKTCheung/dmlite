/// @file   include/dmlite/dm_poolhandler.h
/// @brief  Pool handling API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_POOLHANDLER_H
#define	DMLITE_POOLHANDLER_H

#include "dm_auth.h"
#include "dm_exceptions.h"
#include "dm_types.h"
#include <vector>

namespace dmlite {

/// Interface for a pool handler
class PoolHandler {
public:
  /// Destructor
  virtual ~PoolHandler();
  
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
  
  /// Return true if the specified replica is available in the pool.
  virtual bool replicaAvailable(const std::string &sfn, const FileReplica& replica) throw (DmException) = 0;

  /// Get the actual location of the file replica. This is pool-dependant.
  virtual Uri getLocation(const std::string &sfn, const FileReplica& replica) throw (DmException) = 0;
  
  /// Remove a replica from the pool.
  virtual void remove(const std::string& sfn, const FileReplica& replica) throw (DmException) = 0;

  /// Get where to put a file
  virtual std::string putLocation(const std::string& sfn, Uri* uri) throw (DmException) = 0;
  
  /// Finish a put
  virtual void putDone(const std::string& sfn, const Uri& uri, const std::string& token) throw (DmException) = 0;
};

class PoolManager;

/// PoolHandler factory
class PoolHandlerFactory {
public:
  /// Destructor.
  virtual ~PoolHandlerFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Supported pool type
  virtual std::string implementedPool() throw () = 0;

  /// Instantiate a handler wrapping the passed pool.
  virtual PoolHandler* createPoolHandler(PoolManager* pm, Pool* pool) throw (DmException) = 0;
};

};

#endif	// DMLITE_POOLTYPE_H
