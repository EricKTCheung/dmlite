/// @file   include/dmlite/dm_pool.h
/// @brief  Pool API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_POOL_H
#define	DMLITE_POOL_H

#include <string>
#include <vector>
#include "dm_auth.h"
#include "dm_exceptions.h"
#include "dm_types.h"

namespace dmlite {
  
/// Internal interface for handling pool metadata.
class PoolMetadata {
public:
  /// Destructor
  virtual ~PoolMetadata();

  /// Get a string field.
  virtual std::string getString(const std::string& field) throw (DmException) = 0;
  
  /// Get an integer field.
  virtual int getInt(const std::string& field) throw (DmException) = 0;
};

/// Interface for pool types.
class PoolManager {
public:
  /// Destructor.
  virtual ~PoolManager();

  /// String ID of the pool implementation.
  virtual std::string getImplId(void) throw() = 0;

  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext* ctx) = 0;
  
  /// Get metadata corresponding to a pool type and name
  /// @note To be freed by the caller.
  virtual PoolMetadata* getPoolMetadata(const Pool& pool) throw (DmException) = 0;

  /// Get the list of pools.
  /// @return A set with all the pools.
  virtual std::vector<Pool> getPools(void) throw (DmException) = 0;
  
  /// Get a specific pool.
  virtual Pool getPool(const std::string& poolname) throw (DmException) = 0;
};

class StackInstance;

/// Plug-ins must implement a concrete factory to be instantiated.
class PoolManagerFactory {
public:
  /// Virtual destructor
  virtual ~PoolManagerFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Instantiate a implementation of Pool
  /// @param si The StackInstance that is instantiating the context. It may be NULL.
  virtual PoolManager* createPoolManager(StackInstance* si) throw (DmException) = 0;

protected:
private:
};


};

#endif	// DMLITE_POOL_H
