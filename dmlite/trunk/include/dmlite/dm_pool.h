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

// Advanced declaration
class StackInstance;

/// Interface for pool types.
class PoolManager {
public:
  /// Destructor.
  virtual ~PoolManager();

  /// String ID of the pool implementation.
  virtual std::string getImplId(void) throw() = 0;

  /// Set the StackInstance.
  /// Some plugins may need to access other stacks (i.e. the pool may need the catalog)
  /// However, at construction time not all the stacks have been populated, so this will
  /// be called once all are instantiated.
  virtual void setStackInstance(StackInstance* si) throw (DmException) = 0;
  
  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException) = 0;
  
  /// Get metadata corresponding to a pool type and name
  /// @note To be freed by the caller.
  virtual PoolMetadata* getPoolMetadata(const std::string& poolName) throw (DmException) = 0;

  /// Get the list of pools.
  /// @return A set with all the pools.
  virtual std::vector<Pool> getPools(void) throw (DmException) = 0;
  
  /// Get a specific pool.
  virtual Pool getPool(const std::string& poolname) throw (DmException) = 0;
  
  /// Get only the available pools
  /// @param write If true, it will be only the pools available for writting.
  virtual std::vector<Pool> getAvailablePools(bool write = true) throw (DmException) = 0;
};


/// Plug-ins must implement a concrete factory to be instantiated.
class PoolManagerFactory {
public:
  /// Virtual destructor
  virtual ~PoolManagerFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

protected:
  // Stack instance is allowed to instantiate PoolManager
  friend class StackInstance;  
  
  /// Children of PoolManagerFactory are allowed to instantiate too (decorator)
  static PoolManager* createPoolManager(PoolManagerFactory* factory, PluginManager* pm) throw (DmException);
  
  /// Instantiate a implementation of Pool
  virtual PoolManager* createPoolManager(PluginManager* pm) throw (DmException) = 0;

private:
};

};

#endif	// DMLITE_POOL_H
