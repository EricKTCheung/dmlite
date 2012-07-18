/// @file   include/dmlite/dm_pool.h
/// @brief  Pool API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_POOL_H
#define	DMLITEPP_POOL_H

#include <string>
#include <vector>
#include "dm_auth.h"
#include "dm_exceptions.h"
#include "../common/dm_types.h"

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
class PoolManager: public virtual BaseInterface {
public:
  enum PoolAvailability { kAny, kNone, kForRead, kForWrite, kForBoth};
  
  /// Destructor.
  virtual ~PoolManager();
  
  /// Get metadata corresponding to a pool type and name
  /// @note To be freed by the caller.
  virtual PoolMetadata* getPoolMetadata(const std::string& poolName) throw (DmException) = 0;

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
  /// @param params The extra parameters returned by dm::Catalog::put
  virtual void doneWriting(const std::string& host, const std::string& rfn,
                           const std::map<std::string, std::string>& params) throw (DmException) = 0;
};


/// Plug-ins must implement a concrete factory to be instantiated.
class PoolManagerFactory: public virtual BaseFactory {
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

#endif	// DMLITEPP_POOL_H
