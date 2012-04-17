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

/// Interface for pool types.
class PoolManager {
public:
  /// Destructor.
  virtual ~PoolManager();

  /// String ID of the pool implementation.
  virtual std::string getImplId(void) throw() = 0;

  /// Set the security credentials.
  /// @param cred The security credentials.
  virtual void setSecurityCredentials(const SecurityCredentials& cred) throw (DmException) = 0;

  /// Get the security context.
  /// @return The generated security context.
  virtual const SecurityContext& getSecurityContext(void) throw (DmException) = 0;

  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext& ctx) = 0;

  /// Get the list of pools.
  /// @return A set with all the pools.
  virtual std::vector<Pool> getPools(void) throw (DmException) = 0;
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

  /// Instantiate a implementation of Pool
  virtual PoolManager* createPoolManager() throw (DmException) = 0;

protected:
private:
};


};

#endif	// DMLITE_POOL_H
