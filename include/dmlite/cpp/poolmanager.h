/// @file   include/dmlite/cpp/poolmanager.h
/// @brief  Pool API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_POOLMANAGER_H
#define DMLITE_CPP_POOLMANAGER_H

#include "../common/config.h"
#include "base.h"
#include "exceptions.h"
#include "pooldriver.h"
#include "utils/extensible.h"

#include <string>
#include <vector>

namespace dmlite {
  
  // Forward declarations.
  class StackInstance;
  
  /// Internal interface for handling pool metadata.
  struct Pool: public Extensible {
    std::string name;
    std::string type;
    
    bool operator == (const Pool&) const;
    bool operator != (const Pool&) const;
    bool operator <  (const Pool&) const;
    bool operator >  (const Pool&) const;
  };

  /// Interface for pool types.
  class PoolManager: public virtual BaseInterface {
   public:
    enum PoolAvailability { kAny, kNone, kForRead, kForWrite, kForBoth};

    /// Destructor.
    virtual ~PoolManager();

    /// Get the list of pools.
    /// @param availability Filter by availability.
    virtual std::vector<Pool> getPools(PoolAvailability availability = kAny) throw (DmException);

    /// Get a specific pool.
    virtual Pool getPool(const std::string& poolname) throw (DmException);
    
    /// Create a new pool.
    virtual void newPool(const Pool& pool) throw (DmException);
    
    /// Update pool metadata.
    virtual void updatePool(const Pool& pool) throw (DmException);
    
    /// Remove a pool.
    virtual void deletePool(const Pool& pool) throw (DmException);

    /// Get a location for a logical name.
    /// @param path     The path to get.
    virtual Location whereToRead(const std::string& path) throw (DmException);
    
    /// Get a location for an inode
    /// @param inode The file inode.
    virtual Location whereToRead(ino_t inode) throw (DmException);

    /// Start the PUT of a file.
    /// @param path  The path of the file to create.
    /// @return      The physical location where to write.
    virtual Location whereToWrite(const std::string& path) throw (DmException);

    /// Cancel a write.
    /// @param path The logical file name.
    /// @param loc  As returned by whereToWrite
    virtual void cancelWrite(const Location& loc) throw (DmException);
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
    virtual PoolManager* createPoolManager(PluginManager* pm) throw (DmException);
  };

};

#endif // DMLITE_CPP_POOLMANAGER_H
