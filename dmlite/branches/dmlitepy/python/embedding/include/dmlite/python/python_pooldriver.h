/// @file   python/embedding/include/dmlite/python/python_pooldriver.h
/// @brief  Python PythonPoolDriver API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#ifndef DMLITE_CPP_PYTHONPOOLDRIVER_H
#define DMLITE_CPP_PYTHONPOOLDRIVER_H

#include <boost/python.hpp>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/pooldriver.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/python/python_common.h>

namespace dmlite {
  
  // Forward declarations.
  //class Pool;
  //class StackInstance;
  

  /// Handler for a pool. Works similary to a file handler.
  class PythonPoolHandler : public virtual PoolHandler, public PythonExceptionHandler {
   public:
    PythonPoolHandler(boost::python::object module_obj);
    /// Destructor
    ~PythonPoolHandler();

    /// Get the pool type of this pool.
    std::string getPoolType(void) throw (DmException);

    /// Get the pool name of this pool.
    std::string getPoolName(void) throw (DmException);

    /// Get the total space of this pool.
    uint64_t getTotalSpace(void) throw (DmException);

    /// Get the free space of this pool.
    uint64_t getFreeSpace(void) throw (DmException);

    /// Check if the pool is actually available
    bool poolIsAvailable(bool write = true) throw (DmException);
    
    /// Check if a replica is available
    bool replicaIsAvailable(const Replica& replica) throw (DmException);

    /// Get the actual location of the file replica. This is pool-specific.
    Location whereToRead(const Replica& replica) throw (DmException);

    /// Remove a replica from the pool.
    void removeReplica(const Replica& replica) throw (DmException);

    /// Get where to put a file
    Location whereToWrite(const std::string& path) throw (DmException);
   private:
    PythonMain py;
  };

  /// Interface for a pool driver
  class PythonPoolDriver: public virtual PoolDriver, public PythonExceptionHandler {
   public:
    PythonPoolDriver(boost::python::object module_obj);
    /// Destructor
    ~PythonPoolDriver();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* ctx) throw (DmException);
    /// String ID of the implementation.
    std::string getImplId(void) const throw();

    /// Create a handler.
    PythonPoolHandler* createPoolHandler(const std::string& poolName) throw (DmException);
    
    /// Called just before adding the pool to the database.
    /// To be used by a plugin, in case it needs to do some previous preparations.
    /// (i.e. legacy filesystem will actually create the pool here)
    void toBeCreated(const Pool& pool) throw (DmException);
    
    /// Called just after a pool is added to the database.
    void justCreated(const Pool& pool) throw (DmException);
    
    /// Called when updating a pool.
    void update(const Pool& pool) throw (DmException);
    
    /// Called just before a pool of this type is removed.
    /// @note The driver may remove the pool itself (i.e. filesystem)
    void toBeDeleted(const Pool& pool) throw (DmException);
   private:
    PythonMain py;
  };

  /// PythonPoolDriver factory
  class PythonPoolDriverFactory: public virtual PoolDriverFactory, public PythonExceptionHandler {
   public:
    PythonPoolDriverFactory(std::string pymodule);
    /// Destructor.
    ~PythonPoolDriverFactory();

    /// Supported pool type
    std::string implementedPool() throw ();

    void configure(const std::string& key, const std::string& value) throw (DmException);

   protected:
    friend class StackInstance;

    /// Instantiate the implemented pool driver.
    PythonPoolDriver* createPoolDriver(void) throw (DmException);
   private:
    PythonMain py;
  };

};

#endif	// DMLITE_CPP_PYTHONPOOLDRIVER_H
