/// @file    plugins/memcache/MemcachePoolManager.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_POOL_H
#define MEMCACHE_POOL_H

#include <dmlite/cpp/poolmanager.h>

#include "Memcache.h"
#include "MemcacheCommon.h"

#include "utils/logger.h"

namespace dmlite {

  extern Logger::bitmask memcachelogmask;
  extern Logger::component memcachelogname;

  class MemcachePoolManager: public PoolManager, protected MemcacheCommon {
    public:
      MemcachePoolManager(PoolContainer<memcached_st*>& connPool,
          PoolManager* decorates,
          MemcacheFunctionCounter* funcCounter,
          bool doFuncCount,
          time_t memcachedExpirationLimit) throw (DmException);
      ~MemcachePoolManager();

      std::string getImplId(void) const throw ();

      void setStackInstance(StackInstance* si) throw (DmException);
      void setSecurityContext(const SecurityContext*) throw (DmException);

      std::vector<Pool> getPools(PoolAvailability availability) throw (DmException);
      Pool getPool(const std::string& poolname) throw (DmException);

      void newPool(const Pool& pool) throw (DmException);
      void updatePool(const Pool& pool) throw (DmException);
      void deletePool(const Pool& pool) throw (DmException);

      Location whereToRead (const std::string& path) throw (DmException);
      Location whereToRead (ino_t inode)             throw (DmException);
      Location whereToWrite(const std::string& path) throw (DmException);

      void cancelWrite(const Location& loc) throw (DmException);
      
      void getDirSpaces(const std::string& path, int64_t &totalfree, int64_t &used) throw (DmException);

    protected:

    private:
      PoolManager* decorated_;

      /// Stack instance
      StackInstance* si_;

      /// Increment the function counter.
      /// Check if counting is enabled and then increment.
      /// The function name is defined by an enum.
      /// @param funcName   The name of the function.
      inline void incrementFunctionCounter(const int funcName);
      

  };

  static const std::string available_pool_key_string[] = {
    std::string("POOL_ANY"),
    std::string("POOL_NONE"),
    std::string("POOL_READ"),
    std::string("POOL_WRITE"),
    std::string("POOL_BOTH")
  };

};

#endif // MEMCACHE_POOL_H
