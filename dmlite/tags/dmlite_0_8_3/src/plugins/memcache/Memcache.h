/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_H
#define MEMCACHE_H

#include <libmemcached/memcached.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <dmlite/cpp/utils/urls.h>
#include <set>

#include "MemcacheFunctionCounter.h"

#include "utils/logger.h"

#define MEMCACHED_DEFAULT_PORT 11211

#define DEFAULT_MEMCACHED_EXPIRATION 60

#define MAX_SERVERNAME_LENGTH 4096

namespace dmlite {

  extern Logger::bitmask memcachelogmask;
  extern Logger::component memcachelogname;

  /// Used to keep the Key Prefixes
  enum {
    PRE_STAT = 0,
    PRE_REPL,
    PRE_REPL_LIST,
    PRE_COMMENT,
    PRE_DIR,
    PRE_DIR_LIST,
    PRE_POOL,
    PRE_POOL_LIST,
    PRE_LOCATION
  };

  /// Used internally to define Key Prefixes.
  /// Must match with PRE_* constants!
  static const char* const key_prefix[] = {
    "STAT",
    "REPL",
    "RPLI",
    "CMNT",
    "DIR",
    "DRLI",
    "POOL",
    "POLI",
    "LOCA"
  };

  class MemcacheConnectionFactory: public PoolElementFactory<memcached_st*> {
    public:
      MemcacheConnectionFactory(std::set<std::string> hosts,
          bool useBinaryProtocol,
          std::string dist);
      virtual ~MemcacheConnectionFactory();

      memcached_st* create() throw ();
      void   destroy(memcached_st*) throw ();
      bool   isValid(memcached_st*) throw ();

      // Attributes
      std::set<std::string>  hosts_;

      /// The memcached protocol (binary/ascii) to use.
      bool useBinaryProtocol_;

      /// The hash distribution algorithm
      std::string dist_;
    protected:
    private:
  };

  /// Concrete factory for the Memcache plugin.
  class MemcacheFactory: public CatalogFactory, public PoolManagerFactory {
    public:
      /// Constructor
      MemcacheFactory(CatalogFactory* catalogFactory,
                      PoolManagerFactory* poolManagerFactory) throw (DmException);
      /// Destructor
      virtual ~MemcacheFactory();

      void configure(const std::string& key, const std::string& value) throw (DmException);
      virtual Catalog* createCatalog(PluginManager*) throw (DmException);
      virtual PoolManager* createPoolManager(PluginManager*) throw (DmException);
    protected:
      /// Decorated
      CatalogFactory* nestedCatalogFactory_;

      /// The decorated PoolManager factory.
      PoolManagerFactory* nestedPoolManagerFactory_;

      /// Connection factory.
      MemcacheConnectionFactory connectionFactory_;

      /// Connection pool.
      PoolContainer<memcached_st*> connectionPool_;

      /// Stats counter for the overloaded functions.
      MemcacheFunctionCounter* funcCounter_;

      /// Do the counting or not.
      bool doFuncCount_;

      /// Configure the logging frequency.
      int funcCounterLogFreq_;

      /// The recursion limit following symbolic links.
      unsigned int symLinkLimit_;

      /// The expiration limit for cached data on memcached in seconds.
      unsigned int memcachedExpirationLimit_;

      /// Enable POSIX-like behaviour.
      bool memcachedPOSIX_;
    private:
  };

/// Little help here to avoid redundancy
#define DELEGATE(method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
this->decorated_->method(__VA_ARGS__);\
} \

/// Little help here to avoid redundancy
#define DELEGATE_RETURN(method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
return this->decorated_->method(__VA_ARGS__);\
} \

/// Little help here to avoid redundancy
#define DELEGATE_ASSIGN(var, method, ...) \
{ \
  if (this->decorated_ == 0x00)\
throw DmException(DMLITE_SYSERR(ENOSYS), "There is no plugin in the stack that implements "#method);\
var = this->decorated_->method(__VA_ARGS__);\
} \


}

#endif // MEMCACHE_H
