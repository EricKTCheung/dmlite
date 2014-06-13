/// @file    plugins/memcache/Memcache.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_H
#define MEMCACHE_H

#include <libmemcached/memcached.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <dmlite/cpp/utils/urls.h>

#include "MemcacheFunctionCounter.h"

#define MEMCACHED_DEFAULT_PORT 11211

#define DEFAULT_MEMCACHED_EXPIRATION 60

#define MAX_SERVERNAME_LENGTH 4096

namespace dmlite {

  class MemcacheConnectionFactory: public PoolElementFactory<memcached_st*> {
    public:
      MemcacheConnectionFactory(std::vector<std::string> hosts,
          bool useBinaryProtocol,
          std::string dist);
      ~MemcacheConnectionFactory();

      memcached_st* create() throw ();
      void   destroy(memcached_st*) throw ();
      bool   isValid(memcached_st*) throw ();

      // Attributes
      std::vector<std::string>  hosts_;

      /// The memcached protocol (binary/ascii) to use.
      bool useBinaryProtocol_;

      /// The hash distribution algorithm
      std::string dist_;
    protected:
    private:
  };

  /// Concrete factory for the Memcache plugin.
  class MemcacheFactory: public CatalogFactory {
    public:
      /// Constructor
      MemcacheFactory(CatalogFactory* catalogFactory) throw (DmException);
      /// Destructor
      ~MemcacheFactory() throw (DmException);

      void configure(const std::string& key, const std::string& value) throw (DmException);
      Catalog* createCatalog(PluginManager*) throw (DmException);
    protected:
      /// Decorated
      CatalogFactory* nestedFactory_;

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

}

#endif // MEMCACHE_H
