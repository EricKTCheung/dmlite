/// @file    plugins/memcache/MemcacheCommon.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_COMMON_H
#define MEMCACHE_COMMON_H

#include <sstream>
#include <libmemcached/memcached.h>

#include <dmlite/cpp/exceptions.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <dmlite/cpp/utils/urls.h>

#include "MemcacheFunctionCounter.h"
#include "Memcache.pb.h"

namespace dmlite {

  class MemcacheException: public DmException {
    public:
      MemcacheException(memcached_return rc, memcached_st *conn)
      {
        this->errorCode_ = (int) rc;
        this->errorMsg_ = std::string(memcached_strerror(conn, rc));
      };
    protected:
    private:
  };

  class MemcacheCommon {
    public:
      MemcacheCommon(PoolContainer<memcached_st*>& connPool,
                     MemcacheFunctionCounter* funcCounter,
                     bool doFuncCount,
                     time_t memcachedExpirationLimit);
      ~MemcacheCommon() {};

    protected:
      /// The decorated interface as string.
      char*    decoratedId_;

      /// The Memcached connection.
      //memcached_st* conn_;
      PoolGrabber<memcached_st*> conn_;

      /// The Memcached connection. Set NOREPLY in the constructor.
      memcached_st* connNoReply_;

      /// Stats counter for the overloaded functions.
      MemcacheFunctionCounter *funcCounter_;

      /// Do the counting or not.
      bool doFuncCount_;

      /// The random seed for rand_r. Random because uninitialized :P
      unsigned int randomSeed_;

      /// The expiration limit for cached data on memcached in seconds.
      time_t memcachedExpirationLimit_;

      /// The current directory
      std::string cwd_;

      /// Serialize an ExtendedStat object into a string.
      /// @param var          The object to serialize.
      /// @param serialString The string to serialize into.
      void serializeExtendedStat(const ExtendedStat& var, std::string& serialString);

      /// Create an object from a serialized string.
      /// @param serial_str   The serialized object as string.
      /// @param var          The deserialized object.
      void deserializeExtendedStat(const std::string& serial_str, ExtendedStat& var);

      /// Serialize a Replica into a string
      /// @param vecRepl      The replica.
      /// @return             The serialized object as string.
      std::string serializeReplica(const Replica& replica);

      /// Create an object from a serialized string.
      /// @param serial_str   The serialized object as string.
      /// @param var          The deserialized object.
      void deserializeReplica(const std::string& serial_str, Replica& replica);

      /// Serialize a std::vector<Replica> into a string
      /// @param vecRepl      The vector of replicas.
      /// @return             The serialized object as string.
      std::string serializeReplicaList(const std::vector<Replica>& vecRepl);

      /// Create an object from a serialized string.
      /// @param serial_str   The serialized object as string.
      /// @param var          The deserialized object.
      void deserializeReplicaList(const std::string& serial_str, std::vector<Replica>& vecRepl);

      /// Serialize a Comment object into a string.
      /// @param var The object to serialize.
      /// @return The serialized object as string.
      std::string serializeComment(const std::string& var);

      /// Create an object from a serialized string.
      /// @param serial_str The serialized object as string.
      /// @param var The deserialized object.
      void deserializeComment(std::string& serial_str, std::string& var);

      /// Serialize a list of keys (strings) into a string.
      /// @param keyList      The list of keys.
      /// @return The serialized object.
      std::string serializeDirList(std::list<std::string>& keyList);

      /// Deserialize a list of keys.
      /// @param serialList The serialized List as string.
      /// @return           The List.
      std::list<std::string> deserializeDirList(std::string& serialList);

      /// Create absolute path.
      /// Create an absolute path based on the current working dir
      /// and a relative path.
      /// This function takes the cwd from the decorated plugin.
      /// @param path       The (relative) path string.
      /// @return           The absolute path string.
      std::string getAbsolutePath(const std::string& path) throw (DmException);

      /// Get the base path.
      /// Behaves similar to dirname, with the exception that it
      /// prints trailing slashes of directories. This matches the way
      /// keys for the cache are generated.
      /// Requires an absolute path, as a path without any '/' simply
      /// returns the whole path.
      /// @param path       The absolute path string.
      /// @return           The base path string.
      std::string getBasePath(const std::string& path) throw (std::bad_alloc, std::length_error);

      /// Concat two paths.
      /// The first path can be relative or absolute, the second must be relative.
      /// If the first one is relative, the whole path is relative.
      /// The function only helps you by putting a slash between the path fragments,
      /// if necessary.
      /// @param basepath   The first part of the path.
      /// @param relpath    The second part of the path.
      /// @return           The concatenated path.
      std::string concatPath(const std::string& basepath, const std::string& relpath) throw ();

      /// Converts a key prefix and string into a key for memcached.
      /// @param preKey key prefix string.
      /// @param path   key string.
      /// @return       The key as string.
      const std::string keyFromString(const char* preKey, const std::string& path);

      /// Converts a key prefix and a URI string into a key for memcached.
      /// @param preKey key prefix string.
      /// @param uri    URI string.
      /// @return       The key as string.
      const std::string keyFromURI(const char* preKey, const std::string& uri);

      /// Return the value to a given key from memcached.
      /// @param key   The memcached key as string.
      /// @return      The value from memcached.
      const std::string getValFromMemcachedKey(const std::string& key) throw (MemcacheException);

      /// Return the value to a given key from memcached.
      /// @param key   The memcached key as string.
      /// @return      The value from memcached.
      const std::string safeGetValFromMemcachedKey(const std::string& key) throw ();

      /// Store a key,value pair on memcached.
      /// @param key      The memcached key as string.
      /// @param value    The memcached value serialized as string.
      /// @param noreply  Flag whether to wait for the return or not.
      void setMemcachedFromKeyValue(const std::string& key,
          const std::string& value, const bool noreply = false) throw (MemcacheException);

      /// Store a key,value pair on memcached.
      /// @param key   The memcached key as string.
      /// @param value The memcached value serialized as string.
      void safeSetMemcachedFromKeyValue(const std::string& key,
          const std::string& value) throw ();

      /// Store a key,value pair on memcached.
      /// Do not overwrite the key, but fail.
      /// @param key   The memcached key as string.
      /// @param value The memcached value serialized as string.
      void addMemcachedFromKeyValue(const std::string& key,
          const std::string& value) throw (MemcacheException);

      /// Store a key,value pair on memcached.
      /// Do not overwrite the key, but fail.
      /// @param key   The memcached key as string.
      /// @param value The memcached value serialized as string.
      void safeAddMemcachedFromKeyValue(const std::string& key,
          const std::string& value) throw ();

      /// Delete a key,value pair on memcached.
      /// @param key      The memcached key as string.
      /// @param noreply  Flag whether to wait for the return or not.
      void delMemcachedFromKey(const std::string& key,
          const bool noreply = false) throw (MemcacheException);

      /// Delete a key,value pair on memcached.
      /// @param key   The memcached key as string.
      void safeDelMemcachedFromKey(const std::string& key) throw ();

      /// Serialize a std::vector<Pool> into a string
      /// @param vecPool      The vector of pools.
      /// @return             The serialized object as string.
      std::string serializePoolList(const std::vector<Pool>& vecPool);

      /// Create an object from a serialized string.
      /// @param serial_str   The serialized object as string.
      /// @param var          The deserialized object.
      void deserializePoolList(const std::string& serial_str, std::vector<Pool>& vecPool);

      /// Serialize a Pool into a string
      /// @param vecPool      The pool.
      /// @return             The serialized object as string.
      std::string serializePool(const Pool& pool);

      /// Create an object from a serialized string.
      /// @param serial_str   The serialized object as string.
      /// @param var          The deserialized object.
      void deserializePool(const std::string& serial_str, Pool& pool);
  };
};

#endif
