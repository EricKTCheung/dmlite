/// @file    plugins/memcache/MemcacheCatalog.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_CATALOG_H
#define MEMCACHE_CATALOG_H

#include <vector>
#include <sstream>
#include <syslog.h>

#include <libmemcached/memcached.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <dmlite/cpp/utils/urls.h>

#include "Memcache.h"
#include "MemcacheCatalog.pb.h"

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

  class MemcacheDir: public Directory {
    public:
      virtual ~MemcacheDir() {};
      Directory *decorated_dirp;
      ExtendedStat dir;
      struct dirent ds;

      std::string basepath;
      SerialKeyList pb_keys;
      int pb_keys_idx;
  };

  class MemcacheCatalog: public Catalog {
    public:
      /// Constructor
      /// @param connPool The memcached connection pool.
      /// @param decorates The underlying decorated catalog.
      MemcacheCatalog(PoolContainer<memcached_st*>& connPool,
          Catalog* decorates,
          MemcacheBloomFilter* filter,
          bool doFilter,
          MemcacheFunctionCounter* funcCounter,
          bool doFuncCount,
          unsigned int symLinkLimit,
          time_t memcachedExpirationLimit,
          bool memcachedPOSIX)
        throw (DmException);

      /// Destructor
      ~MemcacheCatalog() throw (DmException);

      std::string getImplId(void) const throw ();

      void setStackInstance(StackInstance* si) throw (DmException);
      void setSecurityContext(const SecurityContext* ctx) throw (DmException);

      /// just delegation
      void changeDir(const std::string& path) throw (DmException);

      /// just delegation
      std::string getWorkingDir(void) throw (DmException);

      ExtendedStat extendedStat(const std::string& path,
          bool followSym = true) throw (DmException);

      /// just delegation
      ExtendedStat extendedStatByRFN(const std::string& rfn) throw (DmException);

      bool access(const std::string& path, int mode) throw (DmException);

      /// just delegation
      bool accessReplica(const std::string& replica, int mode) throw (DmException);

      /// just delegation
      void addReplica(const Replica& replica) throw (DmException);

      /// just delegation
      void deleteReplica(const Replica& replica) throw (DmException);

      std::vector<Replica> getReplicas(const std::string& path) throw (DmException);

      /// just delegation
      void symlink(const std::string& path,
          const std::string& symlink) throw (DmException);

      /// just delegation
      std::string readLink(const std::string& path) throw (DmException);

      void unlink(const std::string& path) throw (DmException);

      /// just delegation
      void create(const std::string& path,
          mode_t mode) throw (DmException);

      /// just delegation
      mode_t umask(mode_t mask) throw ();

      void setMode(const std::string& path,
          mode_t mode) throw (DmException);

      void setOwner(const std::string& path, uid_t newUid, gid_t newGid,
          bool followSymLink = true) throw (DmException);

      void setSize(const std::string& path,
          size_t newSize) throw (DmException);

      void setChecksum(const std::string& path,
          const std::string& csumtype,
          const std::string& csumvalue) throw (DmException);

      void setAcl(const std::string& path,
          const Acl& acl) throw (DmException);

      void utime(const std::string& path,
          const struct utimbuf* buf) throw (DmException);

      std::string getComment(const std::string& path) throw (DmException);

      void setComment(const std::string& path,
          const std::string& comment) throw (DmException);

      void setGuid(const std::string& path,
          const std::string &guid) throw (DmException);

      void updateExtendedAttributes(const std::string& path,
          const Extensible& attr) throw (DmException);

      Directory* openDir(const std::string& path) throw (DmException);

      void closeDir(Directory* dir) throw (DmException);

      struct dirent* readDir(Directory* dir) throw (DmException);

      ExtendedStat* readDirx(Directory* dir) throw (DmException);

      /// just delegation
      void makeDir(const std::string& path,
          mode_t mode) throw (DmException);

      void rename(const std::string& oldPath,
          const std::string& newPath) throw (DmException);

      void removeDir(const std::string& path) throw (DmException);

      Replica getReplicaByRFN(const std::string& rfn) throw (DmException);

      void updateReplica(const Replica& replica) throw (DmException);

  protected:
  /// The decorated Catalog.
  Catalog* decorated_;
  char*    decoratedId_;

  /// Security context
  const SecurityContext* secCtx_;

  /// User secondary groups.
  std::vector<GroupInfo> groups_;

  /// Stack instance
  StackInstance* si_;

  /// The current directory
  std::string cwd_;

  /// The Memcached connection.
  //memcached_st* conn_;
  PoolGrabber<memcached_st*> conn_;

  /// The Memcached connection. Set NOREPLY in the constructor.
  memcached_st* connNoReply_;

  /// Bloom Filter to track elements in memcached..
  MemcacheBloomFilter* bloomFilter_;

  /// Use the bloom filter or not.
  bool doFilter_;

  /// Stats counter for the overloaded functions.
  MemcacheFunctionCounter *funcCounter_;

  /// Do the counting or not.
  bool doFuncCount_;

  /// The random seed for rand_r. Random because uninitialized :P
  unsigned int randomSeed_;

  /// Symlink limit
  unsigned int symLinkLimit_;

  /// The expiration limit for cached data on memcached in seconds.
  time_t memcachedExpirationLimit_;

  /// Enable POSIX-like behaviour.
  bool memcachedPOSIX_;

  private:
  /// Get an ExtendedStat POSIX-like.
  /// The same logic as in BuiltInCatalog, but using cached entries.
  /// @param path         The path, absolute or relative
  /// @param followSym    Flag whether to follow symlinks
  /// @return             ExtendedStat
  ExtendedStat extendedStatPOSIX(const std::string& path, bool followSym) throw (DmException);

  /// Get an ExtendedStat object store-like.
  /// Fetches the ExtendedStat without checking permissions
  /// on the parent entities. POSIX semantics might still
  /// apply if the decorated plugin implements them.
  /// @param path         The path, absolute or relative
  /// @return             ExtendedStat
  ExtendedStat extendedStatNoPOSIX(const std::string& path, bool followSym) throw (DmException);

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

  /// Increment the function counter.
  /// Check if counting is enabled and then increment.
  /// The function name is defined by an enum.
  /// @param funcName   The name of the function.
  inline void incrementFunctionCounter(const int funcName);

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

  /// Delegate readDirx.
  /// Delegate the readDirx function and add the filename to
  /// the cache. First it is stored in the dirp, it will later
  /// be uploaded to memcached.
  /// @param dirp        The directory pointer.
  /// @return            The file stats.
  ExtendedStat* delegateReadDirxAndAddEntryToCache(MemcacheDir *dirp) throw (DmException);

  /// Get a file entry from the cache.
  /// Take a filename from the dirp->pb_keys and retrieve its
  /// ExtendedStat. Try memcached first, get it from the delegated plugin
  /// otherwise and store it in the cache. Uses the dirp->pb_key_idx to
  /// find out which one to take.
  /// @param dirp        The directory pointer.
  /// @return            The file stats.
  ExtendedStat* getDirEntryFromCache(MemcacheDir *dirp) throw (DmException);

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

  /// Get the parent of a directory.
  /// @param The path to split.
  /// @return The parent path as string.
  //std::string getParent(const std::string& path) throw (DmException);
};
}

#endif // MEMCACHED_CATALOG_H

