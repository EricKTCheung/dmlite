/// @file    plugins/memcache/MemcacheCatalog.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_CATALOG_H
#define MEMCACHE_CATALOG_H

#include <vector>

#include <dmlite/cpp/catalog.h>

#include "Memcache.h"
#include "MemcacheCommon.h"

#include "utils/logger.h"

namespace dmlite {

  extern Logger::bitmask memcachelogmask;
  extern Logger::component memcachelogname;

  class MemcacheDir: public Directory {
    public:
      virtual ~MemcacheDir() {};
      Directory *decorated_dirp;
      ExtendedStat dir;
      struct dirent ds;

      std::string basepath;
      SerialKeyList pb_keys;
      int pb_keys_idx;
      bool has_called_opendir;
  };

  class MemcacheCatalog: public Catalog, protected MemcacheCommon {
    public:
      /// Constructor
      /// @param connPool The memcached connection pool.
      /// @param decorates The underlying decorated catalog.
      MemcacheCatalog(PoolContainer<memcached_st*>& connPool,
          Catalog* decorates,
          MemcacheFunctionCounter* funcCounter,
          bool doFuncCount,
          unsigned int symLinkLimit,
          time_t memcachedExpirationLimit,
          bool memcachedPOSIX)
        throw (DmException);

      /// Destructor
      virtual ~MemcacheCatalog();

      std::string getImplId(void) const throw ();

      void setStackInstance(StackInstance* si) throw (DmException);
      void setSecurityContext(const SecurityContext* ctx) throw (DmException);

      /// just delegation
      void changeDir(const std::string& path) throw (DmException);

      /// just delegation
      std::string getWorkingDir(void) throw (DmException);

      DmStatus extendedStat(ExtendedStat &xstat, const std::string& path,
          bool followSym = true) throw (DmException);

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

      void getChecksum(const std::string& path,
                       const std::string& csumtype,
                       std::string& csumvalue,
                       const std::string &pfn,
                       const bool forcerecalc = false,
                       const int waitsecs = 0) throw (DmException);

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
  Catalog* decorated_;

  /// Security context
  const SecurityContext* secCtx_;

  /// User secondary groups.
  std::vector<GroupInfo> groups_;

  /// Stack instance
  StackInstance* si_;

  /// Symlink limit
  unsigned int symLinkLimit_;

  /// Enable POSIX-like behaviour.
  bool memcachedPOSIX_;

  private:
  /// Get an ExtendedStat POSIX-like.
  /// It's an efficient implementation for the case that no relative
  /// movement ('.' or '..') is used nor symlinks. For these cases
  /// it falls back to extendedStatPOSIX().
  /// @param path         The path, absolute or relative
  /// @param followSym    Flag whether to follow symlinks
  /// @return             ExtendedStat
  DmStatus extendedStatSimplePOSIX(ExtendedStat &xstat, const std::string& path, bool followSym) throw (DmException);

  /// Get an ExtendedStat POSIX-like.
  /// The same logic as in BuiltInCatalog, but using cached entries.
  /// @param path         The path, absolute or relative
  /// @param followSym    Flag whether to follow symlinks
  /// @return             ExtendedStat
  DmStatus extendedStatPOSIX(ExtendedStat &xstat, const std::string& path, bool followSym) throw (DmException);

  /// Get an ExtendedStat object store-like.
  /// Fetches the ExtendedStat without checking permissions
  /// on the parent entities. POSIX semantics might still
  /// apply if the decorated plugin implements them.
  /// @param path         The path, absolute or relative
  /// @return             The status
  DmStatus extendedStatNoPOSIX(ExtendedStat &xstat, const std::string& path, bool followSym) throw (DmException);

  /// Get an ExtendedStat object without permission checking.
  /// Fetches the ExtendedStat without checking permissions.
  /// It is basically a wrapper around the memcache get function.
  /// Complete POSIX semantics apply if the decorated plugin
  /// implements them.
  /// @param absPath      The path, must be absolute
  /// @return             ExtendedStat
  DmStatus extendedStatNoCheck(ExtendedStat &xstat, const std::string& absPath, bool followSym) throw (DmException);

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

  /// Get the full path of a file fiven the RFN
  /// @param The RFN
  /// @return The path as string.
  std::string getFullPathByRFN(const std::string& rfn) throw (DmException);

  /// Increment the function counter.
  /// Check if counting is enabled and then increment.
  /// The function name is defined by an enum.
  /// @param funcName   The name of the function.
  inline void incrementFunctionCounter(const int funcName);
};
}

#endif // MEMCACHED_CATALOG_H
