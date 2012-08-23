/// @file     core/builtin/Catalog.h
/// @brief    Implementation of a Catalog using other plugins, as INode.
/// @detailed Intended to ease the development of database backends.
/// @author   Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef BUILTIN_CATALOG_H
#define	BUILTIN_CATALOG_H

#include <dmlite/cpp/dm_catalog.h>
#include <dmlite/cpp/dm_inode.h>
#include <dmlite/cpp/dm_pool.h>
#include <dmlite/cpp/dm_pooldriver.h>

namespace dmlite {
  
struct BuiltInDir {
  IDirectory*  idir;
  ExtendedStat dir;
};
  
class BuiltInCatalog: public Catalog {
public:
  BuiltInCatalog(bool updateATime, unsigned symLinkLimit) throw (DmException);
  ~BuiltInCatalog();

  std::string getImplId(void) throw();
  
  void setStackInstance(StackInstance* si) throw (DmException);
  
  void setSecurityContext(const SecurityContext*) throw (DmException);  
  
  void        changeDir     (const std::string&) throw (DmException);
  std::string getWorkingDir (void) throw (DmException);
  ino_t       getWorkingDirI(void) throw (DmException);
  
  ExtendedStat extendedStat(const std::string& path, bool followSym = true) throw (DmException);

  void addReplica   (const std::string& guid, int64_t id, const std::string& server,
                     const std::string& sfn, char status, char fileType,
                     const std::string& poolName, const std::string& fileSystem) throw (DmException);
  void deleteReplica(const std::string& guid, int64_t id,
                     const std::string& sfn) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string& path) throw (DmException);
  Location get(const std::string& path) throw (DmException);
  
  Location put(const std::string& path) throw (DmException);
  Location put(const std::string& path,
               const std::string& guid) throw (DmException);
  void     putDone(const std::string& host, const std::string& rfn,
                   const std::map<std::string, std::string>& params) throw (DmException);

  void symlink(const std::string& oldpath, const std::string& newpath) throw (DmException);

  void unlink(const std::string& path) throw (DmException);

  void create(const std::string& path, mode_t mode) throw (DmException);
  
  void makeDir  (const std::string& path, mode_t mode) throw (DmException);
  void removeDir(const std::string& path) throw (DmException);

  void rename(const std::string& oldPath, const std::string& newPath) throw (DmException);

  mode_t umask(mode_t mask) throw ();

  void changeMode     (const std::string& path, mode_t mode) throw (DmException);
  void changeOwner    (const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink = true) throw (DmException);
  
  void changeSize    (const std::string& path, size_t newSize) throw (DmException);
  void changeChecksum(const std::string& path, const std::string& csumtype, const std::string& csumvalue) throw (DmException);
  
  void setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException);

  void utime(const std::string& path, const struct utimbuf* buf) throw (DmException);

  std::string getComment(const std::string& path) throw (DmException);
  void        setComment(const std::string& path, const std::string& comment) throw (DmException);

  void setGuid(const std::string& path, const std::string &guid) throw (DmException);
  

  Directory*     openDir (const std::string& path) throw (DmException);
  void           closeDir(Directory* dir) throw (DmException);
  struct dirent* readDir (Directory* dir) throw (DmException);
  ExtendedStat*  readDirx(Directory* dir) throw (DmException);

  void replicaSetLifeTime  (const std::string& replica, time_t ltime) throw (DmException);
  void replicaSetAccessTime(const std::string& replica) throw (DmException);
  void replicaSetType      (const std::string& replica, char type) throw (DmException);
  void replicaSetStatus    (const std::string& replica, char status) throw (DmException);

protected:
  /// Get the parent of a directory.
  /// @param path       The path to split.
  /// @param parentPath Where to put the parent path.
  /// @param name       Where to put the file name (stripping last /).
  /// @return           The parent metadata.
  ExtendedStat getParent(const std::string& path, std::string* parentPath,
                         std::string* name) throw (DmException);
  
  /// Update access time (if updateATime is true)
  void updateAccessTime(const ExtendedStat& meta) throw (DmException);
  
  /// Traverse backwards to check permissions.
  /// @param file The file at the end
  /// @note       Throws an exception if it is not possible.
  void traverseBackwards(const ExtendedStat& meta) throw (DmException);
  
private:
  StackInstance*   si_;
  
  const SecurityContext* secCtx_;
  
  std::string cwdPath_;
  ino_t       cwd_;
  
  mode_t   umask_;
  bool     updateATime_;
  unsigned symLinkLimit_;
};

/// Plug-ins must implement a concrete factory to be instantiated.
class BuiltInCatalogFactory: public CatalogFactory {
public:
  BuiltInCatalogFactory();
  ~BuiltInCatalogFactory();
  
  void configure(const std::string&, const std::string&) throw (DmException);
  
  Catalog* createCatalog(PluginManager*) throw (DmException);

protected:
private:
  bool     updateATime_;
  unsigned symLinkLimit_;
};
  
};

#endif	// BUILTIN_CATALOG_H
