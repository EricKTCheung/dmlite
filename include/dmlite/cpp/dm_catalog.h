/// @file   include/dmlite/dm_catalog.h
/// @brief  Catalog API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_CATALOG_H
#define	DMLITEPP_CATALOG_H

#include <cstdarg>
#include <string>
#include <vector>
#include <utime.h>
#include "dm_exceptions.h"
#include "../common/dm_types.h"

namespace dmlite {

/// Typedef for directories
typedef void Directory;

// Advanced declarations
class StackInstance;
class PluginManager;

/// Interface for Catalog (Namespaces)
class Catalog {
public: 
  /// Destructor.
  virtual ~Catalog();

  /// String ID of the catalog implementation.
  virtual std::string getImplId(void) throw() = 0;

  /// Set the StackInstance.
  /// Some plugins may need to access other stacks (i.e. the pool may need the catalog)
  /// However, at construction time not all the stacks have been populated, so this will
  /// be called once all are instantiated.
  virtual void setStackInstance(StackInstance* si) throw (DmException) = 0;
  
  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException) = 0;

  /// Change the working dir. Future not-absolute paths will use this as root.
  /// @param path The new working dir.
  virtual void changeDir(const std::string& path) throw (DmException) = 0;

  /// Get the current working dir.
  /// @return The current working dir.
  virtual std::string getWorkingDir(void) throw (DmException) = 0;

  /// Do an extended stat of a file or directory.
  /// @param path      The path of the file or directory.
  /// @param followSym If true, symlinks will be followed.
  /// @return          The extended status of the file.
  virtual ExtendedStat extendedStat(const std::string& path, bool followSym = true) throw (DmException) = 0;

  /// Add a new replica for a file.
  /// @param guid       The Grid Unique Identifier. It can be null.
  /// @param id         The file ID within the NS.
  /// @param server     The SE that hosts the file (if NULL, it will be retrieved from the rfn).
  /// @param rfn        The SURL or physical path of the replica being added.
  /// @param status     '-' for available, 'P' for being populated, 'D' for being deleted.
  /// @param fileType   'V' for volatile, 'D' for durable, 'P' for permanent.
  /// @param poolName   The pool where the replica is (not used for LFCs)
  /// @param fileSystem The filesystem where the replica is (not used for LFCs)
  virtual void addReplica(const std::string& guid, int64_t id, const std::string& server,
                          const std::string& rfn, char status, char fileType,
                          const std::string& poolName, const std::string& fileSystem) throw (DmException) = 0;

  /// Delete a replica.
  /// @param guid The Grid Unique Identifier. it can be null.
  /// @param id   The file ID within the NS.
  /// @param rfn  The replica being removed.
  virtual void deleteReplica(const std::string& guid, int64_t id,
                             const std::string& rfn) throw (DmException) = 0;

  /// Get replicas for a file.
  /// @param path The file for which replicas will be retrieved.
  virtual std::vector<FileReplica> getReplicas(const std::string& path) throw (DmException) = 0;

  /// Get a location for a logical name.
  /// @param path     The path to get.
  virtual Location get(const std::string& path) throw (DmException) = 0;
  
  /// Start the PUT of a file.
  /// @param path  The path of the file to create.
  /// @return      The physical location where to write.
  virtual Location put(const std::string& path) throw (DmException) = 0;

  /// Finish a PUT
  /// @param host   The host where the replica is hosted.
  /// @param rfn    The replica file name.
  /// @param params The extra parameters returned by dm::Catalog::put
  virtual void putDone(const std::string& host, const std::string& rfn,
                       const std::map<std::string, std::string>& params) throw (DmException) = 0;

  /// Creates a new symlink.
  /// @param oldpath The existing path.
  /// @param newpath The new access path.
  virtual void symlink(const std::string& oldpath, const std::string& newpath) throw (DmException) = 0;

  /// Remove a file.
  /// @param path The path to remove.
  virtual void unlink(const std::string& path) throw (DmException) = 0;

  /// Creates an entry in the catalog.
  /// @param path The new file.
  /// @param mode The creation mode.
  virtual void create(const std::string& path, mode_t mode) throw (DmException) = 0;

  /// Sets the calling process’s file mode creation mask to mask & 0777.
  /// @param mask The new mask.
  /// @return     The value of the previous mask.
  virtual mode_t umask(mode_t mask) throw () = 0;

  /// Change the mode of a file.
  /// @param path The file to change.
  /// @param mode The new mode as an integer (i.e. 0755)
  virtual void changeMode(const std::string& path, mode_t mode) throw (DmException) = 0;

  /// Change the owner of a file.
  /// @param path   The file to change.
  /// @param newUid The uid of the new owneer.
  /// @param newGid The gid of the new group.
  virtual void changeOwner(const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink = true) throw (DmException) = 0;
  
  /// Change the size of a file.
  /// @param path    The file to change.
  /// @param newSize The new file size.
  virtual void changeSize(const std::string& path, size_t newSize) throw (DmException) = 0;
  
  /// Change the checksum of a file.
  /// @param path      The file to change.
  /// @param csumtype  The checksum type (CS, AD or MD).
  /// @param csumvalue The checksum value.
  virtual void changeChecksum(const std::string& path, const std::string& csumtype, const std::string& csumvalue) throw (DmException) = 0;

  /// Change the ACLs
  /// @param path The file to change.
  /// @param acls The ACL's.
  virtual void setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException) = 0;

  /// Change access and/or modification time.
  /// @param path The file path.
  /// @param buf  A struct holding the new times.
  virtual void utime(const std::string& path, const struct utimbuf* buf) throw (DmException) = 0;

  /// Get the comment associated with a file.
  /// @param path The file or directory.
  /// @return     The associated comment.
  virtual std::string getComment(const std::string& path) throw (DmException) = 0;

  /// Set the comment associated with a file.
  /// @param path    The file or directory.
  /// @param comment The new comment.
  virtual void setComment(const std::string& path, const std::string& comment) throw (DmException) = 0;

  /// Set GUID of a file.
  /// @param path The file.
  /// @param guid The new GUID.
  virtual void setGuid(const std::string& path, const std::string &guid) throw (DmException) = 0;
  
  /// Open a directory for reading.
  /// @param path The directory to open.
  /// @return     A pointer to a handle that can be used for later calls.
  virtual Directory* openDir(const std::string& path) throw (DmException) = 0;

  /// Close a directory opened previously.
  /// @param dir The directory handle as returned by NsInterface::openDir.
  virtual void closeDir(Directory* dir) throw (DmException) = 0;

  /// Read next entry from a directory (simple read).
  /// @param dir The directory handle as returned by NsInterface::openDir.
  /// @return    0x00 on failure or end of directory.
  virtual struct dirent* readDir(Directory* dir) throw (DmException) = 0;

  /// Read next entry from a directory (stat information added).
  /// @param dir The directory handle as returned by NsInterface::openDir.
  /// @return    0x00 on failure (and errno is set) or end of directory.
  virtual ExtendedStat* readDirx(Directory* dir) throw (DmException) = 0;

  /// Create a new empty directory.
  /// @param path The path of the new directory.
  /// @param mode The creation mode.
  virtual void makeDir(const std::string& path, mode_t mode) throw (DmException) = 0;

  /// Rename a file or directory.
  /// @param oldPath The old name.
  /// @param newPath The new name.
  virtual void rename(const std::string& oldPath, const std::string& newPath) throw (DmException) = 0;

  /// Remove a directory.
  /// @param path The path of the directory to remove.
  virtual void removeDir(const std::string& path) throw (DmException) = 0;

  /// Set the life time of a replica
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @param ltime   The new life time.
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetLifeTime(const std::string& rfn, time_t ltime) throw (DmException) = 0;

  /// Set the access time of a replica
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetAccessTime(const std::string& rfn) throw (DmException) = 0;

  /// Set the type of a replica
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @param ftype   The new type ('V', 'D' or 'P')
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetType(const std::string& rfn, char type) throw (DmException) = 0;

  /// Set the status of a replica.
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @param status  The new status ('-', 'P', 'D')
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetStatus(const std::string& rfn, char status) throw (DmException) = 0;
};



/// Plug-ins must implement a concrete factory to be instantiated.
class CatalogFactory: public virtual BaseFactory {
public:
  /// Virtual destructor
  virtual ~CatalogFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

protected:
  // Stack instance is allowed to instantiate catalogs
  friend class StackInstance;  
  
  /// Children of CatalogFactory are allowed to instantiate too (decorator)
  static Catalog* createCatalog(CatalogFactory* factory, PluginManager* pm) throw (DmException);
  
  /// Instantiate a implementation of Catalog
  virtual Catalog* createCatalog(PluginManager* pm) throw (DmException) = 0;

private:
};

};

#endif	// DMLITEPP_CATALOG_H
