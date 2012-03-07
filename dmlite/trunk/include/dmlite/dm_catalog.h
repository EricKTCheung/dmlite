/// @file   include/dmlite/dm_catalog.h
/// @brief  Catalog API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CATALOG_H
#define	DMLITE_CATALOG_H

#include <cstdarg>
#include <string>
#include <vector>
#include <utime.h>
#include "dm_auth.h"
#include "dm_exceptions.h"
#include "dm_types.h"

namespace dmlite {

/// Typedef for directories
typedef void Directory;

/// Interface for Catalog (Namespaces)
class Catalog: public AuthBase {
public:
  /// Constructor.
  Catalog() throw (DmException);
  
  /// Destructor.
  virtual ~Catalog();

  /// String ID of the catalog implementation.
  virtual std::string getImplId(void) throw() = 0;

  /// Set a configuration parameter.
  /// @param key   The configuration parameter.
  virtual void set(const std::string& key, ...) throw (DmException);

  /// Set a configuration parameter.
  /// @param key   The configuration parameter.
  /// @param varg  The list of arguments. Depend on the key.
  virtual void set(const std::string& key, va_list varg) throw (DmException) = 0;

  /// Change the working dir. Future not-absolute paths will use this as root.
  /// @param path The new working dir.
  virtual void changeDir(const std::string& path) throw (DmException) = 0;

  /// Get the current working dir.
  /// @return The current working dir.
  virtual std::string getWorkingDir(void) throw (DmException) = 0;

  /// Get the current working dir inode.
  /// @return The cwd inode.
  virtual ino_t getWorkingDirI(void) throw (DmException) = 0;

  // Stat calls are implemented by default as extendedStat(...).stat

  /// Do a stat of a file or directory.
  /// @param path The path of the file or directory.
  /// @return     The status of the file.
  virtual struct stat stat(const std::string& path) throw (DmException);

  /// Do a stat of an entry using its inode.
  /// @param inode The entry inode.
  /// @return      The status of the file.
  /// @note        No security checks will be done.
  virtual struct stat stat(ino_t inode) throw (DmException);

  /// Do a stat of an entry using the parent inode and the name.
  /// @param parent The parent inode.
  /// @param name   The file or directory name.
  /// @note         No security check will be done.
  virtual struct stat stat(ino_t parent, const std::string& name) throw (DmException);

  /// Do a stat of a file or directory. Stats symbolic links.
  /// @param path The path of the file or direntory
  /// @return     The status of the link.
  /// @note       Implemented as extendedStat(path, false).stat
  virtual struct stat linkStat(const std::string& path) throw (DmException);

  /// Do an extended stat of a file or directory.
  /// @param path      The path of the file or directory.
  /// @param followSym If true, symlinks will be followed.
  /// @return          The extended status of the file.
  virtual ExtendedStat extendedStat(const std::string& path, bool followSym = true) throw (DmException) = 0;

  /// Do an extended stat of en entry using its inode.
  /// @param inode The entry inode.
  /// @return      The extended status of the file.
  /// @note        No security checks will be done.
  virtual ExtendedStat extendedStat(ino_t inode) throw (DmException) = 0;

  /// Do an extended stat of an entry using the parent inode and the name.
  /// @param parent The parent inode.
  /// @param name   The file or directory name.
  /// @note         No security check will be done.
  virtual ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException) = 0;

  /// Get the symlink associated with a inode.
  /// @param inode The inode to check.
  /// @return      A SymLink struct.
  /// @note        If inode is not a symlink, an exception will be thrown.
  virtual SymLink readLink(ino_t inode) throw (DmException) = 0;

  /// Add a new replica for a file.
  /// @param guid       The Grid Unique Identifier. It can be null.
  /// @param id         The file ID within the NS.
  /// @param server     The SE that hosts the file (if NULL, it will be retrieved from the sfn).
  /// @param sfn        The SURL or physical path of the replica being added.
  /// @param status     '-' for available, 'P' for being populated, 'D' for being deleted.
  /// @param fileType   'V' for volatile, 'D' for durable, 'P' for permanent.
  /// @param poolName   The pool where the replica is (not used for LFCs)
  /// @param fileSystem The filesystem where the replica is (not used for LFCs)
  virtual void addReplica(const std::string& guid, int64_t id, const std::string& server,
                          const std::string& sfn, char status, char fileType,
                          const std::string& poolName, const std::string& fileSystem) throw (DmException) = 0;

  /// Delete a replica.
  /// @param guid The Grid Unique Identifier. it can be null.
  /// @param id   The file ID within the NS.
  /// @param sfn  The replica being removed.
  virtual void deleteReplica(const std::string& guid, int64_t id,
                             const std::string& sfn) throw (DmException) = 0;

  /// Get replicas for a file.
  /// @param path         The file for which replicas will be retrieved. It can be null.
  /// @note               Either path or guid must be provided.
  virtual std::vector<FileReplica> getReplicas(const std::string& path) throw (DmException) = 0;

  /// Get a location for a logical name.
  /// @param path     The path to get.
  virtual FileReplica get(const std::string& path) throw (DmException) = 0;

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

  /// Start the PUT of a file.
  /// @param path  The path of the file to create.
  /// @param uri   The destination location will be put here.
  /// @return      The PUT token.
  virtual std::string put(const std::string& path, Uri* uri) throw (DmException) = 0;

  /// Start the PUT of a file.
  /// @param path  The path of the file to create.
  /// @param uri   The destination location will be put here.
  /// @param guid  The Grid Unique ID.
  /// @return      The PUT token.
  virtual std::string put(const std::string& path, Uri* uri,
                          const std::string& guid) throw (DmException) = 0;

  /// Get the PUT status
  /// @param path  The path of the file that was put
  /// @param token As returned by dm::Catalog::put
  /// @param uri   The destination location will be put here.
  virtual void putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException) = 0;

  /// Finish a PUT
  /// @param path  The path of the file that was put
  /// @param token As returned by dm::Catalog::put
  virtual void putDone(const std::string& path, const std::string& token) throw (DmException) = 0;

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
  virtual void changeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException) = 0;

  /// Change the owner of a file. Symbolic links are not followed.
  /// @param path   The file to change.
  /// @param newUid The uid of the new owneer.
  /// @param newGid The gid of the new group.
  virtual void linkChangeOwner(const std::string& path, uid_t newUid, gid_t newGid) throw (DmException) = 0;

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

  /// Get the group name associated with a group id.
  /// @param gid The group ID.
  /// @return    The group.
  virtual GroupInfo getGroup(gid_t gid) throw (DmException) = 0;

  /// Get the group id of a specific group name.
  /// @param groupName The group name.
  /// @return          The group.
  virtual GroupInfo getGroup(const std::string& groupName) throw (DmException) = 0;

  /// Get the uid/gid mapping of a user name.
  /// @param userName   The user name to map.
  /// @param groupNames An array of group names.
  /// @param uid        Will be set to the mapped uid.
  /// @param gids       Will be set to the mapped gids (one per group name).
  virtual void getIdMap(const std::string& userName, const std::vector<std::string>& groupNames,
                        uid_t* uid, std::vector<gid_t>* gids) throw (DmException) = 0;

  /// Get the name associated with a user id.
  /// @param uid      The user ID.
  virtual UserInfo getUser(uid_t uid) throw (DmException) = 0;

  /// Get the user id of a specific user name.
  /// @param userName The user name.
  virtual UserInfo getUser(const std::string& userName) throw (DmException) = 0;

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
  virtual struct direntstat* readDirx(Directory* dir) throw (DmException) = 0;

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
  virtual void replicaSetLifeTime(const std::string& replica, time_t ltime) throw (DmException) = 0;

  /// Set the access time of a replica
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetAccessTime(const std::string& replica) throw (DmException) = 0;

  /// Set the type of a replica
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @param ftype   The new type ('V', 'D' or 'P')
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetType(const std::string& replica, char type) throw (DmException) = 0;

  /// Set the status of a replica.
  /// @param context The DM context.
  /// @param replica The replica to modify.
  /// @param status  The new status ('-', 'P', 'D')
  /// @return        0 on success, error code otherwise.
  virtual void replicaSetStatus(const std::string& replica, char status) throw (DmException) = 0;

protected:
  /// The parent plugin can use this to let the decorated know
  /// who is over.
  virtual void setParent(Catalog* parent);

  /// Allow the plugin to get its parent.
  virtual Catalog* getParent(void);

private:
  Catalog* parent_;
};


/// Plug-ins must implement a concrete factory to be instantiated.
class CatalogFactory {
public:
  /// Virtual destructor
  virtual ~CatalogFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Instantiate a implementation of Catalog
  virtual Catalog* createCatalog() throw (DmException) = 0;

protected:
private:
};

};

#endif	// DMLITE_CATALOG_H

