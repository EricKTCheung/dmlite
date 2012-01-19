/// @file   include/dm/dm_interfaces.h
/// @brief  API to be used by client code and implemented by plugins.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PLUGIN_H
#define	PLUGIN_H

#include <cstdarg>
#include <vector>
#include "dm_types.h"
#include "dm_exceptions.h"

namespace dmlite {

const unsigned API_VERSION = 20120117;

/// Typedef for directories
typedef void Directory;

/// Interface for Catalog (Namespaces)
class Catalog {
public:
  /// Constructor.
  Catalog() throw (DmException);
  
  /// Destructor.
  virtual ~Catalog();

  /// String ID of the catalog implementation.
  virtual std::string getImplId(void) = 0;

  /// Set a configuration parameter.
  /// @param key   The configuration parameter.
  virtual void set(const std::string& key, ...) throw (DmException) = 0;

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

  /// Do a stat of a file or directory.
  /// @param path The path of the file or directory.
  /// @return     The status of the file.
  virtual struct stat stat(const std::string& path) throw (DmException) = 0;

  /// Do a stat of a file or directory. Stats symbolic links.
  /// @param path The path of the file or direntory
  /// @return     The status of the link.
  virtual struct stat linkStat(const std::string& path) throw (DmException) = 0;

  /// Add a new replica for a file.
  /// @param guid       The Grid Unique Identifier. It can be null.
  /// @param id         The file ID within the NS.
  /// @param server     The SE that hosts the file.
  /// @param sfn        The SURL or physical path of the replica being added.
  /// @param status     '-' for available, 'P' for being populated, 'D' for being deleted.
  /// @param fileType   'V' for volatile, 'D' for durable, 'P' for permanent.
  /// @param poolName   The pool where the replica is (not used for LFCs)
  /// @param fileSystem The filesystem where the replica is (not used for LFCs)
  virtual void addReplica(const std::string& guid, int64_t id, const std::string& server,
                          const std::string& sfn, const char status, const char fileType,
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

  /// Start the PUT of a file.
  /// @param path  The path of the file to create.
  /// @param uri   The destination location will be put here.
  /// @return      The PUT token.
  virtual std::string put(const std::string& path, Uri* uri) throw (DmException) = 0;

  /// Get the PUT status
  /// @param path  The path of the file that was put
  /// @param token As returned by dm::Catalog::put
  /// @param uri   The destination location will be put here.
  virtual void putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException) = 0;

  /// Finish a PUT
  /// @param path  The path of the file that was put
  /// @param token As returned by dm::Catalog::put
  virtual void putDone(const std::string& path, const std::string& token) throw (DmException) = 0;

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

  /// Set the user ID that will perform the actions.
  /// @param uid The UID.
  /// @param gid The GID.
  /// @param dn  The full DN (i.e. /DC=ch/DC=cern/OU=Organic Units/...).
  virtual void setUserId(uid_t uid, gid_t gid, const std::string& dn) throw (DmException) = 0;

  /// Set the user associated VO data.
  /// @param vo     The main Virtual Organization (i.e. dteam).
  /// @param fqans  The FQANS.
  virtual void setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException) = 0;

  /// Get the list of pools.
  /// @return A set with all the pools.
  virtual std::vector<Pool> getPools(void) throw (DmException) = 0;

  /// Get the list of filesystems in a pool.
  /// @param poolname The pool name.
  /// @return         A set with the filesystems that belong to the pool.
  virtual std::vector<FileSystem> getPoolFilesystems(const std::string& poolname) throw (DmException) = 0;

protected:
  /// Can be used by decorators to let the underlying plugin know who is on top.
  /// @param catalog The catalog on top.
  virtual void setParent(Catalog* catalog);

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
  virtual void set(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Instantiate a implementation of Interface
  virtual Catalog* create() throw (DmException) = 0;

protected:
private:
};

class PluginManager;
/// This structure is the join between the plug-ins and libdm.
struct PluginIdCard {
  /// Used to make sure API is consistent.
  unsigned const  ApiVersion;
  /// Let the plug-in register itself and its concrete factories
  void (*registerPlugin)(PluginManager* pm) throw (DmException);
};

};

#endif	/* PLUGIN_H */

