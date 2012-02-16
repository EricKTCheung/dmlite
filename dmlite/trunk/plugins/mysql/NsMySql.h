/// @file    plugins/mysql/NsMySql.h
/// @brief   MySQL NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NSMYSQL_H
#define	NSMYSQL_H

#include <dmlite/common/PoolContainer.h>
#include <dmlite/common/Security.h>
#include <dmlite/dm_catalog.h>
#include <mysql/mysql.h>
#include <vector>


#include "MySqlWrapper.h"

namespace dmlite {

/// Symbolic links
struct SymLink {
  uint64_t fileId;         ///< The file unique ID.
  char     link[PATH_MAX]; ///< Where the link is pointing to.
};

/// File metadata as is in the DB
struct FileMetadata {
  struct xstat xStat;
  char         acl[ACL_ENTRIES_MAX * 13];
};

/// Struct used internally to read directories.
struct NsMySqlDir {
  uint64_t          dirId;         ///< The directory ID.
  FileMetadata      current;       ///< Current entry metadata.
  struct direntstat ds;            ///< The structure used to hold the returned data.
  Statement        *stmt;          ///< The statement.
};

/// Replica extended information. Used by the DPM plugin.
struct ExtendedReplica {
  FileReplica replica;             ///< The regular replica information.
  char        pool[16];            ///< The pool where the replica is.
  char        host[HOST_NAME_MAX]; ///< The physicial host where the replica is.
  char        fs  [80];            ///< The filesystem where the replica is.
};

/// Implementation of NS MySQL backend.
class NsMySqlCatalog: public Catalog {
public:

  /// Constructor
  /// @param connPool  The MySQL connection pool.
  /// @param db        The MySQL db to use.
  /// @param symLimit  The recursion limit for symbolic links.
  NsMySqlCatalog(PoolContainer<MYSQL*>* connPool,
                 const std::string& db, unsigned int symLimit) throw (DmException);

  /// Destructor
  ~NsMySqlCatalog() throw (DmException);

  // Overloading
  std::string getImplId(void) throw ();

  void set(const std::string& key, va_list varg) throw (DmException);

  void        changeDir    (const std::string&) throw (DmException);
  std::string getWorkingDir(void)               throw (DmException);
  
  struct stat  stat        (const std::string&) throw (DmException);
  struct stat  stat        (ino_t)              throw (DmException);
  struct stat  linkStat    (const std::string&) throw (DmException);
  struct xstat extendedStat(const std::string&) throw (DmException);
  struct xstat extendedStat(ino_t)              throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                  const std::string&, char, char,
                  const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                     const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  FileReplica              get        (const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&)                     throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  std::string put      (const std::string&, Uri*)                     throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  void        putDone  (const std::string&, const std::string&)       throw (DmException);

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent*     readDir (Directory*) throw (DmException);
  struct direntstat* readDirx(Directory*) throw (DmException);

  mode_t umask(mode_t) throw ();

  void   changeMode     (const std::string&, mode_t)       throw (DmException);
  void   changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void   linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  std::string getComment(const std::string&)                     throw (DmException);
  void        setComment(const std::string&, const std::string&) throw (DmException);

  void setGuid(const std::string& path, const std::string &guid) throw (DmException);

  void makeDir  (const std::string&, mode_t) throw (DmException);
  void removeDir(const std::string&)         throw (DmException);

  void rename(const std::string&, const std::string&) throw (DmException);

  UserInfo  getUser (uid_t)              throw (DmException);
  UserInfo  getUser (const std::string&) throw (DmException);
  GroupInfo getGroup(gid_t)              throw (DmException);
  GroupInfo getGroup(const std::string&) throw (DmException);

  void getIdMap     (const std::string&, const std::vector<std::string>&,
                     uid_t*, std::vector<gid_t>*) throw (DmException);

  void setUserId  (uid_t, gid_t, const std::string&) throw (DmException);
  void setVomsData(const std::string& vo, const std::vector<std::string>& fqans) throw (DmException);
  
protected:
  UserInfo  user_;  ///< User.
  GroupInfo group_; ///< User main group.

  /// User secondary groups.
  std::vector<GroupInfo> groups_;

  /// The MySQL connection
  MYSQL* conn_;

  /// The connection pool.
  PoolContainer<MYSQL*>* connectionPool_;

  /// Current working dir (path)
  std::string cwdPath_;

  /// Current working dir
  ino_t cwd_;

  /// Get a list of extended replicas
  std::vector<ExtendedReplica> getExReplicas (const std::string&) throw (DmException);

  /// Get a list of replicas using file id
  std::vector<FileReplica> getReplicas(ino_t) throw (DmException);

  /// Umask
  mode_t umask_;

private:
  /// NS DB.
  std::string nsDb_;

  /// Symlink limit
  unsigned int symLinkLimit_;

  /// Precompiled statements
  std::vector<MYSQL_STMT*> preparedStmt_;

  // Private methods

  /// Returns the preparted statement with the specified ID.
  /// @param stId The statement ID (see STMT_*)
  /// @return     A pointer to a MySQL statement.
  MYSQL_STMT* getPreparedStatement(unsigned stId);

  /// Get a file using its unique ID.
  /// @param fileId The file unique ID.
  FileMetadata getFile(uint64_t fileId) throw (DmException);

  /// Get a file using its GUID.
  /// @param guid The file GUID.
  FileMetadata getFile(const std::string& guid) throw (DmException);

  /// Get a file from a directory using its name.
  /// @param name     The file name (NOT path).
  /// @param parentId The parent directory unique ID.
  FileMetadata getFile(const std::string& name, uint64_t parentId) throw (DmException);

  /// Get a symbolic link using its unique ID.
  /// @param linkId The link ID.
  SymLink getLink(uint64_t linkId) throw (DmException);

  /// Parses a path looking for a file, and returns its metadata.
  /// @param path     The file path.
  /// @param folloSym If symbolic links must be followed.
  FileMetadata parsePath(const std::string& path, bool followSym = true) throw (DmException);

  /// Create a file/directory and returns its metadata.
  /// @param parent    The parent metadata.
  /// @param name      The new file name.
  /// @param mode      The new file mode.
  /// @param nlink     The number of links.
  /// @param size      The file size.
  /// @param type      The file type.
  /// @param status    The file status.
  /// @param csumtype  The checksum type.
  /// @param csumvalue The checksum value.
  /// @param acl       The access control list.
  /// @note No transaction is used! Calling method is supposed to do it.
  FileMetadata newFile(FileMetadata& parent, const std::string& name, mode_t mode,
                       long nlink, size_t size, short type, char status,
                       const std::string& csumtype, const std::string& csumvalue,
                       const std::string& acl) throw (DmException);

  /// Get the parent of a directory.
  /// @param path       The path to split.
  /// @param parentPath Where to put the parent path.
  /// @param name       Where to put the file name (stripping last /).
  /// @return           The parent metadata.
  FileMetadata getParent(const std::string& path, std::string* parentPath,
                         std::string* name) throw (DmException);

  /// Add a new user.
  /// @param uname The user name.
  /// @param ca    The user CA.
  /// @return      A UserInfo struct, including the new uid.
  /// @note        Transactions are used.
  UserInfo  newUser(const std::string& uname, const std::string& ca) throw (DmException);

  /// Add a new group.
  /// @param gname The group name.
  /// @return      A GroupInfo struct, including the new gid.
  /// @note        Transactions are used.
  GroupInfo newGroup(const std::string& gname) throw (DmException);

  /// Change the file owner
  /// @param meta
  /// @param newUid
  /// @param newGid
  void changeOwner(FileMetadata& meta, uid_t newUid, gid_t newGid) throw (DmException);

  /// Traverse backwards to check permissions.
  /// @param file The file at the end
  /// @note       Throws an exception if it is not possible.
  void traverseBackwards(const FileMetadata& meta) throw (DmException);

};

};

#endif	// NSMYSQL_H
