/// @file    plugins/mysql/NsMySql.h
/// @brief   MySQL NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NSMYSQL_H
#define	NSMYSQL_H

#include <dmlite/Dummy.h>
#include <mysql/mysql.h>
#include <vector>

#include "../common/PoolContainer.h"
#include "../common/Security.h"

namespace dmlite {

/// Symbolic links
struct SymLink {
  uint64_t fileId;         ///< The file unique ID.
  char     link[PATH_MAX]; ///< Where the link is pointing to.
};

/// File metadata as is in the DB
struct FileMetadata {
  uint64_t    parentId;                  ///< The parent unique ID.
  char        guid[GUID_MAX+1];          ///< The file GUID.
  char        name[NAME_MAX+1];          ///< The file name.
  short       fileClass;                 ///< The file class.
  char        status;                    ///< The status of the file.
  char        csumtype[3];               ///< Checksum type.
  char        csumvalue[33];             ///< Checksum value.
  char        acl[ACL_ENTRIES_MAX * 13]; ///< ACL
  struct stat stStat;                    ///< File standard information.
};

/// Struct used internally to read directories.
struct NsMySqlDir {
  uint64_t          dirId;         ///< The directory ID.
  FileMetadata      current;       ///< Current entry metadata.
  struct direntstat extended;      ///< The structure used to hold the returned data.
  MYSQL_BIND        bindParam[0];  ///< Bind parameters.
  MYSQL_BIND        bindResult[17];///< Bind results.
  MYSQL_STMT       *statement;     ///< Statement.
};

/// Replica extended information. Used by the DPM plugin.
struct ExtendedReplica {
  FileReplica replica;             ///< The regular replica information.
  char        pool[16];            ///< The pool where the replica is.
  char        host[HOST_NAME_MAX]; ///< The physicial host where the replica is.
  char        fs  [80];            ///< The filesystem where the replica is.
};

/// Implementation of NS MySQL backend.
class NsMySqlCatalog: public DummyCatalog {
public:

  /// Constructor
  /// @param connPool  The MySQL connection pool.
  /// @param db        The MySQL db to use.
  /// @param decorates The underlying decorated catalog.
  /// @param symLimit  The recursion limit for symbolic links.
  NsMySqlCatalog(PoolContainer<MYSQL*>* connPool,
                 const std::string& db, Catalog* decorates,
                 unsigned int symLimit) throw (DmException);

  /// Destructor
  ~NsMySqlCatalog() throw (DmException);

  // Overloading
  std::string getImplId(void);

  void        changeDir    (const std::string&) throw (DmException);
  std::string getWorkingDir(void)               throw (DmException);
  
  struct stat  stat        (const std::string&) throw (DmException);
  struct stat  stat        (ino_t)              throw (DmException);
  struct stat  linkStat    (const std::string&) throw (DmException);
  struct xstat extendedStat(const std::string&) throw (DmException);
  struct xstat extendedStat(ino_t)              throw (DmException);

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent*     readDir (Directory*) throw (DmException);
  struct direntstat* readDirx(Directory*) throw (DmException);

  UserInfo  getUser (uid_t)              throw (DmException);
  UserInfo  getUser (const std::string&) throw (DmException);
  GroupInfo getGroup(gid_t)              throw (DmException);
  GroupInfo getGroup(const std::string&) throw (DmException);

  void getIdMap     (const std::string&, const std::vector<std::string>&,
                     uid_t*, std::vector<gid_t>*) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  FileReplica              get        (const std::string&) throw (DmException);

  void  setUserId     (uid_t, gid_t, const std::string&) throw (DmException);

  std::string getComment(const std::string&)                     throw (DmException);

  void setGuid(const std::string& path, const std::string &guid) throw (DmException);
  
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

  /// Current working dir (metadata)
  FileMetadata cwdMeta_;

  /// Get a list of extended replicas
  std::vector<ExtendedReplica> getExReplicas (const std::string&) throw (DmException);

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
  FileMetadata getFile (uint64_t fileId) throw (DmException);

  /// Get a file from a directory using its name.
  /// @param name     The file name (NOT path).
  /// @param parentId The parent directory unique ID.
  FileMetadata getFile (const std::string& name, uint64_t parentId) throw (DmException);

  /// Get a symbolic link using its unique ID.
  /// @param linkId The link ID.
  SymLink getLink (uint64_t linkId) throw (DmException);

  /// Parses a path looking for a file, and returns its metadata.
  /// @param path     The file path.
  /// @param folloSym If symbolic links must be followed.
  FileMetadata parsePath (const std::string& path, bool followSym = true) throw (DmException);
  
  /// Get a user from the database using the username or the user ID.
  /// @details If userName is empty, the uid will be used instead.
  /// @param userName The user name.
  /// @param uid      The user unique id. Used if userName is NULL.
  UserInfo getUser (const std::string& userName, uid_t uid) throw (DmException);

  /// Get a group from the database using the groupname or the group ID.
  /// @details If groupName is empty, the gid will be used instead.
  /// @param groupName The group name.
  /// @param gid       The group unique id. Used if groupName is NULL.
  GroupInfo getGroup (const std::string& groupName, gid_t gid) throw (DmException);
};

};

#endif	// NSMYSQL_H
