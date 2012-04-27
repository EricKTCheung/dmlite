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

/// Struct used internally to read directories.
struct NsMySqlDir {
  uint64_t      dirId;         ///< The directory ID.
  ExtendedStat  current;       ///< Current entry metadata.
  struct dirent ds;            ///< The structure used to hold the returned data.
  Statement    *stmt;          ///< The statement.
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

  SecurityContext* createSecurityContext(const SecurityCredentials&) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

  void        changeDir     (const std::string&) throw (DmException);
  std::string getWorkingDir (void)               throw (DmException);
  ino_t       getWorkingDirI(void)               throw (DmException);
  
  ExtendedStat extendedStat(const std::string&, bool = true) throw (DmException);
  ExtendedStat extendedStat(ino_t)              throw (DmException);
  ExtendedStat extendedStat(ino_t, const std::string&) throw (DmException);

  SymLink readLink(ino_t) throw (DmException);

  void addReplica(const std::string&, int64_t, const std::string&,
                  const std::string&, char, char,
                  const std::string&, const std::string&) throw (DmException);

  void deleteReplica(const std::string&, int64_t,
                     const std::string&) throw (DmException);

  std::vector<FileReplica> getReplicas(const std::string&) throw (DmException);
  Uri                      get        (const std::string&) throw (DmException);

  void symlink(const std::string&, const std::string&) throw (DmException);
  void unlink (const std::string&)                     throw (DmException);

  void create(const std::string&, mode_t) throw (DmException);

  std::string put      (const std::string&, Uri*)                     throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putDone  (const std::string&, const Uri&, const std::string&) throw (DmException);

  Directory* openDir (const std::string&) throw (DmException);
  void       closeDir(Directory*)         throw (DmException);

  struct dirent* readDir (Directory*) throw (DmException);
  ExtendedStat*  readDirx(Directory*) throw (DmException);

  mode_t umask(mode_t) throw ();

  void changeMode     (const std::string&, mode_t)       throw (DmException);
  void changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);

  void changeSize(const std::string&, size_t) throw (DmException);
  
  void setAcl(const std::string&, const std::vector<Acl>&) throw (DmException);

  void utime(const std::string&, const struct utimbuf*) throw (DmException);
  void utime(ino_t, const struct utimbuf*)              throw (DmException);

  std::string getComment(const std::string&)                     throw (DmException);
  void        setComment(const std::string&, const std::string&) throw (DmException);

  void setGuid(const std::string& path, const std::string &guid) throw (DmException);

  void makeDir  (const std::string&, mode_t) throw (DmException);
  void removeDir(const std::string&)         throw (DmException);

  void rename(const std::string&, const std::string&) throw (DmException);

  void replicaSetLifeTime  (const std::string&, time_t) throw (DmException);
  void replicaSetAccessTime(const std::string&)         throw (DmException);
  void replicaSetType      (const std::string&, char)   throw (DmException);
  void replicaSetStatus    (const std::string&, char)   throw (DmException);

  UserInfo  getUser (uid_t)              throw (DmException);
  UserInfo  getUser (const std::string&) throw (DmException);
  GroupInfo getGroup(gid_t)              throw (DmException);
  GroupInfo getGroup(const std::string&) throw (DmException);
  
protected:
  /// Security context
  const SecurityContext* secCtx_;

  /// The MySQL connection
  MYSQL* conn_;

  /// The connection pool.
  PoolContainer<MYSQL*>* connectionPool_;

  /// Current working dir (path)
  std::string cwdPath_;

  /// Current working dir
  ino_t cwd_;

  /// Get a list of replicas using file id
  std::vector<FileReplica> getReplicas(ino_t) throw (DmException);

  /// Umask
  mode_t umask_;

private:
  /// NS DB.
  std::string nsDb_;

  /// Symlink limit
  unsigned int symLinkLimit_;

  // Private methods

  /// Get a file using its GUID.
  /// @param guid The file GUID.
  ExtendedStat guidStat(const std::string& guid) throw (DmException);

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
  ExtendedStat newFile(ExtendedStat& parent, const std::string& name, mode_t mode,
                       long nlink, size_t size, short type, char status,
                       const std::string& csumtype, const std::string& csumvalue,
                       const std::string& acl) throw (DmException);

  /// Get the parent of a directory.
  /// @param path       The path to split.
  /// @param parentPath Where to put the parent path.
  /// @param name       Where to put the file name (stripping last /).
  /// @return           The parent metadata.
  ExtendedStat getParent(const std::string& path, std::string* parentPath,
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
  void changeOwner(ExtendedStat& meta, uid_t newUid, gid_t newGid) throw (DmException);

  /// Traverse backwards to check permissions.
  /// @param file The file at the end
  /// @note       Throws an exception if it is not possible.
  void traverseBackwards(const ExtendedStat& meta) throw (DmException);

  /// Get a replica by its URL
  /// @param replica The replica URL.
  FileReplica replicaGet(const std::string& replica) throw (DmException);

  /// Set the replica attributes
  void replicaSet(const FileReplica& rdata) throw (DmException);

  /// Get the ID mapping of a user.
  void getIdMap(const std::string&, const std::vector<std::string>&,
                UserInfo*, std::vector<GroupInfo>*) throw (DmException);
};

};

#endif	// NSMYSQL_H
