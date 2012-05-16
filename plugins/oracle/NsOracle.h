/// @file    plugins/oracle/NsOracle.h
/// @brief   Oracle NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NSORACLEL_H
#define	NSORACLE_H

#include <dmlite/common/Security.h>
#include <dmlite/dm_catalog.h>
#include <occi.h>
#include <vector>


namespace dmlite {

/// Struct used internally to read directories.
struct NsOracleDir {
  uint64_t                 dirId;         ///< The directory ID.
  ExtendedStat             current;       ///< Current entry metadata.
  struct dirent            ds;            ///< The structure used to hold the returned data.
  oracle::occi::Statement *stmt;          ///< The statement.
  oracle::occi::ResultSet *rs;            ///< The result set.
};

/// Replica extended information. Used by the DPM plugin.
struct ExtendedReplica {
  FileReplica replica;             ///< The regular replica information.
  char        pool[16];            ///< The pool where the replica is.
  char        host[HOST_NAME_MAX]; ///< The physicial host where the replica is.
  char        fs  [80];            ///< The filesystem where the replica is.
};

/// Implementation of NS Oracle backend.
class NsOracleCatalog: public Catalog {
public:

  /// Constructor
  /// @param pool     The Oracle connection pool.
  /// @param conn     The Oracle connection.
  /// @param symLimit The recursion limit for symbolic links.
  NsOracleCatalog(oracle::occi::ConnectionPool* pool,
                  oracle::occi::Connection* conn,
                  unsigned int symLimit) throw (DmException);

  /// Destructor
  ~NsOracleCatalog() throw (DmException);

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
  std::vector<Uri>         getReplicasLocation(const std::string&) throw (DmException);
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

  void   changeMode     (const std::string&, mode_t)       throw (DmException);
  void   changeOwner    (const std::string&, uid_t, gid_t) throw (DmException);
  void   linkChangeOwner(const std::string&, uid_t, gid_t) throw (DmException);
  
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

  /// The Oracle connection pool.
  oracle::occi::ConnectionPool* pool_;

  /// The Oracle connection
  oracle::occi::Connection* conn_;

  /// Current working dir (path)
  std::string cwdPath_;

  /// Current working dir
  ino_t cwd_;

  /// Get a list of replicas using file id
  std::vector<FileReplica> getReplicas(ino_t) throw (DmException);

  /// Umask
  mode_t umask_;

private:
  /// Symlink limit
  unsigned int symLinkLimit_;

  // Private methods

  /// Returns the preparted statement with the specified ID.
  /// @param stId The statement ID (see STMT_*)
  /// @return     A pointer to a Oracle statement.
  oracle::occi::Statement* getPreparedStatement(unsigned stId);

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

  /// Get user mapping.
  void getIdMap(const std::string&, const std::vector<std::string>&,
                UserInfo*, std::vector<GroupInfo>*) throw (DmException);

  /// Update nlinks
  void updateNlink(ino_t fileid, int diff) throw (DmException);
};

};

#endif	// NSORACLE_H
