/// @file    plugins/mysql/NsMySql.h
/// @brief   MySQL NS Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NSMYSQL_H
#define	NSMYSQL_H

#include <dmlite/cpp/utils/dm_poolcontainer.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/dm_inode.h>
#include <mysql/mysql.h>
#include <vector>


#include "MySqlWrapper.h"

namespace dmlite {

/// Struct used internally to read directories.
struct NsMySqlDir {
  ExtendedStat  dir;           ///< Directory being read.
  ExtendedStat  current;       ///< Current entry metadata.
  struct dirent ds;            ///< The structure used to hold the returned data.
  Statement    *stmt;          ///< The statement.
};

/// Implementation of INode MySQL backend.
class INodeMySql: public INode {
public:

  /// Constructor
  /// @param connPool    The MySQL connection pool.
  /// @param db          The MySQL db to use.
  INodeMySql(PoolContainer<MYSQL*>* connPool,
               const std::string& db) throw (DmException);

  /// Destructor
  ~INodeMySql() throw (DmException);

  // Overloading
  std::string getImplId(void) throw ();
  
  void setStackInstance(StackInstance* si) throw (DmException);
  void setSecurityContext(const SecurityContext* ctx) throw (DmException);
  
  void begin(void) throw (DmException);
  void commit(void) throw (DmException);
  void rollback(void) throw (DmException);

  ExtendedStat create(ino_t parent, const std::string& name,
                      uid_t uid, gid_t gid, mode_t mode,
                      size_t size, short type, char status,
                      const std::string& csumtype, const std::string& csumvalue,
                      const std::string& acl) throw (DmException);

  void symlink(ino_t inode, const std::string &link) throw (DmException);

  void unlink(ino_t inode) throw (DmException);
  
  void move  (ino_t inode, ino_t dest) throw (DmException);
  void rename(ino_t inode, const std::string& name) throw (DmException);
  
  ExtendedStat extendedStat(ino_t inode) throw (DmException);
  ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException);
  ExtendedStat extendedStat(const std::string& guid) throw (DmException);

  SymLink readLink(ino_t inode) throw (DmException);
  
  void addReplica   (ino_t inode, const std::string& server,
                     const std::string& sfn, char status, char fileType,
                     const std::string& poolName, const std::string& fileSystem) throw (DmException);  
  void deleteReplica(ino_t id, const std::string& sfn) throw (DmException);
  
  std::vector<FileReplica> getReplicas(ino_t inode) throw (DmException);
  
  FileReplica getReplica(const std::string& sfn) throw (DmException);
  void        setReplica(const FileReplica& replica) throw (DmException);
  
  void utime(ino_t inode, const struct utimbuf* buf) throw (DmException);
  
  void changeMode (ino_t inode, uid_t uid, gid_t gid, mode_t mode, const std::string& acl) throw (DmException);
  
  void changeSize(ino_t inode, size_t size) throw (DmException);
  
  std::string getComment   (ino_t inode) throw (DmException);
  void        setComment   (ino_t inode, const std::string& comment) throw (DmException);
  void        deleteComment(ino_t inode) throw (DmException);
  
  void setGuid(ino_t inode, const std::string& guid) throw (DmException);
  
  IDirectory*     openDir (ino_t inode) throw (DmException);
  void           closeDir(IDirectory* dir) throw (DmException);  
  ExtendedStat*  readDirx(IDirectory* dir) throw (DmException);
  struct dirent* readDir (IDirectory* dir) throw (DmException);
  
protected:
  /// The MySQL connection
  MYSQL* conn_;

  /// The connection pool.
  PoolContainer<MYSQL*>* connectionPool_;
  
  /// Transaction level, so begins and commits can be nested.
  unsigned transactionLevel_;

private:
  /// NS DB.
  std::string nsDb_;
  
};

};

#endif	// NSMYSQL_H
