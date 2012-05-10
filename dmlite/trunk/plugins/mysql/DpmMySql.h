/// @file    plugins/mysql/DpmMySql.h
/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DPMMYSQL_H
#define	DPMMYSQL_H

#include "NsMySql.h"
#include <dmlite/dm_pool.h>

namespace dmlite {

const int FS_ENABLED  = 0;
const int FS_DISABLED = 1;
const int FS_RDONLY   = 2;

/// Struct used internally to store information about a DPM
/// filesystem.
struct DpmFileSystem {
  char poolName[16];           ///< The pool name.
  char server  [HOST_NAME_MAX];///< The server where the filesystem is.
  char fs      [80];           ///< The file system whithin the server.
  int  status;                 ///< The status of the filesystem.
  int  weight;                 ///< The associated weight.
};

/// Implementation of DPM MySQL backend.
class DpmMySqlCatalog: public NsMySqlCatalog {
public:

  /// Constructor
  /// @param conn      The MySQL connection pool.
  /// @param nsDb      The MySQL DB name for the NS.
  /// @param dpmDb     The MySQL DB name for DPM.
  /// @param updateATime Update access time on each read.
  /// @param symLimit  The recursion limit for symbolic links.
  /// @param si        The stack instance.
  DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool,
                  const std::string& nsDb, const std::string& dpmDb,
                  bool updateATime, unsigned int symLimit,
                  StackInstance* si) throw(DmException);

  /// Destructor
  ~DpmMySqlCatalog() throw (DmException);

  // Overloading
  std::string getImplId(void) throw ();
  
  std::vector<Uri> getReplicasLocation(const std::string&) throw (DmException);
  Uri get (const std::string&) throw (DmException);

  void unlink (const std::string&) throw (DmException);

  std::string put      (const std::string&, Uri*)                     throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putDone  (const std::string&, const Uri&, const std::string&) throw (DmException);
  
protected:
private:
  /// DPM DB.
  std::string dpmDb_;
  
  /// Stack instance.
  StackInstance* stack_;
};

/// Pool manager implementation.
class MySqlPoolManager: public PoolManager {
public:
  MySqlPoolManager(PoolContainer<MYSQL*>* connPool, const std::string& dpmDb, StackInstance* si) throw (DmException);
  ~MySqlPoolManager();
  
  std::string getImplId(void) throw ();
  
  void setSecurityContext(const SecurityContext*) throw (DmException);
  
  PoolMetadata* getPoolMetadata(const Pool&) throw (DmException);
  
  std::vector<Pool> getPools() throw (DmException);
  Pool getPool(const std::string& poolname) throw (DmException);
  std::vector<Pool> getAvailablePools(bool) throw (DmException);
  
private:
  /// Plugin stack.
  StackInstance* stack_;
  
  /// DPM DB.
  std::string dpmDb_;
  
  /// The MySQL connection
  MYSQL* conn_;

  /// The connection pool.
  PoolContainer<MYSQL*>* connectionPool_;
  
  /// Security context
  const SecurityContext* secCtx_;
};

/// PoolMetadata handler
class MySqlPoolMetadata: public PoolMetadata {
public:
  MySqlPoolMetadata(Statement* stmt);
  ~MySqlPoolMetadata();
  
  std::string getString(const std::string& field) throw (DmException);
  int getInt(const std::string& field) throw (DmException);
private:
  Statement* stmt_;
};

};

#endif	// DPMMYSQL_H

