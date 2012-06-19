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

/// Pool manager implementation.
class MySqlPoolManager: public PoolManager {
public:
  MySqlPoolManager(PoolContainer<MYSQL*>* connPool, const std::string& dpmDb) throw (DmException);
  ~MySqlPoolManager();
  
  std::string getImplId(void) throw ();
  
  void setStackInstance(StackInstance* si) throw (DmException);
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

