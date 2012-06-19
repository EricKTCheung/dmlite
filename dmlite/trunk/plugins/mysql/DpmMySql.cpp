/// @file    plugins/mysql/DpmMySql.cpp
/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>
#include <dmlite/common/Urls.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include "DpmMySql.h"
#include "Queries.h"

using namespace dmlite;



MySqlPoolManager::MySqlPoolManager(PoolContainer<MYSQL*>* connPool,
                                   const std::string& dpmDb) throw (DmException):
      connectionPool_(connPool), dpmDb_(dpmDb), stack_(0x00)
{
  this->conn_ = connPool->acquire();
}



MySqlPoolManager::~MySqlPoolManager()
{
  this->connectionPool_->release(this->conn_);
}



std::string MySqlPoolManager::getImplId() throw ()
{
  return "mysql_pool_manager";
}



void MySqlPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack_ = si;
}



void MySqlPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



std::vector<Pool> MySqlPoolManager::getPools() throw (DmException)
{
  Pool pool;
  std::vector<Pool> pools;
  
  try {
    Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOLS);

    stmt.execute();

    stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
    stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));

    while (stmt.fetch())
      pools.push_back(pool);
  }
  catch (DmException e) {
    if (e.code() != DM_UNKNOWN_FIELD)
      throw;
    
    // Fallback to legacy mode
    Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOLS_LEGACY);

    stmt.execute();

    stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
    stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));

    while (stmt.fetch())
      pools.push_back(pool);
  }

  return pools;
}



Pool MySqlPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  Pool pool;
  
  try {
    Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOL);

    stmt.bindParam(0, poolname);
    stmt.execute();
    stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
    stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));
    if (!stmt.fetch())
      throw DmException(DM_NO_SUCH_POOL, poolname + " not found");
  }
  catch (DmException e) {
    if (e.code() != DM_UNKNOWN_FIELD)
      throw;
    // Fallback to legacy mode
    strncpy(pool.pool_name, poolname.c_str(), sizeof(pool.pool_name));
    strcpy(pool.pool_type, "filesystem");
  }
  
  return pool;
}



std::vector<Pool> MySqlPoolManager::getAvailablePools(bool) throw (DmException)
{
  std::vector<Pool> pools = this->getPools();
  std::vector<Pool> available;
  
  for (unsigned i = 0; i < pools.size(); ++i) {
    PoolDriver* handler = this->stack_->getPoolDriver(pools[i]);
    
    if (handler->isAvailable())
      available.push_back(pools[i]);
  }
  
  return available;
}



PoolMetadata* MySqlPoolManager::getPoolMetadata(const Pool& pool) throw (DmException)
{
  // Build the query
  char query[128];
  snprintf(query, sizeof(query), "SELECT * FROM %s_pools WHERE poolname = ?", pool.pool_type);
  
  Statement* stmt = new Statement(this->conn_, this->dpmDb_, query);
  stmt->bindParam(0, pool.pool_name);
  stmt->execute(true);
  
  if (!stmt->fetch()) {
    delete stmt;
    throw DmException(DM_NO_SUCH_POOL, "Pool %s not found (type %s)",
                                       pool.pool_name, pool.pool_type);
  }    
  
  return new MySqlPoolMetadata(stmt);
}



MySqlPoolMetadata::MySqlPoolMetadata(Statement* stmt): stmt_(stmt)
{
  // Nothing
}



MySqlPoolMetadata::~MySqlPoolMetadata()
{
  delete this->stmt_;
}



std::string MySqlPoolMetadata::getString(const std::string& field) throw (DmException)
{
  return this->stmt_->getString(this->stmt_->getFieldIndex(field));
}



int MySqlPoolMetadata::getInt(const std::string& field) throw (DmException)
{
  return this->stmt_->getInt(this->stmt_->getFieldIndex(field));
}
