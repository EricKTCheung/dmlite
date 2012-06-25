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
      stack_(0x00), dpmDb_(dpmDb), connectionPool_(connPool)
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
    PoolHandler* handler = this->stack_->getPoolDriver(pools[i].pool_type)->createPoolHandler(pools[i].pool_name);
    
    if (handler->isAvailable())
      available.push_back(pools[i]);
    
    delete handler;
  }
  
  return available;
}



PoolMetadata* MySqlPoolManager::getPoolMetadata(const std::string& pool) throw (DmException)
{
  Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOL_META);
  stmt.bindParam(0, pool.c_str());
  stmt.execute();
  
  char buffer[1024];
  stmt.bindResult(0, buffer, sizeof(buffer));
  
  if (!stmt.fetch()) {
    throw DmException(DM_NO_SUCH_POOL, "Pool %s not found", pool.c_str());
  }    
  
  return new MySqlPoolMetadata(buffer);
}



MySqlPoolMetadata::MySqlPoolMetadata(const char* meta)
{
  const char *cur, *next, *eq;
  std::string key, val;
  
  cur  = meta;
  
  while (cur != 0x00) {
    eq   = strchr(cur, '=');
    next = strchr(cur, ';');

    if (eq) {
      key = std::string(cur, eq - cur);
      if (next)
        val = std::string(eq + 1, next - eq - 1);
      else
        val = std::string(eq + 1);
      
      this->meta_.insert(std::pair<std::string, std::string>(key, val));
    }
    
    if (next)
      cur = next + 1;
    else
      cur = 0x00;
  };  
}



MySqlPoolMetadata::~MySqlPoolMetadata()
{
  // Nothing
}



std::string MySqlPoolMetadata::getString(const std::string& field) throw (DmException)
{
  return this->meta_[field];
}



int MySqlPoolMetadata::getInt(const std::string& field) throw (DmException)
{
  return atoi(this->meta_[field].c_str());
}
