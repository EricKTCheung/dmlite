/// @file    plugins/mysql/DpmMySql.cpp
/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dmlite++.h>
#include <dmlite/common/Uris.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include "DpmMySql.h"
#include "Queries.h"

using namespace dmlite;



DpmMySqlCatalog::DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& nsDb,
                                 const std::string& dpmDb,
                                 unsigned int symLinkLimit, StackInstance* si) throw(DmException):
  NsMySqlCatalog(connPool, nsDb, symLinkLimit), dpmDb_(dpmDb), stack_(si)
{
  // Nothing
}



DpmMySqlCatalog::~DpmMySqlCatalog() throw(DmException)
{
  // Nothing
}



std::string DpmMySqlCatalog::getImplId() throw ()
{
  return std::string("DpmMySqlCatalog");
}



Uri DpmMySqlCatalog::get(const std::string& path) throw(DmException)
{
  // Get replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Get the status for the Fs+Pool+Server for each replica
  std::vector<Uri> available;

  // Iterate and mark as unavailable if the FS is unavailable
  unsigned i;
  for (i = 0; i < replicas.size(); ++i) {
    Pool pool = this->stack_->getPoolManager()->getPool(replicas[i].pool);
    PoolHandler* handler = this->stack_->getPoolHandler(&pool);

    if (handler->replicaAvailable(path, replicas[i])) {
      available.push_back(handler->getLocation(path, replicas[i]));
    }
  }

  if (available.size() > 0) {
    // Pick a random one from the available
    i = rand() % available.size();
    return available[i];
  }
  else {
    throw DmException(DM_NO_REPLICAS, "No available replicas");
  }
}



std::string DpmMySqlCatalog::put(const std::string& path, Uri* uri) throw (DmException)
{
  return this->put(path, uri, std::string());
}



std::string DpmMySqlCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
  PoolManager* pm = this->stack_->getPoolManager();
  
  // Get the available pool list
  std::vector<Pool> pools = pm->getAvailablePools();
  
  // Pick a random one
  unsigned i = rand()  % pools.size();
  
  // Get the handler
  PoolHandler* handler = this->stack_->getPoolHandler(&pools[i]);
  
  // Create the entry
  this->create(path, 0777);
  
  // Delegate to it
  std::string token = handler->putLocation(path, uri);
  
  // Set the GUID
  if (!guid.empty())
    this->setGuid(path, guid);
  
  // Done!
  return token;
}



void DpmMySqlCatalog::putDone(const std::string& path, const Uri& uri, const std::string& token) throw (DmException)
{
  // Get the replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);
 
  // Pick the proper one
  for (unsigned i = 0; i < replicas.size(); ++i) {
    Uri replicaUri = dmlite::splitUri(replicas[i].url);
    
    if ((replicaUri.host[0] == '\0' || strcmp(replicaUri.host, uri.host) == 0) &&
        strcmp(replicaUri.path, uri.path) == 0) {
      Pool pool = this->stack_->getPoolManager()->getPool(replicas[i].pool);
      PoolHandler* handler = this->stack_->getPoolHandler(&pool);
      
      handler->putDone(path, uri, token);

      return;
    }
  }
  
  // :(
  throw DmException(DM_NO_SUCH_REPLICA,
                    "Replica %s for file %s not found", uri.path, path.c_str());
}



void DpmMySqlCatalog::unlink(const std::string& path) throw (DmException)
{
  // Stat without following
  ExtendedStat stat = this->extendedStat(path, false);

  switch (stat.stat.st_mode & S_IFMT) {
    case S_IFDIR:
      throw DmException(DM_IS_DIRECTORY, "Can not remove a directory");
      break;
    case S_IFLNK:
      NsMySqlCatalog::unlink(path);
      break;
    default:
      try {
        std::vector<FileReplica> replicas = this->getReplicas(stat.stat.st_ino);
        std::vector<FileReplica>::iterator i;
        
        for (i = replicas.begin(); i != replicas.end(); ++i) {
          // Delegate to the proper pool handler
          Pool pool = this->stack_->getPoolManager()->getPool(i->pool);
          PoolHandler* handler = this->stack_->getPoolHandler(&pool);
          handler->remove(path, *i);
          // Remove the replica itself
          this->deleteReplica(std::string(), stat.stat.st_ino, i->url);
        }
      }
      catch (DmException e) {
        if (e.code() != DM_NO_REPLICAS)
          throw;
      }
      NsMySqlCatalog::unlink(path);
  }
}



MySqlPoolManager::MySqlPoolManager(PoolContainer<MYSQL*>* connPool,
                                   const std::string& dpmDb,
                                   StackInstance* si) throw (DmException):
      connectionPool_(connPool), dpmDb_(dpmDb), stack_(si)
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
    PoolHandler* handler = this->stack_->getPoolHandler(&pools[i]);
    
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
