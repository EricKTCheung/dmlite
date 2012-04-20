/// @file    plugins/mysql/DpmMySql.cpp

#include "dmlite/dmlite++.h"

/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "DpmMySql.h"
#include "Queries.h"

using namespace dmlite;



DpmMySqlCatalog::DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& nsDb,
                                 const std::string& dpmDb, Catalog* decorates,
                                 unsigned int symLinkLimit, StackInstance* si) throw(DmException):
  NsMySqlCatalog(connPool, nsDb, symLinkLimit), dpmDb_(dpmDb),
  decorated_(decorates), stack_(si)
{
  // Nothing
}



DpmMySqlCatalog::~DpmMySqlCatalog() throw(DmException)
{
  if (this->decorated_ != 0x00)
    delete this->decorated_;
}



std::string DpmMySqlCatalog::getImplId() throw ()
{
  return std::string("DpmMySqlCatalog");
}



void DpmMySqlCatalog::set(const std::string& key, va_list varg) throw (DmException)
{
  if (this->decorated_ != 0x00) {
    this->decorated_->set(key, varg);
  }
  else {
    NsMySqlCatalog::set(key, varg);
  }
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
    PoolHandler* handler = this->stack_->createPoolHandler(&pool);

    if (handler->replicaAvailable(path, replicas[i])) {
      available.push_back(handler->getPhysicalLocation(path, replicas[i]));
    }

    delete handler;
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
  if (this->decorated_ == 0x00)
    throw DmException(DM_NO_CATALOG, "DpmMySqlCatalog::put Can not delegate");
  return this->decorated_->put(path, uri);
}



std::string DpmMySqlCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
  if (this->decorated_ == 0x00)
    throw DmException(DM_NO_CATALOG, "DpmMySqlCatalog::put Can not delegate");
  
  // Try to delegate
  try {
    return this->decorated_->put(path, uri, guid);
  }
  catch (DmException e) {
    // If not implemented, we take over, thanks
    if (e.code() != DM_NOT_IMPLEMENTED)
      throw;
  }

  // Do the regular PUT
  std::string token = this->decorated_->put(path, uri);
  
  // The underlying layer probably created the entry already (DPM backend)
  try {
    this->setGuid(path, guid);
  }
  catch (DmException e) {
    // It may be it doesn't work that way :(
    // Just ignore it, although the GUID won't be there
    // (Is this right?)
    if (e.code() != DM_NO_SUCH_FILE)
      throw;
  } 

  return token;
}



void DpmMySqlCatalog::putStatus(const std::string& path, const std::string& token, Uri* uri) throw (DmException)
{
  if (this->decorated_ == 0x00)
    throw DmException(DM_NO_CATALOG, "DpmMySqlCatalog::putStatus Can not delegate");
  this->decorated_->putStatus(path, token, uri);
}



void DpmMySqlCatalog::putDone(const std::string& path, const std::string& token) throw (DmException)
{
  if (this->decorated_ == 0x00)
    throw DmException(DM_NO_CATALOG, "DpmMySqlCatalog::putDone Can not delegate");
  this->decorated_->putDone(path, token);
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
        this->getReplicas(path);
        if (this->decorated_ == 0x00)
          throw DmException(DM_NO_CATALOG, "DpmMySqlCatalog::unlink Can not delegate");
        this->decorated_->unlink(path);
      }
      catch (DmException e) {
        if (e.code() == DM_NO_REPLICAS)
          NsMySqlCatalog::unlink(path);
        else
          throw;
      }
  }
}



MySqlPoolManager::MySqlPoolManager(PoolContainer<MYSQL*>* connPool, const std::string& dpmDb) throw (DmException):
      connectionPool_(connPool), dpmDb_(dpmDb)
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
  Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOLS);

  stmt.execute();

  stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
  stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));

  std::vector<Pool> pools;
  while (stmt.fetch())
    pools.push_back(pool);

  return pools;
}



Pool MySqlPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  Pool pool;
  Statement stmt(this->conn_, this->dpmDb_, STMT_GET_POOL);

  stmt.bindParam(0, poolname);

  stmt.execute();

  stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
  stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));
  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_POOL, poolname + " not found");

  return pool;
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
