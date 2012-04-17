/// @file    plugins/mysql/DpmMySql.cpp

#include "dmlite/dmlite++.h"

/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <vector>
#include <stdlib.h>
#include <string.h>

#include "DpmMySql.h"

using namespace dmlite;



/// Used to keep prepared statements
enum {
  STMT_GET_POOLS = 0,
  STMT_GET_POOL,
  STMT_SENTINEL
};

/// Used internally to define prepared statements.
/// Must match with STMT_* constants!
static const char* statements[] = {
  "SELECT poolname, COALESCE(pooltype, 'filesystem')\
        FROM dpm_pool",
  "SELECT poolname, COALESCE(pooltype, 'filesystem')\
        FROM dpm_pool\
        where poolname = ?",
};



MYSQL_STMT* DpmMySqlCatalog::getPreparedStatement(unsigned stId)
{
  if (mysql_select_db(this->conn_, this->dpmDb_.c_str()) != 0)
    throw DmException(DM_QUERY_FAILED, std::string(mysql_error(this->conn_)));

  MYSQL_STMT* stmt = mysql_stmt_init(this->conn_);
  if (mysql_stmt_prepare(stmt, statements[stId], strlen(statements[stId])) != 0) {
      throw DmException(DM_QUERY_FAILED, std::string(mysql_stmt_error(stmt)));
  }

  return stmt;
}



DpmMySqlCatalog::DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& nsDb,
                                 const std::string& dpmDb, Catalog* decorates,
                                 unsigned int symLinkLimit, PluginManager* pm) throw(DmException):
  NsMySqlCatalog(connPool, nsDb, symLinkLimit), dpmDb_(dpmDb),
  decorated_(decorates), pluginManager_(pm)
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



void DpmMySqlCatalog::setSecurityCredentials(const SecurityCredentials& cred) throw (DmException)
{
  if (this->decorated_ != 0x00)
    this->decorated_->setSecurityCredentials(cred);
  NsMySqlCatalog::setSecurityCredentials(cred);
}



void DpmMySqlCatalog::setSecurityContext(const SecurityContext& ctx)
{
  if (this->decorated_ != 0x00)
    this->decorated_->setSecurityContext(ctx);
  NsMySqlCatalog::setSecurityContext(ctx);
}



const SecurityContext& DpmMySqlCatalog::getSecurityContext(void) throw (DmException)
{
  return NsMySqlCatalog::getSecurityContext();
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
    Pool pool = this->getPool(replicas[i].pool);
    PoolHandler* handler = this->pluginManager_->getPoolHandler(&pool);

    if (handler->replicaAvailable(replicas[i])) {
      available.push_back(handler->getPhysicalLocation(replicas[i]));
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



std::vector<Pool> DpmMySqlCatalog::getPools() throw (DmException)
{
  Pool pool;
  Statement stmt(this->getPreparedStatement(STMT_GET_POOLS));

  stmt.execute();

  stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
  stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));

  std::vector<Pool> pools;
  while (stmt.fetch())
    pools.push_back(pool);

  return pools;
}



Pool DpmMySqlCatalog::getPool(const std::string& poolname) throw (DmException)
{
  Pool pool;
  Statement stmt(this->getPreparedStatement(STMT_GET_POOL));

  stmt.bindParam(0, poolname);

  stmt.execute();

  stmt.bindResult(0, pool.pool_name, sizeof(pool.pool_name));
  stmt.bindResult(1, pool.pool_type, sizeof(pool.pool_type));
  if (!stmt.fetch())
    throw DmException(DM_NO_SUCH_POOL, poolname + " not found");

  return pool;
}
