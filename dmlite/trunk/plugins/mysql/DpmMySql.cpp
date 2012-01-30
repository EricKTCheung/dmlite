/// @file    plugins/mysql/DpmMySql.cpp
/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <vector>
#include <stdlib.h>
#include <string.h>

#include "DpmMySql.h"

using namespace dmlite;



/// Used to keep prepared statements
enum {
  STMT_GET_FS_STATUS = 0,
  STMT_SENTINEL
};

/// Used internally to define prepared statements.
/// Must match with STMT_* constants!
static const char* statements[] = {
  "SELECT status\
        FROM dpm_fs\
        WHERE poolname = ? AND server = ? AND fs = ?",
};



MYSQL_STMT* DpmMySqlCatalog::getPreparedStatement(unsigned stId)
{
  if (this->preparedStmt_[stId] == 0x00) {

    if (mysql_select_db(this->conn_, this->dpmDb_.c_str()) != 0)
      throw DmException(DM_QUERY_FAILED, std::string(mysql_error(this->conn_)));

    this->preparedStmt_[stId] = mysql_stmt_init(this->conn_);
    if (mysql_stmt_prepare(this->preparedStmt_[stId],
                           statements[stId], strlen(statements[stId])) != 0) {
      throw DmException(DM_QUERY_FAILED, std::string(mysql_stmt_error(this->preparedStmt_[stId])));
    }
  }

  return this->preparedStmt_[stId];
}



DpmMySqlCatalog::DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool, const std::string& nsDb,
                                 const std::string& dpmDb, Catalog* decorates,
                                 unsigned int symLinkLimit) throw(DmException):
  NsMySqlCatalog(connPool, nsDb, decorates, symLinkLimit), dpmDb_(dpmDb),
  preparedStmt_(STMT_SENTINEL, 0x00)
{
  // Nothing
}



DpmMySqlCatalog::~DpmMySqlCatalog() throw(DmException)
{
  std::vector<MYSQL_STMT*>::iterator i;

  for (i = this->preparedStmt_.begin(); i != this->preparedStmt_.end(); i++) {
    if (*i != 0x00)
      mysql_stmt_close(*i);
  }
}



std::string DpmMySqlCatalog::getImplId() throw ()
{
  return std::string("DpmMySqlCatalog");
}



FileReplica DpmMySqlCatalog::get(const std::string& path) throw(DmException)
{
  // Get replicas
  std::vector<ExtendedReplica> replicas = this->getExReplicas(path);

  // Get the status for the Fs+Pool+Server for each replica
  std::vector<int> available;

  // Iterate and mark as unavailable if the FS is unavailable
  unsigned i;
  for (i = 0; i < replicas.size(); ++i) {
    if (this->getFsStatus(replicas[i].pool,
                          replicas[i].host,
                          replicas[i].fs) != FS_DISABLED)
      available.push_back(i);
  }

  if (available.size() > 0) {
    // Pick a random one from the available
    i = rand() % available.size();
    return replicas[i].replica;
  }
  else {
    throw DmException(DM_NO_REPLICAS, "No available replicas");
  }
}

std::string DpmMySqlCatalog::put(const std::string& path, Uri* uri, const std::string& guid) throw (DmException)
{
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

int DpmMySqlCatalog::getFsStatus(const std::string& pool, const std::string& server,
                                 const std::string& fs) throw(DmException)
{
  MYSQL_STMT *stmt;
  MYSQL_BIND  bindParam[3], bindResult[1];
  int         status;
  my_bool     isNull;
  
  long unsigned poolLen, serverLen, fsLen;

  stmt = this->getPreparedStatement(STMT_GET_FS_STATUS);

  // Bind the parameters
  memset(bindParam, 0, sizeof(bindParam));

  poolLen   = pool.size();
  serverLen = server.size();
  fsLen     = fs.size();

  bindParam[0].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[0].buffer      = (void*)pool.c_str();
  bindParam[0].length      = &poolLen;

  bindParam[1].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[1].buffer      = (void*)server.c_str();
  bindParam[1].length      = &serverLen;

  bindParam[2].buffer_type = MYSQL_TYPE_VARCHAR;
  bindParam[2].buffer      = (void*)fs.c_str();
  bindParam[2].length      = &fsLen;

  mysql_stmt_bind_param(stmt, bindParam);
  
  // Execute query
  if (mysql_stmt_execute(stmt) != 0) {
    throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));
    return -1;
  }

  // Bind the results
  memset(bindResult, 0, sizeof(bindResult));

  bindResult[0].buffer_type = MYSQL_TYPE_LONG;
  bindResult[0].buffer      = &status;
  bindResult[0].is_null     = &isNull;

  mysql_stmt_bind_result(stmt, bindResult);

  // Fetch
  try {
    switch (mysql_stmt_fetch(stmt)) {
      case 0:
        break;
      case MYSQL_NO_DATA:
        throw DmException(DM_INTERNAL_ERROR, "%s:%s:%s not found!", pool.c_str(), server.c_str(), fs.c_str());
      case MYSQL_DATA_TRUNCATED:
        throw DmException(DM_QUERY_FAILED, "Data truncated");
      default:
        throw DmException(DM_QUERY_FAILED, mysql_stmt_error(stmt));
    }

    mysql_stmt_free_result(stmt);

    // And done
    return status;
  }
  catch (...) {
    mysql_stmt_free_result(stmt);
    throw;
  }
}
