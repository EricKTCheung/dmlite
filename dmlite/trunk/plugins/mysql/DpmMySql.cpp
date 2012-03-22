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
  NsMySqlCatalog(connPool, nsDb, symLinkLimit), dpmDb_(dpmDb),
  preparedStmt_(STMT_SENTINEL, 0x00), decorated_(decorates)
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



FileReplica DpmMySqlCatalog::get(const std::string& path) throw(DmException)
{
  // Get replicas
  std::vector<FileReplica> replicas = this->getReplicas(path);

  // Get the status for the Fs+Pool+Server for each replica
  std::vector<int> available;

  // Iterate and mark as unavailable if the FS is unavailable
  unsigned i;
  for (i = 0; i < replicas.size(); ++i) {
    if (this->getFsStatus(replicas[i].pool,
                          replicas[i].server,
                          replicas[i].filesystem) != FS_DISABLED)
      available.push_back(i);
  }

  if (available.size() > 0) {
    // Pick a random one from the available
    i = rand() % available.size();
    return replicas[i];
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



int DpmMySqlCatalog::getFsStatus(const std::string& pool, const std::string& server,
                                 const std::string& fs) throw(DmException)
{
  Statement stmt(this->getPreparedStatement(STMT_GET_FS_STATUS));
  int       status;

  // Bind the parameters
  stmt.bindParam(0, pool);
  stmt.bindParam(1, server);
  stmt.bindParam(2, fs);

  stmt.execute();
  
  stmt.bindResult(0, &status);

  if (!stmt.fetch())
    throw DmException(DM_INTERNAL_ERROR, "%s:%s:%s not found!", pool.c_str(), server.c_str(), fs.c_str());

  return status;
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
