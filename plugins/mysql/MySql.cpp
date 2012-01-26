/// @file    plugins/mysql/MySql.cpp
/// @brief   MySQL backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstring>
#include "MySql.h"
#include "NsMySql.h"
#include "DpmMySql.h"

using namespace dmlite;



MySqlConnectionFactory::MySqlConnectionFactory(const std::string& host, unsigned int port,
                                               const std::string& user, const std::string& passwd):
  host(host), port(port), user(user), passwd(passwd)
{
  // Nothing
}



MySqlConnectionFactory::~MySqlConnectionFactory()
{
  // Nothing
}



MYSQL* MySqlConnectionFactory::create()
{
  MYSQL*  c;
  my_bool reconnect  = 1;
  my_bool truncation = 0;
  
  c = mysql_init(NULL);

  mysql_options(c, MYSQL_OPT_RECONNECT,          &reconnect);
  mysql_options(c, MYSQL_REPORT_DATA_TRUNCATION, &truncation);

  if (mysql_real_connect(c, host.c_str(),
                         user.c_str(), passwd.c_str(),
                         NULL, port, NULL, 0) == NULL) {
    std::string err("Could not connect! ");
    err += mysql_error(c);
    mysql_close(c);
    throw DmException(DM_CONNECTION_ERROR, err);
  }
  return c;
}



void MySqlConnectionFactory::destroy(MYSQL* c)
{
  mysql_close(c);
}


bool MySqlConnectionFactory::isValid(MYSQL*)
{
  // Reconnect set to 1, so even if the connection dropped,
  // it will reconnect.
  return true;
}



TransactionAndLock::TransactionAndLock(MYSQL* conn, ...) throw (DmException)
{
  this->connection = conn;
  this->pending    = true;

  std::string query("LOCK TABLES");
  va_list tables;
  va_start(tables, conn);

  const char* table;
  while ((table = va_arg(tables, const char*)) != NULL) {
    query += " ";
    query += table;
    query += " WRITE,";
  }

  va_end(tables);

  // Remove last ','
  query[query.length() - 1] = '\0';

  if (mysql_query(conn, query.c_str()) != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(conn));
  if (mysql_query(conn, "BEGIN") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(conn));
}



TransactionAndLock::~TransactionAndLock() throw (DmException)
{
  if (this->pending)
    this->rollback();
  if (mysql_query(this->connection, "UNLOCK TABLES") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->connection));
}



void TransactionAndLock::commit() throw (DmException)
{
  if (mysql_query(this->connection, "COMMIT") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->connection));
  this->pending = false;
}



void TransactionAndLock::rollback() throw (DmException)
{
  if (mysql_query(this->connection, "ROLLBACK") != 0)
    throw DmException(DM_QUERY_FAILED, mysql_error(this->connection));
  this->pending = false;
}



NsMySqlFactory::NsMySqlFactory(CatalogFactory* catalogFactory) throw(DmException):
  nestedFactory_(catalogFactory),
  connectionFactory_(std::string("localhost"), 0, std::string("root"), std::string()),
  connectionPool_(&connectionFactory_, 25), nsDb_("cns_db")
{
  // Initialize MySQL library
  mysql_library_init(0, NULL, NULL);
}



NsMySqlFactory::~NsMySqlFactory() throw(DmException)
{
  // Close MySQL library
  mysql_library_end();
}



void NsMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "Host")
    this->connectionFactory_.host = value;
  else if (key == "MySqlUsername")
    this->connectionFactory_.user = value;
  else if (key == "MySqlPassword")
    this->connectionFactory_.passwd = value;
  else if (key == "MySqlPort")
    this->connectionFactory_.port = atoi(value.c_str());
  else if (key == "NsDatabase")
    this->nsDb_ = value;
  else if (key == "SymLinkLimit")
    this->symLinkLimit_ = atoi(value.c_str());
  else if (key == "NsPoolSize")
    this->connectionPool_.resize(atoi(value.c_str()));
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}


Catalog* NsMySqlFactory::createCatalog() throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nestedFactory_ != 0x00)
    nested = this->nestedFactory_->createCatalog();

  return new NsMySqlCatalog(&this->connectionPool_, this->nsDb_,
                            nested, this->symLinkLimit_);
}



DpmMySqlFactory::DpmMySqlFactory(CatalogFactory* catalogFactory) throw(DmException):
                  NsMySqlFactory(catalogFactory), dpmDb_("dpm_db")
{
  // MySQL initialization done by NsMySqlFactory
}



DpmMySqlFactory::~DpmMySqlFactory() throw(DmException)
{
  // MySQL termination done by NsMySqlFactory
}



void DpmMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "DpmDatabase")
    this->dpmDb_ = value;
  else
    return NsMySqlFactory::configure(key, value);
}



Catalog* DpmMySqlFactory::createCatalog() throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nestedFactory_ != 0x00)
    nested = this->nestedFactory_->createCatalog();

  return new DpmMySqlCatalog(&this->connectionPool_,
                             this->nsDb_, this->dpmDb_,
                             nested, this->symLinkLimit_);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nested = 0x00;
  try {
    nested = pm->getCatalogFactory();
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }
  pm->registerCatalogFactory(new NsMySqlFactory(nested));
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  CatalogFactory* nested = 0x00;
  try {
    nested = pm->getCatalogFactory();
  }
  catch (DmException e) {
    if (e.code() != DM_NO_FACTORY)
      throw;
  }
  pm->registerCatalogFactory(new DpmMySqlFactory(nested));
}



/// This is what the PluginManager looks for
PluginIdCard plugin_mysql_ns = {
  API_VERSION,
  registerPluginNs
};

PluginIdCard plugin_mysql_dpm = {
  API_VERSION,
  registerPluginDpm
};
