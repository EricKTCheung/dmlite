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



NsMySqlFactory::NsMySqlFactory(CatalogFactory* catalogFactory) throw(DmException):
  DummyFactory(catalogFactory),
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



void NsMySqlFactory::set(const std::string& key, const std::string& value) throw(DmException)
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


Catalog* NsMySqlFactory::create() throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nested_factory_ != 0x00)
    nested = this->nested_factory_->create();

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



void DpmMySqlFactory::set(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "DpmDatabase")
    this->dpmDb_ = value;
  else
    return NsMySqlFactory::set(key, value);
}



Catalog* DpmMySqlFactory::create() throw(DmException)
{
  Catalog* nested = 0x00;

  if (this->nested_factory_ != 0x00)
    nested = this->nested_factory_->create();

  return new DpmMySqlCatalog(&this->connectionPool_,
                             this->nsDb_, this->dpmDb_,
                             nested, this->symLinkLimit_);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new NsMySqlFactory(pm->getCatalogFactory()));
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{
  pm->registerCatalogFactory(new DpmMySqlFactory(pm->getCatalogFactory()));
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