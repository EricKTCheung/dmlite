/// @file    plugins/mysql/MySqlFactories.cpp
/// @brief   MySQL backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include "MySqlFactories.h"
#include "NsMySql.h"
#include "DpmMySql.h"
#include "UserGroupDbMySql.h"

using namespace dmlite;



static pthread_once_t initialize_mysql_thread = PTHREAD_ONCE_INIT;
static pthread_key_t  destructor_key;

static void destroy_thread(void*)
{
  mysql_thread_end();
}

static void init_thread(void)
{
  mysql_thread_init();
  pthread_key_create(&destructor_key, destroy_thread);
}



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



NsMySqlFactory::NsMySqlFactory() throw(DmException):
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
  if (key == "MySqlHost")
    this->connectionFactory_.host = value;
  else if (key == "MySqlUsername")
    this->connectionFactory_.user = value;
  else if (key == "MySqlPassword")
    this->connectionFactory_.passwd = value;
  else if (key == "MySqlPort")
    this->connectionFactory_.port = atoi(value.c_str());
  else if (key == "NsDatabase")
    this->nsDb_ = value;
  else if (key == "NsPoolSize")
    this->connectionPool_.resize(atoi(value.c_str()));
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}


INode* NsMySqlFactory::createINode(StackInstance* si) throw(DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new INodeMySql(&this->connectionPool_, this->nsDb_);
}



UserGroupDb* NsMySqlFactory::createUserGroupDb(StackInstance* si) throw (DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new UserGroupDbMySql(&this->connectionPool_, this->nsDb_);
}



DpmMySqlFactory::DpmMySqlFactory() throw(DmException):
  connectionFactory_(std::string("localhost"), 0, std::string("root"), std::string()),
  connectionPool_(&connectionFactory_, 25), dpmDb_("dpm_db")
{
  // MySQL initialization done by NsMySqlFactory
}



DpmMySqlFactory::~DpmMySqlFactory() throw(DmException)
{
  // MySQL termination done by NsMySqlFactory
}



void DpmMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
if (key == "MySqlHost")
    this->connectionFactory_.host = value;
  else if (key == "MySqlUsername")
    this->connectionFactory_.user = value;
  else if (key == "MySqlPassword")
    this->connectionFactory_.passwd = value;
  else if (key == "MySqlPort")
    this->connectionFactory_.port = atoi(value.c_str());
  else if (key == "DpmDatabase")
    this->dpmDb_ = value;
  else
    throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
}



PoolManager* DpmMySqlFactory::createPoolManager(StackInstance* si) throw (DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new MySqlPoolManager(&this->connectionPool_,
                              this->dpmDb_, si);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  pm->registerFactory(static_cast<INodeFactory*>(new NsMySqlFactory()));
  pm->registerFactory(static_cast<UserGroupDbFactory*>(new NsMySqlFactory()));
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
  pm->registerFactory(static_cast<INodeFactory*>(new NsMySqlFactory()));
  pm->registerFactory(static_cast<UserGroupDbFactory*>(new NsMySqlFactory()));
  pm->registerFactory(new DpmMySqlFactory());
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
