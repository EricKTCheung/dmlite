/// @file   MySqlFactories.cpp
/// @brief  MySQL backend for libdm.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include "MySqlFactories.h"
#include "NsMySql.h"
#include "DpmMySql.h"
#include "AuthnMySql.h"

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
                         NULL, port, NULL, CLIENT_FOUND_ROWS) == NULL) {
    std::string err("Could not connect! ");
    err += mysql_error(c);
    mysql_close(c);
    throw DmException(DMLITE_DBERR(ECOMM), err);
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
  connectionPool_(&connectionFactory_, 25), nsDb_("cns_db"),
  mapFile_("/etc/lcgdm-mapfile"), hostDnIsRoot_(false), hostDn_("")
{
  mysql_library_init(0, NULL, NULL);  
  pthread_key_create(&this->thread_mysql_conn_, NULL);
}



NsMySqlFactory::~NsMySqlFactory()
{
  mysql_library_end();
  pthread_key_delete(this->thread_mysql_conn_);
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
  else if (key == "NsPoolSize") {
      int i = atoi(value.c_str());
      this->connectionPool_.resize(i);
    }
  else if (key == "MapFile")
    this->mapFile_ = value;
  else if (key == "HostDNIsRoot")
    this->hostDnIsRoot_ = (value != "no");
  else if (key == "HostCertificate")
    this->hostDn_ = getCertificateSubject(value);
  else
    throw DmException(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
                      std::string("Unknown option ") + key);
}


INode* NsMySqlFactory::createINode(PluginManager*) throw(DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new INodeMySql(this, this->nsDb_);
}



Authn* NsMySqlFactory::createAuthn(PluginManager*) throw (DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new AuthnMySql(this, this->nsDb_, this->mapFile_,
                        this->hostDnIsRoot_, this->hostDn_);
}



PoolContainer<MYSQL*>& NsMySqlFactory::getPool(void) throw ()
{
  return this->connectionPool_;
}



DpmMySqlFactory::DpmMySqlFactory() throw(DmException):
  NsMySqlFactory(), dpmDb_("dpm_db"), adminUsername_("root")
{
  // MySQL initialization done by NsMySqlFactory
}



DpmMySqlFactory::~DpmMySqlFactory()
{
  // MySQL termination done by NsMySqlFactory
}



void DpmMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  if (key == "DpmDatabase")
    this->dpmDb_ = value;
  else if (key == "AdminUsername")
    this->adminUsername_ = value;
  else
    NsMySqlFactory::configure(key, value);
}



PoolManager* DpmMySqlFactory::createPoolManager(PluginManager*) throw (DmException)
{
  pthread_once(&initialize_mysql_thread, init_thread);
  return new MySqlPoolManager(this, this->dpmDb_, this->adminUsername_);
}



static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  NsMySqlFactory* nsFactory = new NsMySqlFactory();
  pm->registerINodeFactory(nsFactory);
  pm->registerAuthnFactory(nsFactory);
}



static void registerPluginDpm(PluginManager* pm) throw(DmException)
{ 
  DpmMySqlFactory* dpmFactory = new DpmMySqlFactory();
  
  pm->registerINodeFactory(dpmFactory);
  pm->registerAuthnFactory(dpmFactory);
  pm->registerPoolManagerFactory(dpmFactory);
}



/// This is what the PluginManager looks for
PluginIdCard plugin_mysql_ns = {
  PLUGIN_ID_HEADER,
  registerPluginNs
};

PluginIdCard plugin_mysql_dpm = {
  PLUGIN_ID_HEADER,
  registerPluginDpm
};
