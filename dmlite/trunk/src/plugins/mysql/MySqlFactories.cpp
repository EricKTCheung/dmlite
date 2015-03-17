/// @file   MySqlFactories.cpp
/// @brief  MySQL backend for libdm.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>

#ifdef __APPLE__
#include <bsm/audit_errno.h>
#endif

#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <stdlib.h>
#include "MySqlFactories.h"
#include "NsMySql.h"
#include "DpmMySql.h"
#include "AuthnMySql.h"
#include "utils/logger.h"
#include "MySqlIO.h"

using namespace dmlite;

// Logger stuff
Logger::bitmask dmlite::mysqllogmask = ~0;
Logger::component dmlite::mysqllogname = "Mysql";





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

//------------------------------------------
// MySqlHolder
// Holder of mysql connections, base class singleton holding the mysql conn pool
//

// Static mysql-related stuff
PoolContainer<MYSQL*> *MySqlHolder::connectionPool_ = 0;
MySqlHolder *MySqlHolder::instance = 0;

MySqlHolder *MySqlHolder::getInstance() {
  if (!instance) {
    instance = new MySqlHolder();
  }
  return instance;
}

MySqlHolder::MySqlHolder() {
    mysql_library_init(0, NULL, NULL);  
    poolsize = 0;
    connectionPool_ = 0;
}

MySqlHolder::~MySqlHolder() {
  if (connectionPool_) delete connectionPool_;
  poolsize = 0;
  connectionPool_ = 0;

}


PoolContainer<MYSQL*> &MySqlHolder::getMySqlPool()  throw(DmException){

  MySqlHolder *h = getInstance();
  
  if (!h->connectionPool_) {
    Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Creating MySQL connection pool" << 
	h->connectionFactory_.user << "@" << h->connectionFactory_.host << ":" <<h->connectionFactory_.port <<
        " size: " << h->poolsize);
    h->connectionPool_ = new PoolContainer<MYSQL*>(&h->connectionFactory_, h->poolsize);
  }
  
  return *(h->connectionPool_);
}

bool MySqlHolder::configure(const std::string& key, const std::string& value) {
  MySqlHolder *h = getInstance();
  bool gotit = true;
  
  
  if (key == "MySqlHost")
    h->connectionFactory_.host = value;
  else if (key == "MySqlUsername")
    h->connectionFactory_.user = value;
  else if (key == "MySqlPassword")
    h->connectionFactory_.passwd = value;
  else if (key == "MySqlPort")
    h->connectionFactory_.port = atoi(value.c_str());
  else if (key == "NsPoolSize") {
    int n = atoi(value.c_str());
      h->poolsize = (n < h->poolsize ? h->poolsize : n);
      if (h->connectionPool_)
	h->connectionPool_->resize(h->poolsize);
    }
  else gotit = false;
  
  if (gotit)
    Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Setting mysql parms. Key: " << key << " Value: " << value);
    
  return gotit;
}

// -----------------------------------------
// MySqlConnectionFactory
//
MySqlConnectionFactory::MySqlConnectionFactory() {
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "MySqlConnectionFactory started");
  // Nothing
}


MYSQL* MySqlConnectionFactory::create()
{
  MYSQL*  c;
  my_bool reconnect  = 1;
  my_bool truncation = 0;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Connecting... " << user << "@" << host << ":" << port);
  
  c = mysql_init(NULL);

  mysql_options(c, MYSQL_OPT_RECONNECT,          &reconnect);
  mysql_options(c, MYSQL_REPORT_DATA_TRUNCATION, &truncation);

  if (mysql_real_connect(c, host.c_str(),
                         user.c_str(), passwd.c_str(),
                         NULL, port, NULL, CLIENT_FOUND_ROWS) == NULL) {
    std::string err("Could not connect! ");
    err += mysql_error(c);
    mysql_close(c);
#ifdef __APPLE__
    throw DmException(DMLITE_DBERR(BSM_ERRNO_ECOMM), err);
#else
    throw DmException(DMLITE_DBERR(ECOMM), err);
#endif
  }
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Connected. " << user << "@" << host << ":" << port);
  return c;
}

void MySqlConnectionFactory::destroy(MYSQL* c)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Destroying... ");
  mysql_close(c);
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Destroyed. ");
}

bool MySqlConnectionFactory::isValid(MYSQL*)
{
  // Reconnect set to 1, so even if the connection dropped,
  // it will reconnect.
  return true;
}






// --------------------------------------------------------
// NsMySqlFactory
//



NsMySqlFactory::NsMySqlFactory() throw(DmException):
  nsDb_("cns_db"),
  mapFile_("/etc/lcgdm-mapfile"), hostDnIsRoot_(false), hostDn_("")
{
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "NsMySqlFactory started");
}




NsMySqlFactory::~NsMySqlFactory()
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  mysql_library_end();
  
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting.");
}



void NsMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Key: " << key << " Value: " << value);
  
  if (key == "MapFile")
    this->mapFile_ = value;
  else if (key == "HostDNIsRoot")
    this->hostDnIsRoot_ = (value != "no");
  else if (key == "HostCertificate")
    this->hostDn_ = getCertificateSubject(value);
  else if (key == "NsDatabase")
    this->nsDb_ = value;
  else
    MySqlHolder::configure(key, value);
  
}


INode* NsMySqlFactory::createINode(PluginManager*) throw(DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  pthread_once(&initialize_mysql_thread, init_thread);
  return new INodeMySql(this, this->nsDb_);
}

Authn* NsMySqlFactory::createAuthn(PluginManager*) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  pthread_once(&initialize_mysql_thread, init_thread);
  return new AuthnMySql(this, this->nsDb_, this->mapFile_,
                        this->hostDnIsRoot_, this->hostDn_);
}









// ------------------------------------------------
// DpmMySqlFactory
//



DpmMySqlFactory::DpmMySqlFactory() throw(DmException):
  NsMySqlFactory(), dpmDb_("dpm_db"), adminUsername_("root")
{
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "DpmMySqlFactory started");
}

DpmMySqlFactory::~DpmMySqlFactory()
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  // MySQL termination done by NsMySqlFactory
}

void DpmMySqlFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Key: " << key << " Value: " << value);
  
  if (key == "DpmDatabase")
    this->dpmDb_ = value;
  else if (key == "AdminUsername")
    this->adminUsername_ = value;
  else
    NsMySqlFactory::configure(key, value);
}

PoolManager* DpmMySqlFactory::createPoolManager(PluginManager*) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  pthread_once(&initialize_mysql_thread, init_thread);
  return new MySqlPoolManager(this, this->dpmDb_, this->adminUsername_);
}






// ------------------------------------------------
// MysqlIOPassthroughFactory
//

MysqlIOPassthroughFactory::MysqlIOPassthroughFactory(IODriverFactory *ioFactory) throw(DmException) {

  this->nestedIODriverFactory_    = ioFactory;

  Log(Logger::Lvl3, mysqllogmask, mysqllogmask, "MysqlIOPassthroughFactory started.");
  
}



void MysqlIOPassthroughFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Key: " << key << " Value: " << value);
  
  MySqlHolder::configure(key, value);
}

IODriver* MysqlIOPassthroughFactory::createIODriver(PluginManager* pm) throw (DmException) {
  IODriver *nested;
  if (this->nestedIODriverFactory_ != 0x00)
    nested = IODriverFactory::createIODriver(this->nestedIODriverFactory_, pm);
  else
    return 0x00;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Creating mysql passthrough IODriver");
  
  return new MysqlIOPassthroughDriver(nested);
  
}
  
  
  
  
  
  
// -------------------------------------------------------
// Static functions implementing hooks for the dynamic loading
//

// Register plugin item that gives name space functionalities
static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "registerPluginNs");
  NsMySqlFactory* nsFactory = new NsMySqlFactory();
  pm->registerINodeFactory(nsFactory);
  pm->registerAuthnFactory(nsFactory);
}


// Register plugin item that gives disk pool manager functionalities
static void registerPluginDpm(PluginManager* pm) throw(DmException)
{ 
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "registerPluginDpm");
  
  DpmMySqlFactory* dpmFactory = new DpmMySqlFactory();
  
  pm->registerINodeFactory(dpmFactory);
  pm->registerAuthnFactory(dpmFactory);
  pm->registerPoolManagerFactory(dpmFactory);
}

// Register plugin item that gives IO stack passthrough functionalities
// e.g. to intercept donewriting events and perform actions
static void registerPluginMysqlIOPassthrough(PluginManager* pm) throw(DmException)
{
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "registerPluginMysqlIOPassthrough");
  MysqlIOPassthroughFactory *pf = new MysqlIOPassthroughFactory(pm->getIODriverFactory());
  
  pm->registerIODriverFactory(pf);
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

PluginIdCard plugin_mysql_iopassthrough = {
  PLUGIN_ID_HEADER,
  registerPluginMysqlIOPassthrough
};
