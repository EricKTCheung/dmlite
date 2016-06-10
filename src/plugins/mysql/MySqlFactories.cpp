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





// --------------------------------------------------------
// NsMySqlFactory
//



NsMySqlFactory::NsMySqlFactory() throw(DmException):
  nsDb_("cns_db"),
  mapFile_("/etc/lcgdm-mapfile"), hostDnIsRoot_(false), hostDn_("")
{
  dirspacereportdepth = 6;
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
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, mysqllogmask, mysqllogname, key, value);
  
  if (key == "MapFile")
    this->mapFile_ = value;
  else if (key == "HostDNIsRoot")
    this->hostDnIsRoot_ = (value != "no");
  else if (key == "HostCertificate")
    this->hostDn_ = getCertificateSubject(value);
  else if (key == "NsDatabase")
    this->nsDb_ = value; 
  else if (key == "MySqlDirectorySpaceReportDepth")
    this->dirspacereportdepth = atoi(value.c_str());
  else
    gotit = MySqlHolder::configure(key, value);
  
  if (gotit)
    LogCfgParm(Logger::Lvl1,  mysqllogmask, mysqllogname, key, value);
  
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

  dirspacereportdepth = 6;
  Log(Logger::Lvl3, mysqllogmask, mysqllogmask, "MysqlIOPassthroughFactory started.");
  
}



void MysqlIOPassthroughFactory::configure(const std::string& key, const std::string& value) throw(DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Key: " << key << " Value: " << value);
  bool gotit = true;
  
  
  if (key == "MySqlDirectorySpaceReportDepth")
    this->dirspacereportdepth = atoi(value.c_str());
  else gotit = false;
  
  if (gotit)
    Log(Logger::Lvl0, mysqllogmask, mysqllogname, "Setting mysql parms. Key: " << key << " Value: " << value);
    
  MySqlHolder::configure(key, value);
}

IODriver* MysqlIOPassthroughFactory::createIODriver(PluginManager* pm) throw (DmException) {
  IODriver *nested;
  if (this->nestedIODriverFactory_ != 0x00)
    nested = IODriverFactory::createIODriver(this->nestedIODriverFactory_, pm);
  else
    return 0x00;
  
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Creating mysql passthrough IODriver");
  
  
  return new MysqlIOPassthroughDriver(nested, this->dirspacereportdepth);
  
}
  
  
  
  
  
  
// -------------------------------------------------------
// Static functions implementing hooks for the dynamic loading
//

// Register plugin item that gives name space functionalities together with authentication
static void registerPluginNs(PluginManager* pm) throw(DmException)
{
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "registerPluginNs");
  NsMySqlFactory* nsFactory = new NsMySqlFactory();
  pm->registerINodeFactory(nsFactory);
  pm->registerAuthnFactory(nsFactory);
}

// Register plugin item that gives name space functionalities only
static void registerPluginNsOnly(PluginManager* pm) throw(DmException)
{
  mysqllogmask = Logger::get()->getMask(mysqllogname);
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "registerPluginNsOnly");
  NsMySqlFactory* nsFactory = new NsMySqlFactory();
  pm->registerINodeFactory(nsFactory);

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

PluginIdCard plugin_mysql_nsonly = {
  PLUGIN_ID_HEADER,
  registerPluginNsOnly
};

PluginIdCard plugin_mysql_dpm = {
  PLUGIN_ID_HEADER,
  registerPluginDpm
};

PluginIdCard plugin_mysql_iopassthrough = {
  PLUGIN_ID_HEADER,
  registerPluginMysqlIOPassthrough
};
