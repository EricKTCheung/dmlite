/// @file   Profiler.cpp
/// @brief  Profiler plugin.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <fstream>
#include <iostream>

#include "Profiler.h"




Logger::bitmask dmlite::profilerlogmask = 0;
Logger::component dmlite::profilerlogname = "Profiler";
Logger::bitmask dmlite::profilertimingslogmask = 0;
Logger::component dmlite::profilertimingslogname = "ProfilerTimings";


using namespace dmlite;

ProfilerFactory::ProfilerFactory(CatalogFactory* catalogFactory,
                                 PoolManagerFactory* poolManagerFactory,
                                 IODriverFactory *ioFactory) throw (DmException)
{
  this->nestedCatalogFactory_     = catalogFactory;
  this->nestedPoolManagerFactory_ = poolManagerFactory;
  this->nestedIODriverFactory_    = ioFactory;

  profilerlogmask = Logger::get()->getMask(profilerlogname);
  profilertimingslogmask = Logger::get()->getMask(profilertimingslogname);
  Log(Logger::Lvl0, profilerlogmask, profilerlogname, "ProfilerFactory started.");
  
}



ProfilerFactory::~ProfilerFactory()
{
  // Nothing
}



void ProfilerFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  bool gotit = true;
  
  LogCfgParm(Logger::Lvl4, profilerlogmask, profilerlogname, key, value);
  
  // the monitor keyword accepts options in the xrootd syntax,
  // i.e. all parameters in one line separated by whitespace
  if (key == "monitor") {
    std::vector<std::string> options;
    boost::split(options, value, boost::is_any_of(" \t"));

    std::vector<std::string>::const_iterator it;
    for (it = options.begin(); it != options.end(); ++it) {
      if (*it == "lfn") {
        XrdMonitor::include_lfn_ = true;
      }
      else if (*it == "rbuff") {
        if (it+1 == options.end())
          break;
        int buf_size = atoi((++it)->c_str());
        if (buf_size > 0) {
          XrdMonitor::redir_max_buffer_size_ = buf_size;
        }
      }
      else if (*it == "dest") {
        if (it+1 == options.end())
          break;
        XrdMonitor::collector_addr_list.insert(*(++it));
      }
      else if (*it == "ops") {
        XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasOPS;
      }
      else if (*it == "ssq") {
        XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasOPS;
        XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasSSQ;
      }
      else if (*it == "auth") {
        XrdMonitor::include_auth_ = true;
        XrdMonitor::include_dn_ = true;
      }
    }
  } else if (key == "Collector") {
    XrdMonitor::collector_addr_list.insert(value);
  } else if (key == "MsgBufferSize") {
    XrdMonitor::redir_max_buffer_size_ = atoi(value.c_str());
    XrdMonitor::file_max_buffer_size_ = atoi(value.c_str());
  } else if (key == "SendLFN") {
    XrdMonitor::include_lfn_ = true;
  } else if (key == "SendAuth") {
    XrdMonitor::include_auth_ = true;
    if (value == "userdn") {
      XrdMonitor::include_dn_ = true;
    }
  } else if (key == "Ops") {
    XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasOPS;
  }  else if (key == "Ssq") {
    XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasOPS;
    XrdMonitor::file_flags_ |= XrdXrootdMonFileHdr::hasSSQ;
  } else 
    gotit = false;
  
  if (gotit)
    LogCfgParm(Logger::Lvl1, profilerlogmask, profilerlogname, key, value);

}



Catalog* ProfilerFactory::createCatalog(PluginManager* pm) throw (DmException)
{
  Catalog *nested;
  if (this->nestedCatalogFactory_ != 0x00)
    nested = CatalogFactory::createCatalog(this->nestedCatalogFactory_, pm);
  else
    return 0x00;

  initXrdMonitorIfNotInitialized();
  
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Creating ProfilerCatalog");
  
  return new ProfilerCatalog(nested);
}



PoolManager* ProfilerFactory::createPoolManager(PluginManager* pm) throw (DmException)
{
  PoolManager *nested;
  if (this->nestedPoolManagerFactory_ != 0x00)
    nested = PoolManagerFactory::createPoolManager(this->nestedPoolManagerFactory_, pm);
  else
    return 0x00;

  initXrdMonitorIfNotInitialized();
  
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Creating ProfilerPoolManager");
  
  return new ProfilerPoolManager(nested);
}


IODriver*   ProfilerFactory::createIODriver(PluginManager* pm)   throw (DmException)
{
  IODriver *nested;
  if (this->nestedIODriverFactory_ != 0x00)
    nested = IODriverFactory::createIODriver(this->nestedIODriverFactory_, pm);
  else
    return 0x00;

  initXrdMonitorIfNotInitialized();
  
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Creating ProfilerIODriver");

  return new ProfilerIODriver(nested);
}

void ProfilerFactory::initXrdMonitorIfNotInitialized() throw (DmException)
{
  int ret;
  ret = XrdMonitor::initOrNOP();
  if (ret < 0) {
    throw DmException(DMLITE_SYSERR(DMLITE_UNKNOWN_ERROR),
        std::string("Could not connect to the monitoring collector"));
  } else if (ret != XRDMON_FUNC_IS_NOP) {
    XrdMonitor::sendServerIdent();
  }
}


static void registerProfilerPlugin(PluginManager* pm) throw(DmException)
{
  ProfilerFactory *pf = new ProfilerFactory(pm->getCatalogFactory(),
                                            pm->getPoolManagerFactory(),
                                            pm->getIODriverFactory());
  pm->registerCatalogFactory(pf);
  pm->registerPoolManagerFactory(pf);
  pm->registerIODriverFactory(pf);
}



/// This is what the PluginManager looks for
PluginIdCard plugin_profiler = {
  PLUGIN_ID_HEADER,
  registerProfilerPlugin
};
