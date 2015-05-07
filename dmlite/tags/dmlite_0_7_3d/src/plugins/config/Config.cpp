/// @file    plugins/config/Config.cpp
/// @brief   This plugin allow to split the configuration between files.
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dirent.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/urls.h>
#include <fnmatch.h>
#include <pthread.h>
#include <sys/stat.h>
#include "Config.h"
//#include "dmlite/common/config.h"
#include "utils/logger.h"


using namespace dmlite;

// If we use globals, we will have a rance conditions between different
// managers used in different threads.
static pthread_key_t  patternGlobalKey;
static pthread_once_t patternGlobalOnce = PTHREAD_ONCE_INIT;

// In Linux the prototype for filter in scandir pass a const struct dirent
// while in MacOS, for instance, it is a struct dirent*
#ifdef __linux__
static int configFilter(const struct dirent* entry)
#elif __APPLE__
static int configFilter(const struct dirent* entry)
#else
static int configFilter(struct dirent* entry)
#endif
{
  const char* pattern = static_cast<const char*>(pthread_getspecific(patternGlobalKey));
  return fnmatch(pattern, entry->d_name, 0) == 0;
}



static void initPatternGlobalKey(void)
{
  pthread_key_create(&patternGlobalKey, NULL);
}



ConfigFactory::ConfigFactory(PluginManager* pm): manager(pm)
{
  pthread_once(&patternGlobalOnce, initPatternGlobalKey);
  Log(Logger::Lvl0, Logger::unregistered, "Config", "----------------- ConfigFactory started. Starting configuration phase. DMLite v" <<
      DMLITE_MAJOR << "." << DMLITE_MINOR << "." << DMLITE_PATCH );
}



ConfigFactory::~ConfigFactory()
{
  // Nothing
}



void ConfigFactory::processIncludes(const std::string& path) throw (DmException)
{
  std::vector<std::string> components = Url::splitPath(path);
  std::string              pattern, location;
  
  if (path.empty())
    throw DmException(DMLITE_CFGERR(EINVAL),
                      "Include does not support empty paths");
  
  if (path[path.length() - 1] != '/') {
    pattern = components.back();
    components.pop_back();
    location = Url::joinPath(components);
  }
  else {
    location = path;
  }
  
  // Stat
  struct stat st;
  if (stat(location.c_str(), &st) != 0)
    throw DmException(DMLITE_CFGERR(errno),
                      "Could not stat %s", path.c_str());
  
  // If it is a file and there is no pattern, include directly
  if (pattern.empty() && S_ISREG(st.st_mode)) {
    this->manager->loadConfiguration(location);
    return;
  }
  // If there is pattern, but file, fail
  else if (!pattern.empty() && S_ISREG(st.st_mode)) {
    throw DmException(DMLITE_CFGERR(ENOTDIR),
                      "%s is not a directory", location.c_str());
  }
  else if (pattern.empty()) {
    pattern = "*";
  }
  
  // Otherwise, include content
  pthread_setspecific(patternGlobalKey, pattern.c_str());
  
  struct dirent **namelist;
  int nmatches = scandir(location.c_str(), &namelist, configFilter, alphasort);
  
  if (nmatches < 0) {
    throw DmException(DMLITE_CFGERR(errno),
                      "Could not list the content of %s", location.c_str());
  }
  
  for (int i = 0; i < nmatches; ++i) {
    std::string cfgf = location + "/" + namelist[i]->d_name;
    
    Log(Logger::Lvl1, Logger::unregistered, "config", "Processing config file:" << cfgf);
    this->manager->loadConfiguration(cfgf);
    free(namelist[i]);
  }
  free(namelist);
}



void ConfigFactory::configure(const std::string& key,
                              const std::string& value) throw (DmException)
{
  bool gotit = true;
  LogCfgParm(Logger::Lvl4, Logger::unregistered, "ConfigFactory", key, value);
  
  
  if (key == "LogLevel" || key == "loglevel") {
    Log(Logger::Lvl0, Logger::unregistered, "config", "Setting global log level to :" << value);
    Logger::get()->setLevel((Logger::Level)atoi(value.c_str()));
  }
  else
  if (key == "Include" || key == "include") {
    Log(Logger::Lvl0, Logger::unregistered, "config", "Processing config directory:" << value);
    this->processIncludes(value);
  }
  else
  if (key == "Log" || key == "log") {
    Log(Logger::Lvl0, Logger::unregistered, "config", "Setting log ON for component '" << value << "'");
    Logger::get()->setLogged(value, true);   
    
  }
  else gotit = false;
  
 
  if (gotit)
    LogCfgParm(Logger::Lvl1, Logger::unregistered, "ConfigFactory", key, value);
    
}



static void registerPluginConfig(PluginManager* pm) throw (DmException)
{
  pm->registerConfigureFactory(new ConfigFactory(pm));
}



struct PluginIdCard plugin_config = {
  PLUGIN_ID_HEADER,
  registerPluginConfig
};
