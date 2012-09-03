/// @file    plugins/config/Config.h
/// @brief   This plugin allow to split the configuration between files.
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PLUGIN_CONFIG_H
#define	PLUGIN_CONFIG_H

#include <dmlite/cpp/base.h>

namespace dmlite {
  
  class ConfigFactory: public BaseFactory {
   public:
     ConfigFactory(PluginManager*);
     ~ConfigFactory();
     
     void configure(const std::string&, const std::string&) throw (DmException);
     
   protected:
     PluginManager* manager;
     
     void processIncludes(const std::string& path) throw (DmException);
  };
  
}

#endif // PLUGIN_CONFIG_H
