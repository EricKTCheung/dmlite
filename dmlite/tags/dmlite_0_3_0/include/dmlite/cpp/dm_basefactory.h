/// @file   include/dmlite/dm_basefactory.h
/// @brief  Base interface for all factories.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_BASEFACTORY_H
#define	DMLITEPP_BASEFACTORY_H

#include <string>
#include "dm_exceptions.h"

namespace dmlite {
  
class BaseFactory {
public:
  /// Virtual destructor
  virtual ~BaseFactory();
  
  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;
};
  
};

#endif	// DMLITEPP_BASEFACTORY_H
