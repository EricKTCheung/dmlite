/// @file   include/dmlite/dm_catalog.h
/// @brief  I/O API. Abstracts how to write or read to/from a disk within
///         a pool.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DM_IO_H
#define	DM_IO_H

#include <iostream>
#include "dm_exceptions.h"

namespace dmlite {

/// Plug-ins must implement a concrete factory to be instantiated.
class IOFactory {
public:
  /// Virtual destructor
  virtual ~IOFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

  /// Instantiate a implementation of std::iostream
  virtual std::iostream* createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException) = 0;
  
protected:
private:
};

};

#endif	// DM_IO_H
