/// @file   include/dmlite/dm_catalog.h
/// @brief  I/O API. Abstracts how to write or read to/from a disk within
///         a pool.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_IO_H
#define	DMLITEPP_IO_H

#include <map>
#include "dm_auth.h"
#include "dm_exceptions.h"

namespace dmlite {

// IO interface
class IOHandler {
public:
  /// Virtual destructor
  virtual ~IOHandler();

  /// Close
  virtual void close(void) throw (DmException) = 0;

  /// Read.
  /// @param buffer Where to store the data.
  /// @param count  Number of bytes to read.
  /// @return       Number of bytes actually read.
  virtual size_t read(char* buffer, size_t count) throw (DmException) = 0;

  /// Write.
  /// @param buffer Data to write.
  /// @param count  Number of bytes to write.
  /// @return       Number of bytes actually written.
  virtual size_t write(const char* buffer, size_t count) throw (DmException) = 0;

  /// Move the cursor.
  /// @param offset The offset.
  /// @param whence Reference (see lseek).
  virtual void seek(long offset, int whence) throw (DmException) = 0;

  /// Return the cursor position.
  virtual long tell(void) throw (DmException) = 0;

  /// Flush the buffer.
  virtual void flush(void) throw (DmException) = 0;

  /// Return true if end of file.
  virtual bool eof(void) throw (DmException) = 0;
};

class StackInstance;

class IODriver: public virtual BaseInterface {
public:
  /// Virtual destructor
  virtual ~IODriver();
  
  /// Instantiate a implementation of IOHandler
  /// @param pfn    The file name.
  /// @param flags  The open mode (see open)
  /// @param extras As was given by the PoolHandler
  virtual IOHandler* createIOHandler(const std::string& pfn,
                                     int flags,
                                     const std::map<std::string, std::string>& extras) throw (DmException) = 0;
  
  /// Perform a stat over pfn.
  virtual struct stat pStat(const std::string& pfn) throw (DmException) = 0;
};

/// Plug-ins must implement a concrete factory to be instantiated.
class IOFactory: public virtual BaseFactory {
public:
  /// Virtual destructor
  virtual ~IOFactory();

  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

protected:
  friend class StackInstance;

  /// Create a IODriver
  virtual IODriver* createIODriver(PluginManager* pm) throw (DmException) = 0;  
  
private:
};

};

#endif	// DMLITEPP_IO_H
