/// @file   include/dmlite/dm_catalog.h
/// @brief  I/O API. Abstracts how to write or read to/from a disk within
///         a pool.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DM_IO_H
#define	DM_IO_H

#include <iostream>
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
  /// @param whence Reference.
  virtual void seek(long offset, std::ios_base::seekdir whence) throw (DmException) = 0;

  /// Return the cursor position.
  virtual long tell(void) throw (DmException) = 0;

  /// Flush the buffer.
  virtual void flush(void) throw (DmException) = 0;

  /// Return true if end of file.
  virtual bool eof(void) throw (DmException) = 0;
};

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
  virtual IOHandler* createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException) = 0;
  
protected:
private:
};

};

#endif	// DM_IO_H
