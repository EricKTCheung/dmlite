/// @file   include/dmlite/cpp/io.h
/// @brief  I/O API. Abstracts how to write or read to/from a disk within
///         a pool.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_IO_H
#define DMLITE_CPP_IO_H

#include <fcntl.h>
#include <map>
#include "base.h"
#include "exceptions.h"
#include "utils/extensible.h"

namespace dmlite {
  
  // Forward declarations.
  class PluginManager;
  class StackInstance;
  
  /// IO interface
  class IOHandler {
   public:
    enum Whence { kSet = SEEK_SET, ///< Beginning of the file
                  kCur = SEEK_CUR, ///< Current position
                  kEnd = SEEK_END  ///< End of file
                };
     
    /// Virtual destructor
    virtual ~IOHandler();

    /// Close
    virtual void close(void) throw (DmException) = 0;

    /// Gets information about a file descriptor.
    /// @note Not all plug-ins will fill all the fields, but st_size is
    ///       a reasonable expectation.
    /// @note Default implementation combining seek/tell is provided.
    virtual struct stat fstat(void) throw (DmException);

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

    /// Read into multiple buffers.
    /// @param vector An array with 'count' iovec structs.
    /// @param count  Number of elements in vector.
    /// @return       The total size read.
    /// @note         See man readv.
    /// @note         A default implementation using read is provided.
    virtual size_t readv(const struct iovec* vector, size_t count) throw (DmException);

    /// Write from multiple buffers.
    /// @param vector An array with 'count' iovec structs.
    /// @param count  Number of elements in vector.
    /// @return       The total size written.
    /// @note         See man write.v
    /// @note         A default implementation using write is provided.
    virtual size_t writev(const struct iovec* vector, size_t count) throw (DmException);

    /// Move the cursor.
    /// @param offset The offset.
    /// @param whence Reference.
    virtual void seek(off_t offset, Whence whence) throw (DmException) = 0;

    /// Return the cursor position.
    virtual off_t tell(void) throw (DmException) = 0;

    /// Flush the buffer.
    virtual void flush(void) throw (DmException) = 0;

    /// Return true if end of file.
    virtual bool eof(void) throw (DmException) = 0;
  };

  /// IO Driver
  class IODriver: public virtual BaseInterface {
   public:
    /// Use this flag in addition to the standard ones to skip any
    /// security check (i.e. token validation)
    /// Example: createIOHandler("/file.txt", O_RDONLY | IODriver::kInsecure, extras);
    enum { kInsecure = 010 };

    /// Virtual destructor
    virtual ~IODriver();

    /// Instantiate a implementation of IOHandler
    /// @param pfn    The file name.
    /// @param flags  The open mode. See man 2 open.
    /// @param extras As was given by the PoolHandler.
    /// @param mode   When called with O_CREAT, it will be used to create the file.
    virtual IOHandler* createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode = 0660) throw (DmException) = 0;
    
    /// Must be called when the front-end is done writing.
    /// @param pfn    The file name.
    /// @param params The extra parameters as was returned by whereToWrite
    virtual void doneWriting(const std::string& pfn,
                             const Extensible& params) throw (DmException) = 0;
  };

  /// Plug-ins must implement a concrete factory to be instantiated.
  class IOFactory: public virtual BaseFactory {
   public:
    /// Virtual destructor
    virtual ~IOFactory();

   protected:
    friend class StackInstance;

    /// Create a IODriver
    virtual IODriver* createIODriver(PluginManager* pm) throw (DmException) = 0;  
  };

};

#endif // DMLITE_CPP_IO_H
