/// @file   include/dmlite/cpp/io.h
/// @brief  I/O API. Abstracts how to write or read to/from a disk within
///         a pool.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_IO_H
#define DMLITE_CPP_IO_H

#include "../common/config.h"
#include "base.h"
#include "exceptions.h"
#include "utils/extensible.h"

#include <fcntl.h>
#include <map>
#include <sys/stat.h>
#include <sys/uio.h>

namespace dmlite {
  
  // Forward declarations.
  class Location;
  class PluginManager;
  class StackInstance;
  
  /// IO interface
  class IOHandler {
   protected:
    /// Locks this default simplified implementation
    /// of primitives like pread and pwrite, that are not thread safe.
    /// This is not too bad unless the application points
    /// multiple threads to the same IOHandler instance
    pthread_mutex_t seeklock;
   public:
    enum Whence { kSet = SEEK_SET, ///< Beginning of the file
                  kCur = SEEK_CUR, ///< Current position
                  kEnd = SEEK_END  ///< End of file
                };
     
    /// Ctor
    IOHandler();
                
    /// Virtual destructor
    virtual ~IOHandler();

    /// Close
    virtual void close(void) throw (DmException);

    /// Gets information about a file descriptor.
    /// @note Not all plug-ins will fill all the fields, but st_size is
    ///       a reasonable expectation.
    /// @note Default implementation combining seek/tell is provided.
    virtual struct ::stat fstat(void) throw (DmException);

    /// Read.
    /// @param buffer Where to store the data.
    /// @param count  Number of bytes to read.
    /// @return       Number of bytes actually read.
    virtual size_t read(char* buffer, size_t count) throw (DmException);

    /// Write.
    /// @param buffer Data to write.
    /// @param count  Number of bytes to write.
    /// @return       Number of bytes actually written.
    virtual size_t write(const char* buffer, size_t count) throw (DmException);

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
    /// @note         See man writev.
    /// @note         A default implementation using write is provided.
    virtual size_t writev(const struct iovec* vector, size_t count) throw (DmException);

    /// Read from the given offset without changing the file offset.
    /// @param buffer Where to put the data.
    /// @param count  Number of bytes to read.
    /// @param offset The operation offset.
    /// @note         A default implementation using read/seek/tell is provided.
    virtual size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);

    /// Write from the given offset without changing the file offset.
    /// @param buffer Data to write.
    /// @param count  Number of bytes to read.
    /// @param offset The operation offset.
    /// @note         A default implementation using read/seek/tell is provided.
    virtual size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);

    /// Move the cursor.
    /// @param offset The offset.
    /// @param whence Reference.
    virtual void seek(off_t offset, Whence whence) throw (DmException);

    /// Return the cursor position.
    virtual off_t tell(void) throw (DmException);

    /// Flush the buffer.
    virtual void flush(void) throw (DmException);

    /// Return true if end of file.
    virtual bool eof(void) throw (DmException);
  };

  /// IO Driver
  class IODriver {
   public:
    /// Use this flag in addition to the standard ones to skip any
    /// security check (i.e. token validation)
    /// Example: createIOHandler("/file.txt", O_RDONLY | IODriver::kInsecure, extras);
    enum { kInsecure = 010 };

    /// Virtual destructor
    virtual ~IODriver();

    /// String ID of the implementation.
    virtual std::string getImplId(void) const throw() = 0;

    /// Instantiate a implementation of IOHandler
    /// @param pfn    The file name.
    /// @param flags  The open mode. See man 2 open.
    /// @param extras As was given by the PoolHandler.
    /// @param mode   When called with O_CREAT, it will be used to create the file.
    virtual IOHandler* createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode = 0660) throw (DmException);
    
    /// Must be called when the front-end is done writing.
    /// @param pfn The file name.
    /// @param loc The Location object as returned by whereToWrite
    virtual void doneWriting(const Location& loc) throw (DmException);

   protected:
    friend class StackInstance;

    virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException);
    static void  setSecurityContext(IODriver* i,
                                    const SecurityContext* ctx) throw (DmException);
  };

  /// Plug-ins must implement a concrete factory to be instantiated.
  class IOFactory: public virtual BaseFactory {
   public:
    /// Virtual destructor
    virtual ~IOFactory();

   protected:
    friend class StackInstance;

    /// Create a IODriver
    virtual IODriver* createIODriver(PluginManager* pm) throw (DmException);
  };

};

#endif // DMLITE_CPP_IO_H
