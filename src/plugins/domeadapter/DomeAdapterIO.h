/// @file   DomeAdapterIO.h
/// @brief  Filesystem IO using dome.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_IO_H
#define DOME_ADAPTER_IO_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <fstream>
#include "utils/DavixPool.h"

namespace dmlite {

  class DomeIOFactory: public IODriverFactory {
  public:
    DomeIOFactory();
    virtual ~DomeIOFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    IODriver* createIODriver(PluginManager* pm) throw (DmException);

  private:
    std::string passwd_;
    bool        useIp_;

    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;
  };

 class DomeIODriver: public IODriver {
   public:
    DomeIODriver(std::string passwd, bool useIp, DavixCtxPool &davixPool);
    virtual ~DomeIODriver();

    std::string getImplId() const throw();

    virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException);
    virtual void setStackInstance(StackInstance* si) throw (DmException);

    IOHandler* createIOHandler(const std::string& pfn, int flags,
                               const Extensible& extras, mode_t mode) throw (DmException);

    void doneWriting(const Location& loc) throw (DmException);

   private:
    const SecurityContext* secCtx_;
    StackInstance* si_;

    std::string passwd_;
    bool        useIp_;

    DavixCtxPool &davixPool_;
  };


  // copy of StdIOHandler
  class DomeIOHandler: public IOHandler {
  public:
    DomeIOHandler(const std::string& path,
     int flags, mode_t mode) throw (DmException);
    virtual ~DomeIOHandler();

    void   close(void) throw (DmException);
    int    fileno() throw (DmException);
    struct stat fstat(void) throw (DmException);

    size_t read (char* buffer, size_t count) throw (DmException);
    size_t write(const char* buffer, size_t count) throw (DmException);
    size_t readv(const struct iovec* vector, size_t count) throw (DmException);
    size_t writev(const struct iovec* vector, size_t count) throw (DmException);

    size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);
    size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);

    void   seek (off_t offset, Whence whence) throw (DmException);
    off_t  tell (void) throw (DmException);
    void   flush(void) throw (DmException);
    bool   eof  (void) throw (DmException);

  protected:
    int  fd_;
    bool eof_;
  };

}

#endif
