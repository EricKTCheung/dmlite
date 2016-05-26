/// @file   DomeAdapterIO.h
/// @brief  Filesystem IO using dome.
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_HEAD_CATALOG_H
#define DOME_ADAPTER_HEAD_CATALOG_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/dummy/DummyCatalog.h>
#include <fstream>
#include "utils/DavixPool.h"

namespace dmlite {
  extern Logger::bitmask domeadapterlogmask;
  extern Logger::component domeadapterlogname;

  class DomeAdapterHeadCatalog;

  class DomeAdapterHeadCatalogFactory : public CatalogFactory {
  public:
    DomeAdapterHeadCatalogFactory();
    virtual ~DomeAdapterHeadCatalogFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    Catalog *createCatalog(PluginManager* pm) throw (DmException);

  private:
    std::string domehead_;
    // std::string passwd_;
    // bool        useIp_;
    // std::string domedisk_;

    DavixCtxFactory davixFactory_;
    DavixCtxPool davixPool_;

    friend class DomeAdapterHeadCatalog;
  };

  class DomeAdapterHeadCatalog : public DummyCatalog {
  public:
    DomeAdapterHeadCatalog(DomeAdapterHeadCatalogFactory *factory);
    virtual ~DomeAdapterHeadCatalog();

    std::string getImplId() const throw();

    virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException);
    virtual void setStackInstance(StackInstance* si) throw (DmException);

    virtual void getChecksum(const std::string& path,
                             const std::string& csumtype,
                             std::string& csumvalue, const bool forcerecalc = false, const int waitsecs = 0) throw (DmException);

   private:
    const SecurityContext* secCtx_;
    StackInstance* si_;

    DomeAdapterHeadCatalogFactory &factory_;
  };

 // class DomeIODriver: public IODriver {
 //   public:
 //    DomeIODriver(std::string passwd, bool useIp, std::string domedisk, DavixCtxPool &davixPool);
 //    virtual ~DomeIODriver();
 //
 //    std::string getImplId() const throw();
 //
 //    virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException);
 //    virtual void setStackInstance(StackInstance* si) throw (DmException);
 //
 //    IOHandler* createIOHandler(const std::string& pfn, int flags,
 //                               const Extensible& extras, mode_t mode) throw (DmException);
 //
 //    void doneWriting(const Location& loc) throw (DmException);
 //
 //   private:
 //    const SecurityContext* secCtx_;
 //    StackInstance* si_;
 //
 //    std::string passwd_;
 //    bool        useIp_;
 //
 //    DavixCtxPool &davixPool_;
 //    std::string domedisk_;
 //  };
 //
 //  class DomeTunnelHandler : public IOHandler {
 //  public:
 //    DomeTunnelHandler(DavixCtxPool &pool, const std::string &url, int flags, mode_t mode) throw (DmException);
 //    virtual ~DomeTunnelHandler();
 //
 //    void   close(void) throw (DmException);
 //    int    fileno() throw (DmException);
 //
 //    size_t read (char* buffer, size_t count) throw (DmException);
 //    size_t write(const char* buffer, size_t count) throw (DmException);
 //
 //    size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);
 //    size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);
 //
 //    void   seek (off_t offset, Whence whence) throw (DmException);
 //    off_t  tell (void) throw (DmException);
 //    void   flush(void) throw (DmException);
 //    bool   eof  (void) throw (DmException);
 //  private:
 //    void checkErr(Davix::DavixError **err) throw (DmException);
 //
 //    std::string url_;
 //    DavixGrabber grabber_;
 //    DavixStuff *ds_;
 //
 //    Davix::DavPosix dpos_;
 //    DAVIX_FD *fd_;
 //
 //    size_t lastRead_;
 //  };
 //
 //
 //  // copy of StdIOHandler
 //  class DomeIOHandler: public IOHandler {
 //  public:
 //    DomeIOHandler(const std::string& path,
 //     int flags, mode_t mode) throw (DmException);
 //    virtual ~DomeIOHandler();
 //
 //    void   close(void) throw (DmException);
 //    int    fileno() throw (DmException);
 //    struct stat fstat(void) throw (DmException);
 //
 //    size_t read (char* buffer, size_t count) throw (DmException);
 //    size_t write(const char* buffer, size_t count) throw (DmException);
 //    size_t readv(const struct iovec* vector, size_t count) throw (DmException);
 //    size_t writev(const struct iovec* vector, size_t count) throw (DmException);
 //
 //    size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);
 //    size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);
 //
 //    void   seek (off_t offset, Whence whence) throw (DmException);
 //    off_t  tell (void) throw (DmException);
 //    void   flush(void) throw (DmException);
 //    bool   eof  (void) throw (DmException);
 //
 //  protected:
 //    int  fd_;
 //    bool eof_;
 //  private:
 //    void mkdirp(const std::string &path) throw (DmException);
 //  };

}

#endif
