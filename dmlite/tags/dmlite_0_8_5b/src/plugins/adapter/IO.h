/// @file   IO.h
/// @brief  Filesystem IO.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef IO_H
#define	IO_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <fstream>

namespace dmlite {

  class StdIOFactory: public IODriverFactory {
   public:
    StdIOFactory();
    virtual ~StdIOFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    IODriver* createIODriver(PluginManager* pm) throw (DmException);

   private:
    std::string passwd_;
    bool        useIp_;
  };

  class StdIODriver: public IODriver {
   public:
    StdIODriver(std::string passwd, bool useIp);
    virtual ~StdIODriver();

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
  };

  class StdIOHandler: public IOHandler {
  public:
    StdIOHandler(const std::string& path,
                 int flags, mode_t mode) throw (DmException);
    virtual ~StdIOHandler();

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

};

#endif	// IO_H
