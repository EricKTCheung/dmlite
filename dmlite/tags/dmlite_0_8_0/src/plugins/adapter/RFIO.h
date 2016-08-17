/// @file   RFIO.h
/// @brief  RFIO IO.
/// @author CERN DPM <dpm-devel@cern.ch>
#ifndef RFIO_H
#define	RFIO_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <fstream>
#include <pthread.h>

namespace dmlite {

  class StdRFIOFactory: public IODriverFactory {
   public:
    StdRFIOFactory();
    virtual ~StdRFIOFactory();

    void configure(const std::string& key, const std::string& value) throw (DmException);
    IODriver* createIODriver(PluginManager* pm) throw (DmException);

   private:
    std::string passwd_;
    bool        useIp_;
  };

  class StdRFIODriver: public IODriver {
   public:
    StdRFIODriver(std::string passwd, bool useIp);
    virtual ~StdRFIODriver();

    std::string getImplId() const throw();

    virtual void setStackInstance(StackInstance* si) throw (DmException);
    virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    IOHandler* createIOHandler(const std::string& pfn, int flags,
                               const Extensible& extras, mode_t mode) throw (DmException);

    void doneWriting(const Location& loc) throw (DmException);

   private:
    StackInstance*         si_;
    const SecurityContext* secCtx_;

    std::string passwd_;
    bool        useIp_;
  };

  class StdRFIOHandler: public IOHandler {
  public:
    StdRFIOHandler(const std::string& path,
                   int flags, mode_t mode) throw (DmException);
    virtual ~StdRFIOHandler();

    void   close(void) throw (DmException);
    int    fileno(void) throw (DmException);
    size_t read (char* buffer, size_t count) throw (DmException);
    size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);
    size_t write(const char* buffer, size_t count) throw (DmException);
    size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);
    void   seek (off_t offset, Whence whence) throw (DmException);
    off_t  tell (void) throw (DmException);
    void   flush(void) throw (DmException);
    bool   eof  (void) throw (DmException);

  protected:
    int  fd_;
    bool eof_;
    pthread_mutex_t mtx_;
    bool islocal_;

    class lk {
      public:
        lk(pthread_mutex_t *mp): mp(mp)
          { int err; if (mp && (err=pthread_mutex_lock(mp)))
             throw DmException(err, "Could not lock a mutex"); }
        ~lk()
          { int err; if (mp && (err=pthread_mutex_unlock(mp)))
            throw DmException(err, "Could not unlock a mutex"); }
      private:
        pthread_mutex_t *mp;
    };
    class pp {
      public:
        pp(int fd, bool *peof, off_t np);
        ~pp();
      private:
        int fd;
        off_t pos;
        bool eof,*peof;
    };
  };

};

#endif	// RFIO_H
