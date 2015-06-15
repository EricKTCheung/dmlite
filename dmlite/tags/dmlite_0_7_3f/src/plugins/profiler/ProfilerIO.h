/// @file   ProfilerIO.h
/// @brief  Profiler plugin.
/// @author Fabrizio Furano <furano@cern.ch>
#ifndef PROFILERIO_H
#define	PROFILERIO_H

#include "ProfilerXrdMon.h"

#include <dmlite/cpp/io.h>


namespace dmlite {

  class ProfilerIOHandler: public IOHandler, private ProfilerXrdMon {
  public:
    ProfilerIOHandler(IOHandler* decorated, const std::string& pfn,
        int flags, const Extensible& extras, SecurityContext secCtx) throw (DmException);
    virtual ~ProfilerIOHandler();

    std::string getImplId(void) const throw() {
      std::string implId = "ProfilerIOHandler";
      implId += " over ";
      implId += this->decoratedId_;

      return implId;
    }

    virtual size_t read(char* buffer, size_t count) throw (DmException);
    virtual void close(void) throw (DmException);
    virtual struct ::stat fstat(void) throw (DmException);
    virtual size_t write(const char* buffer, size_t count) throw (DmException);
    virtual size_t readv(const struct iovec* vector, size_t count) throw (DmException);
    virtual size_t writev(const struct iovec* vector, size_t count) throw (DmException);
    virtual size_t pread(void* buffer, size_t count, off_t offset) throw (DmException);
    virtual size_t pwrite(const void* buffer, size_t count, off_t offset) throw (DmException);
    virtual void seek(off_t offset, Whence whence) throw (DmException);
    virtual off_t tell(void) throw (DmException);
    virtual void flush(void) throw (DmException);
    virtual bool eof(void) throw (DmException);

  protected:
    IOHandler* decorated_;
    char*      decoratedId_;

    void resetCounters();
  };


  class ProfilerIODriver: public IODriver {
  public:
    ProfilerIODriver(IODriver* decorates) throw (DmException);
    virtual ~ProfilerIODriver();

    std::string getImplId(void) const throw() {
      return std::string("ProfilerIODriver");
    }


    virtual IOHandler* createIOHandler(const std::string& pfn,
                                       int flags,
                                       const Extensible& extras,
                                       mode_t mode = 0660) throw (DmException);

    void setStackInstance(StackInstance* si) throw (DmException);

    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    virtual void doneWriting(const Location& loc) throw (DmException);
  protected:
    StackInstance *stack_;

    IODriver*  decorated_;
    char*      decoratedId_;
  };

};

#endif	// PROFILERIO_H
