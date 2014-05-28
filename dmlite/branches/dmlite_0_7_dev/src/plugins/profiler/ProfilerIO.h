/// @file   ProfilerIO.h
/// @brief  Profiler plugin.
/// @author Fabrizio Furano <furano@cern.ch>
#ifndef PROFILERIO_H
#define	PROFILERIO_H

#include <dmlite/cpp/io.h>

namespace dmlite {

  class ProfilerIOHandler: public IOHandler {
  public:
    ProfilerIOHandler(IOHandler* decorated) throw (DmException);
    virtual ~ProfilerIOHandler();

    std::string getImplId(void) const throw() {
      return std::string("ProfilerIOHandler");
    }

    virtual size_t read(char* buffer, size_t count) throw (DmException);

  protected:
    IOHandler* decorated_;
    char*      decoratedId_;
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
                                       mode_t mode = 0660) throw (DmException) {
      return new ProfilerIOHandler( decorated_->createIOHandler(pfn, flags, extras, mode) );

    };


    void setStackInstance(StackInstance* si) throw (DmException);

    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

  protected:


    IODriver*  decorated_;
    char*      decoratedId_;
  };

};

#endif	// PROFILERIO_H
