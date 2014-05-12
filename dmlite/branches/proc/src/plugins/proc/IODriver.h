/// @file    plugins/proc/IODriver.h
/// @brief   Dummy IO driver for proc virtual files
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PROC_IO_DRIVER_H
#define PROC_IO_DRIVER_H

#include <dmlite/cpp/inode.h>
#include <dmlite/cpp/io.h>
#include "Provider.h"

namespace dmlite {

class ProcIOHandler: public IOHandler {
public:
  ProcIOHandler(const std::string& content): content_(content)
  {
  }

  size_t read(char* buffer, size_t count) throw (DmException)
  {
    content_.read(buffer, count);
    return content_.gcount();
  }

protected:
  std::istringstream content_;
};

class ProcIODriver: public IODriver {
public:
  ProcIODriver() {
  }

  virtual ~ProcIODriver() {
  }

  std::string getImplId() const throw () {
    return "ProcIODriver";
  }

  void setStackInstance(StackInstance* si) throw (DmException) {
    this->si_ = si;
  }

  void setSecurityContext(const SecurityContext* ctx) throw (DmException) {
  }

  IOHandler* createIOHandler(const std::string& pfn,
      int flags, const Extensible& extras, mode_t mode = 0660) throw (DmException) {
    ProcProvider* root = boost::any_cast<ProcProvider*>(this->si_->get("proc.root"));
    ProcProvider* provider = ProcProvider::follow(root, "/", pfn);
    return new ProcIOHandler(provider->getContent());
  }

protected:
  StackInstance* si_;
};

};

#endif // PROC_IO_DRIVER_H
