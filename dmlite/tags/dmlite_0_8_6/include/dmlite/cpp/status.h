/// @file   include/dmlite/cpp/status.h
/// @brief  Status objects used by the API
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DMLITE_CPP_STATUS_H
#define DMLITE_CPP_STATUS_H

#include "dmlite/common/config.h"
#include "dmlite/common/errno.h"
#include "exceptions.h"

#include <cstdarg>
#include <exception>
#include <string>

namespace dmlite {

class DmStatus {
public:
  DmStatus();
  DmStatus(int code);
  DmStatus(int code, const std::string &string);
  DmStatus(int code, const char* fmt, va_list args);
  DmStatus(int code, const char* fmt, ...);

  DmStatus(const DmStatus &de);
  DmStatus(const DmException &de);

  virtual ~DmStatus() throw();

  int         code() const throw();
  const char* what() const throw();

  bool ok() const throw();
  DmException exception() const throw();

protected:
  int         errorCode_;
  std::string errorMsg_;

  void setMessage(const char* fmt, va_list args);
};

};
#endif // DMLITE_CPP_STATUS_H
