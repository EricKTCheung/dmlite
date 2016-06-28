/// @file   core/Exceptions.cpp
/// @brief  Exceptions used by the API
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <dmlite/cpp/exceptions.h>
#include <iomanip>
#include <sstream>
#include "utils/logger.h"
#include <string>

using namespace dmlite;








DmException::DmException(): std::exception(), errorCode_(0), errorMsg_()
{
  Logger::getStackTrace(stacktrace_);
  Err("", " DmException() at " << stacktrace_);
  // Nothing
}



DmException::DmException(int code): std::exception(), errorCode_(code), errorMsg_()
{
  Logger::getStackTrace(stacktrace_);
  Err("", " DmException(" << code << ") at " << std::endl << stacktrace_);
  // Nothing
}

void DmException::report() const throw() {
    Err("", " DmException(..):" << this->errorMsg_ << " at "  << std::endl << stacktrace_);
}

DmException::DmException(int code, const std::string& string): std::exception(), errorCode_(code)
{
  Logger::getStackTrace(stacktrace_);

  std::ostringstream os;
  os << "[#"
     << std::setfill('0') << std::setw(2) << (DMLITE_ETYPE(code) >> 24) << '.'
     << std::setfill('0') << std::setw(6) << (DMLITE_ERRNO(code))
     << "] " << string;
  errorMsg_ = os.str();

  if(code != ENOENT || Logger::get()->getLevel() >= 3) {
    this->report();
  }
  else {
    Err("", string);
  }
}

DmException::DmException(int code, const char* fmt, ...): std::exception(), errorCode_(code)
{
  va_list args;

  va_start(args, fmt);
  this->setMessage(fmt, args);
  va_end(args);

}



DmException::DmException(int code, const char* fmt, va_list args): std::exception(), errorCode_(code)
{
  this->setMessage(fmt, args);
}



DmException::DmException(const DmException &base) : std::exception()
{
  this->errorCode_ = base.errorCode_;
  this->errorMsg_  = base.errorMsg_;
}



DmException::~DmException() throw()
{
  // Nothing
}



int DmException::code() const throw()
{
  return this->errorCode_;
}



const char* DmException::what() const throw()
{
  return this->errorMsg_.c_str();
}

void DmException::setMessage(const char* fmt, va_list args)
{
  Logger::getStackTrace(stacktrace_);

  char buffer[512];
  int  n;

  n = snprintf(buffer, sizeof(buffer),
               "[#%02d.%06d] ", DMLITE_ETYPE(errorCode_) >> 24,
                                DMLITE_ERRNO(errorCode_));

  vsnprintf(buffer + n, sizeof(buffer) - n, fmt, args);
  buffer[sizeof(buffer)-1] = '\0';

  this->errorMsg_ = buffer;

  if(errorCode_ != ENOENT || Logger::get()->getLevel() >= 3) {
    this->report();
  }
  else {
    Err("", this->errorMsg_);
  }
}
