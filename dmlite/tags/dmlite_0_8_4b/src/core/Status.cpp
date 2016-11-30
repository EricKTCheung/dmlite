/// @file   core/Status.cpp
/// @brief  Status objects used by the API
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <dmlite/cpp/status.h>
#include <iomanip>
#include <sstream>
#include "utils/logger.h"
#include <string>

using namespace dmlite;

DmStatus::DmStatus(): errorCode_(DMLITE_SUCCESS), errorMsg_()
{
  // Nothing
}

DmStatus::DmStatus(int code): errorCode_(code), errorMsg_()
{
  // Nothing
}

DmStatus::DmStatus(int code, const std::string& string): errorCode_(code)
{
  std::string s;
  Logger::getStackTrace(s);

  std::ostringstream os;
  os << "[#"
     << std::setfill('0') << std::setw(2) << (DMLITE_ETYPE(code) >> 24) << '.'
     << std::setfill('0') << std::setw(6) << (DMLITE_ERRNO(code))
     << "] " << string;
  errorMsg_ = os.str();

  Err("", "Info: " << this->errorMsg_);
}

DmStatus::DmStatus(int code, const char* fmt, ...): errorCode_(code)
{
  va_list args;

  va_start(args, fmt);
  this->setMessage(fmt, args);
  va_end(args);

}

DmStatus::DmStatus(int code, const char* fmt, va_list args): errorCode_(code)
{
  this->setMessage(fmt, args);
}

DmStatus::DmStatus(const DmStatus &base)
{
  this->errorCode_ = base.errorCode_;
  this->errorMsg_  = base.errorMsg_;
}

DmStatus::DmStatus(const DmException &base) {
  this->errorCode_ = base.code();
  this->errorMsg_  = base.what();
}

DmStatus::~DmStatus() throw()
{
  // Nothing
}

int DmStatus::code() const throw()
{
  return this->errorCode_;
}

const char* DmStatus::what() const throw()
{
  return this->errorMsg_.c_str();
}

void DmStatus::setMessage(const char* fmt, va_list args)
{
  std::string s;
  Logger::getStackTrace(s);

  char buffer[512];
  int  n;

  n = snprintf(buffer, sizeof(buffer),
               "[#%02d.%06d] ", DMLITE_ETYPE(errorCode_) >> 24,
                                DMLITE_ERRNO(errorCode_));

  vsnprintf(buffer + n, sizeof(buffer) - n, fmt, args);
  buffer[sizeof(buffer)-1] = '\0';

  this->errorMsg_ = buffer;
}

bool DmStatus::ok() const throw()
{
  return this->errorCode_ == DMLITE_SUCCESS;
}

DmException DmStatus::exception() const throw() {
  return DmException(this->errorCode_, this->errorMsg_);
}
