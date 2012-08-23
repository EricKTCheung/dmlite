/// @file   core/Types.cpp
/// @brief  C++ wrapper for C structs.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstdarg>
#include <cstring>
#include <dmlite/common/dm_types.h>



static char* duplicate(const char* s)
{
  size_t len = std::strlen(s);
  char*  copy = new char[len + 1];
  std::strcpy(copy, s);
  return copy;
}



Location::Location()
{
  this->available = false;
  this->priority  = 0;
  this->nextra    = 0;
  this->extra     = 0x00;
  std::memset(this->host, 0, sizeof(Location::host));
  std::memset(this->path, 0, sizeof(Location::path));
}



Location::Location(const Location& l)
{
  this->available = l.available;
  this->priority  = l.nextra;
  
  std::strncpy(this->host, l.host, sizeof(Location::host));
  std::strncpy(this->path, l.path, sizeof(Location::path));
  
  this->nextra = l.nextra;
  if (l.nextra > 0) {
    this->extra = new keyvalue[l.nextra];
    for (unsigned i = 0; i < l.nextra; ++i) {
      this->extra[i].key   = duplicate(l.extra[i].key);
      this->extra[i].value = duplicate(l.extra[i].value);
    }
  }
  else {
    this->extra = 0x00;
  }
}



Location::Location(const struct location& l)
{
  this->available = l.available;
  this->priority  = l.nextra;
  
  std::strncpy(this->host, l.host, sizeof(Location::host));
  std::strncpy(this->path, l.path, sizeof(Location::path));
  
  this->nextra = l.nextra;
  if (l.nextra > 0) {
    this->extra = new keyvalue[l.nextra];
    for (unsigned i = 0; i < l.nextra; ++i) {
      this->extra[i].key   = duplicate(l.extra[i].key);
      this->extra[i].value = duplicate(l.extra[i].value);
    }
  }
  else {
    this->extra = 0x00;
  }
}



Location::Location(const Url& url)
{
  this->available = true;
  this->priority  = 0;
  this->nextra    = 0;
  this->extra     = 0x00;
  
  std::strncpy(this->host, url.host, sizeof(Location::host));
  std::strncpy(this->path, url.path, sizeof(Location::path));
}



Location::Location(const char* host, const char* path, bool available, unsigned npairs, ...)
{
  this->available = available;
  this->priority  = 0;
  
  std::strncpy(this->host, host, sizeof(Location::host));
  std::strncpy(this->path, path, sizeof(Location::path));
  
  this->nextra = npairs;
  if (npairs > 0) {
    this->extra = new keyvalue[npairs];
    va_list args;
    va_start(args, npairs);
    
    for (unsigned i = 0; i < npairs; ++i) {
      const char* key   = va_arg(args, const char*);
      const char* value = va_arg(args, const char*);
      
      this->extra[i].key   = duplicate(key);
      this->extra[i].value = duplicate(value);
    }
    
    va_end(args);
  }
  else {
    this->extra = 0x00;
  }      
}



Location::~Location()
{
  if (this->extra != 0x00) {
    for (unsigned i = 0; i < this->nextra; ++i) {
      delete [] this->extra[i].key;
      delete [] this->extra[i].value;
    }
    delete [] this->extra;
  }
}



const Location& Location::operator = (const Location& src)
{
  this->available = src.available;
  this->priority  = src.priority;
  
  strncpy(this->host, src.host, sizeof(Location::host));
  strncpy(this->path, src.path, sizeof(Location::path));
  
  this->nextra = src.nextra;
  if (this->nextra > 0) {
    this->extra = new keyvalue[this->nextra];
    for (unsigned i = 0; i < src.nextra; ++i) {
      this->extra[i].key   = duplicate(src.extra[i].key);
      this->extra[i].value = duplicate(src.extra[i].value);
    }
  }
  else
    this->extra = 0x00;
  
  return *this;
}
