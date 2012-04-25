/// @file   core/IO.cpp
/// @brief  Built-in IO factory (std::fstream)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <errno.h>
#include <fstream>
#include <iosfwd>
#include "Private.h"

using namespace dmlite;


IOHandler::~IOHandler()
{
  // Nothing
}



IOFactory::~IOFactory()
{
  // Nothing
}



StdIOFactory::StdIOFactory()
{
  // Nothing
}



StdIOFactory::~StdIOFactory()
{
  // Nothing
}



void StdIOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  throw DmException(DM_UNKNOWN_OPTION, key + " not known");
}



IOHandler* StdIOFactory::createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
  return new StdIOHandler(uri, openmode);
}



StdIOHandler::StdIOHandler(const std::string& path, std::iostream::openmode openmode) throw (DmException):
  stream_(path.c_str(), openmode)
{
  // Check we actually opened
  if (this->stream_.fail()) {
    throw DmException(DM_NO_SUCH_FILE, "Could not open " + path);
  }
}



StdIOHandler::~StdIOHandler()
{
  this->close();
}



void StdIOHandler::close() throw (DmException)
{
  this->stream_.close();
}



size_t StdIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  return this->stream_.read(buffer, count).gcount();
}



size_t StdIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  this->stream_.write(buffer, count);
  return count;
}



void StdIOHandler::seek(long offset, std::ios_base::seekdir whence) throw (DmException)
{
  this->stream_.seekg(offset, whence);
  this->stream_.seekp(offset, whence);
}



long StdIOHandler::tell(void) throw (DmException)
{
  return this->stream_.tellg();
}



void StdIOHandler::flush(void) throw (DmException)
{
  this->stream_.flush();
}



bool StdIOHandler::eof(void) throw (DmException)
{
  return this->stream_.eof();
}
