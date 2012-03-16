/// @file   core/IO.cpp
/// @brief  Built-in IO factory (std::fstream)
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <errno.h>
#include <fstream>
#include <iosfwd>
#include "Private.h"

using namespace dmlite;

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



std::iostream* StdIOFactory::createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
  std::iostream* stream;

  stream = new std::fstream(uri.c_str(), openmode);
  if (stream->fail()) {
    delete stream;
    switch (errno) {
      case ENOENT:
        throw DmException(DM_NO_SUCH_FILE, uri + " not found");
      default:
        throw DmException(DM_BAD_OPERATION, "Could not open %s (%d)", uri.c_str(), errno);
    }
  }

  return stream;
}
