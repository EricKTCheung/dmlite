#include <dmlite/cpp/io.h>
#include "NotImplemented.h"


using namespace dmlite;



IOFactory::~IOFactory()
{
  // Nothing
}



FACTORY_NOT_IMPLEMENTED(IODriver* IOFactory::createIODriver(PluginManager*) throw (DmException));



IODriver::~IODriver()
{
  // Nothing
}



NOT_IMPLEMENTED(IOHandler* IODriver::createIOHandler(const std::string&, int, const Extensible&, mode_t) throw (DmException));
NOT_IMPLEMENTED(void IODriver::doneWriting(const Location&) throw (DmException));
NOT_IMPLEMENTED(void IODriver::setSecurityContext(const SecurityContext*) throw (DmException));



void IODriver::setSecurityContext(IODriver* i, const SecurityContext* ctx) throw (DmException)
{
  if (i != NULL) i->setSecurityContext(ctx);
}



IOHandler::~IOHandler()
{
  // Nothing
}



NOT_IMPLEMENTED_WITHOUT_ID(void IOHandler::close(void) throw (DmException));



struct stat IOHandler::fstat() throw (DmException)
{
  struct stat st;
  off_t where = this->tell();
  this->seek(0, kEnd);
  st.st_size = this->tell();
  this->seek(where, kSet);
  return st;
}



NOT_IMPLEMENTED_WITHOUT_ID(size_t IOHandler::read(char*, size_t) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(size_t IOHandler::write(const char*, size_t) throw (DmException));



size_t IOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  size_t total = 0;
  for (size_t i = 0; i < count; ++i) {
    total += this->read(static_cast<char*>(vector[i].iov_base), vector[i].iov_len);
  }
  return total;
}



size_t IOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  size_t total = 0;
  for (size_t i = 0; i < count; ++i) {
    total += this->write(static_cast<char*>(vector[i].iov_base), vector[i].iov_len);
  }
  return total;
}



size_t IOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  off_t prev = this->tell();
  this->seek(offset, IOHandler::kSet);
  size_t nread = this->read((char*)buffer, count);
  this->seek(prev, IOHandler::kSet);
  return nread;
}



size_t IOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  off_t prev = this->tell();
  this->seek(offset, IOHandler::kSet);
  size_t nwrite = this->write((const char*)buffer, count);
  this->seek(prev, IOHandler::kSet);
  return nwrite;
}



NOT_IMPLEMENTED_WITHOUT_ID(void IOHandler::seek(off_t, Whence) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(off_t IOHandler::tell(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(void IOHandler::flush(void) throw (DmException));
NOT_IMPLEMENTED_WITHOUT_ID(bool IOHandler::eof(void) throw (DmException));
