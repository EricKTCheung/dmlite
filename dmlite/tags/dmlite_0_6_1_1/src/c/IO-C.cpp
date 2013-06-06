/// @file   core/IO-C.cpp
/// @brief  C wrapper for dmlite::IOFactory.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/io.h>
#include <dmlite/c/dmlite.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/pooldriver.h>
#include <fcntl.h>
#include <stdarg.h>
#include "Private.h"


extern void dmlite_clocation_to_cpplocation(const dmlite_location* locp,
                                            dmlite::Location& loc);



dmlite_fd* dmlite_fopen(dmlite_context* context, const char* path, int flags,
                        const dmlite_any_dict* extra, ...)
{
  TRY(context, fopen)
  NOT_NULL(path);
  
  dmlite::IOHandler* stream;
  
  if (flags & O_CREAT) {
    va_list args;
    va_start(args, extra);
    mode_t mode = (mode_t)va_arg(args, int);
    va_end(args);

    if (extra != NULL)
      stream = context->stack->getIODriver()->
                                createIOHandler(path, flags, extra->extensible, mode);
    else
      stream = context->stack->getIODriver()->
                                createIOHandler(path, flags, dmlite::Extensible(), mode);
  }
  else {
    if (extra != NULL)
      stream = context->stack->getIODriver()->
                                createIOHandler(path, flags, extra->extensible);
    else
      stream = context->stack->getIODriver()->
                                createIOHandler(path, flags, dmlite::Extensible());
  }

  dmlite_fd* iofd = new dmlite_fd();
  iofd->stream  = stream;
  return iofd;
  CATCH_POINTER(context, fopen)
}



int dmlite_fclose(dmlite_fd* fd)
{
  TRY(fd, fclose)
  NOT_NULL(fd->stream);

  fd->stream->close();
  delete fd->stream;
  fd->stream    = NULL;
  fd->errorCode = 0;
  fd->errorString.clear();
  delete fd;
  CATCH(fd, fclose)
}



int dmlite_fstat(dmlite_fd* fd, struct stat* buf)
{
  TRY(fd, fstat)
  NOT_NULL(fd->stream);
  NOT_NULL(buf);
  *buf = fd->stream->fstat();
  CATCH(fd, fclose)
}



int dmlite_fseek(dmlite_fd* fd, off_t offset, int whence)
{
  TRY(fd, fseek)
  NOT_NULL(fd->stream);
  fd->stream->seek(offset, static_cast<dmlite::IOHandler::Whence>(whence));
  CATCH(fd, fseek)
}



off_t dmlite_ftell(dmlite_fd* fd)
{
  TRY(fd, ftell)
  NOT_NULL(fd->stream);
  return fd->stream->tell();
  CATCH(fd, ftell)
}



ssize_t dmlite_fread(dmlite_fd* fd, void* buffer, size_t count)
{
  TRY(fd, fread)
  NOT_NULL(fd->stream);
  NOT_NULL(buffer);
  return fd->stream->read((char*)buffer, count);
  CATCH_NEGATIVE(fd, fread)
}



ssize_t dmlite_fwrite(dmlite_fd* fd, const void* buffer, size_t count)
{
  TRY(fd, fwrite)
  NOT_NULL(fd->stream);
  NOT_NULL(buffer);
  return fd->stream->write((char*)buffer, count);
  CATCH_NEGATIVE(fd, fwrite)
}



ssize_t dmlite_freadv(dmlite_fd* fd, const struct iovec* vector, size_t count)
{
  TRY(fd, freadv)
  NOT_NULL(fd->stream);
  NOT_NULL(vector);
  return fd->stream->readv(vector, count);
  CATCH_NEGATIVE(fd, freadv)
}



ssize_t dmlite_fwritev(dmlite_fd* fd, const struct iovec* vector, size_t count)
{
  TRY(fd, fwritev)
  NOT_NULL(fd->stream);
  NOT_NULL(vector);
  return fd->stream->writev(vector, count);
  CATCH_NEGATIVE(fd, fwritev)
}



ssize_t dmlite_fpread(dmlite_fd* fd, void* buffer, size_t count, off_t offset)
{
  TRY(fd, fpread)
  NOT_NULL(fd->stream);
  NOT_NULL(buffer);
  return fd->stream->pread(buffer, count, offset);
  CATCH_NEGATIVE(fd, fpread)
}



ssize_t dmlite_fpwrite(dmlite_fd* fd, const void* buffer, size_t count, off_t offset)
{
  TRY(fd, fpwrite)
  NOT_NULL(fd->stream);
  NOT_NULL(buffer);
  return fd->stream->pwrite(buffer, count, offset);
  CATCH_NEGATIVE(fd, fwritev)
}



int dmlite_feof(dmlite_fd* fd)
{
  TRY(fd, feof)
  NOT_NULL(fd->stream);
  return fd->stream->eof();
  CATCH(fd, feof)
}



int dmlite_ferrno(dmlite_fd* fd)
{
  if (!fd || !fd->stream)
    return EFAULT;
  return fd->errorCode;
}



const char* dmlite_ferror(dmlite_fd* fd)
{
  if (!fd)
    return "The passed file descriptor is a NULL pointer";
  if (!fd->stream)
    return "The passed file descriptor was already closed";
  return fd->errorString.c_str();
}



int dmlite_donewriting(dmlite_context* context,
                       const dmlite_location* loc)
{
  TRY(context, donewriting)
  NOT_NULL(loc);
  dmlite::Location location;
  dmlite_clocation_to_cpplocation(loc, location);
  context->stack->getIODriver()->doneWriting(location);
  CATCH(context, donewriting)
}
