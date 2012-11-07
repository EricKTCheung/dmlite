/// @file   core/IO-C.cpp
/// @brief  C wrapper for dmlite::IOFactory.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/io.h>
#include <dmlite/c/dmlite.h>
#include <dmlite/cpp/io.h>
#include <fcntl.h>
#include <stdarg.h>
#include "Private.h"

struct dmlite_fd {
  dmlite_context*        context;
  dmlite::IOHandler* stream;
};



dmlite_fd* dmlite_fopen(dmlite_context* context, const char* path, int flags,
                        const dmlite_any_dict* extra, ...)
{
  TRY(context, fopen)
  NOT_NULL(path);
  
  dmlite::IOHandler* stream;
  
  if (flags & O_CREAT) {
    va_list args;
    va_start(args, extra);
    mode_t mode = va_arg(args, mode_t);
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
  iofd->context = context;
  iofd->stream  = stream;
  return iofd;
  CATCH_POINTER(context, fopen)
}



int dmlite_fclose(dmlite_fd* fd)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fclose)
  NOT_NULL(fd);
  fd->stream->close();
  delete fd->stream;
  ::memset(fd, 0, sizeof(dmlite_fd));
  delete fd;
  CATCH(fd->context, fclose)
}



int dmlite_fstat(dmlite_fd* fd, struct stat* buf)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fstat)
  NOT_NULL(fd);
  NOT_NULL(buf);
  *buf = fd->stream->fstat();
  CATCH(fd->context, fclose)
}



int dmlite_fseek(dmlite_fd* fd, long offset, int whence)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fseek)
  NOT_NULL(fd);
  fd->stream->seek(offset, static_cast<dmlite::IOHandler::Whence>(whence));
  CATCH(fd->context, fseek)
}



long dmlite_ftell(dmlite_fd* fd)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, ftell)
  NOT_NULL(fd);
  return fd->stream->tell();
  CATCH(fd->context, ftell)
}



size_t dmlite_fread(dmlite_fd* fd, void* buffer, size_t count)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fread)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->read((char*)buffer, count);
  CATCH(fd->context, fread)
}



size_t dmlite_fwrite(dmlite_fd* fd, const void* buffer, size_t count)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fwrite)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->write((char*)buffer, count);
  CATCH(fd->context, fwrite)
}



size_t dmlite_freadv(dmlite_fd* fd, const struct iovec* vector, size_t count)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, freadv)
  NOT_NULL(fd);
  NOT_NULL(vector);
  return fd->stream->readv(vector, count);
  CATCH(fd->context, freadv)
}



size_t dmlite_fwritev(dmlite_fd* fd, const struct iovec* vector, size_t count)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, fwritev)
  NOT_NULL(fd);
  NOT_NULL(vector);
  return fd->stream->writev(vector, count);
  CATCH(fd->context, fwritev)
}



int dmlite_feof(dmlite_fd* fd)
{
  if (!fd)
    return DMLITE_SYSERR(EFAULT);
  TRY(fd->context, feof)
  NOT_NULL(fd);
  return fd->stream->eof();
  CATCH(fd->context, feof)
}



int dmlite_donewriting(dmlite_context* context,
                       const char* pfn,
                       const dmlite_any_dict* extra)
{
  TRY(context, donewriting)
  NOT_NULL(pfn);  
  if (extra != NULL)
    context->stack->getIODriver()->doneWriting(pfn, extra->extensible);
  else
    context->stack->getIODriver()->doneWriting(pfn, extra->extensible);
  CATCH(context, donewriting)
}
