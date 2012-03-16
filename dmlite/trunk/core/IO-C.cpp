/// @file   core/IO-C.cpp
/// @brief  C wrapper for dmlite::IOFactory.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/dmlite.h>
#include <fcntl.h>
#include "Private.h"

struct dm_fd {
  dm_context*    context;
  std::iostream* stream;
};



dm_fd* dm_fopen(dm_context* context, const char* path, int flags)
{
  std::ios_base::openmode openmode;

  switch (flags & 07) {
    case O_WRONLY:
      openmode = std::ios_base::out;
      break;
    case O_RDWR:
      openmode = std::ios_base::in | std::ios_base::out;
      break;
    default:
      openmode = std::ios_base::in;
      break;
  }
  
  if (flags & O_APPEND)
    openmode |= std::ios_base::app;
  if (flags & O_TRUNC)
    openmode |= std::ios_base::trunc;

  TRY(context, fopen)
  NOT_NULL(path);
  std::iostream* stream = context->io->createIO(path, openmode);
  dm_fd* iofd = new dm_fd();
  iofd->context = context;
  iofd->stream  = stream;
  return iofd;
  CATCH_POINTER(context, fopen)
}



int dm_fclose(dm_fd* fd)
{
  if (fd == NULL)
    return DM_NULL_POINTER;
  
  fd->stream->flush();
  delete fd->stream;
  delete fd;
  return 0;
}



int dm_fseek(dm_fd* fd, long offset, int whence)
{
  if (fd == NULL)
    return DM_NULL_POINTER;

  std::ios_base::seekdir seek;

  switch (whence) {
    case SEEK_SET:
      seek = std::ios_base::beg;
      break;
    case SEEK_CUR:
      seek = std::ios_base::cur;
      break;
    case SEEK_END:
      seek = std::ios_base::end;
      break;
    default:
      return DM_INVALID_VALUE;
  }

  fd->stream->seekg(offset, seek);
  fd->stream->seekp(offset, seek);
  return 0;
}



int dm_fread(dm_fd* fd, void* buffer, size_t count)
{
  if (fd == NULL)
    return DM_NULL_POINTER;

  size_t r = fd->stream->read((char*)buffer, count).gcount();

  if (fd->stream->bad()) {
    fd->context->errorCode   = DM_INTERNAL_ERROR;
    fd->context->errorString = "Could not read from the file descriptor";
    return -1;
  }

  return r;
}



int dm_fwrite(dm_fd* fd, const void* buffer, size_t count)
{
  if (fd == NULL)
    return DM_NULL_POINTER;

  fd->stream->write((char*)buffer, count);
  if (fd->stream->bad()) {
    fd->context->errorCode   = DM_INTERNAL_ERROR;
    fd->context->errorString = "Could not write to the file descriptor";
    return -1;
  }
  return count;
}



int dm_feof(dm_fd* fd)
{
  if (fd == NULL)
    return DM_NULL_POINTER;

  return fd->stream->eof();
}
