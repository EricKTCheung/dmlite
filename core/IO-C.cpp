/// @file   core/IO-C.cpp
/// @brief  C wrapper for dmlite::IOFactory.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/dmlite.h>
#include <fcntl.h>
#include <map>
#include "Private.h"

struct dm_fd {
  dm_context*        context;
  dmlite::IOHandler* stream;
};



int dm_pstat(dm_context* context, const char* rfn, struct stat* st)
{
  TRY(context, pstat)
  NOT_NULL(rfn);
  NOT_NULL(st);
  
  *st = context->stack->getIODriver()->pStat(rfn);
  
  CATCH(context, pstat)
}



dm_fd* dm_fopen(dm_context* context, const char* path, int flags,
                unsigned nextras, struct keyvalue* extrasp)
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
   
  std::map<std::string, std::string> extras;
  
  for (unsigned i = 0; i < nextras; ++i)
    extras.insert(std::pair<std::string, std::string>(extrasp[i].key, extrasp[i].value));
  
  dmlite::IOHandler* stream = context->stack->
                                 getIODriver()->
                                 createIOHandler(path, openmode, extras);
  dm_fd* iofd = new dm_fd();
  iofd->context = context;
  iofd->stream  = stream;
  return iofd;
  CATCH_POINTER(context, fopen)
}



int dm_fclose(dm_fd* fd)
{
  TRY(fd->context, fclose)
  NOT_NULL(fd);
  fd->stream->close();
  delete fd->stream;
  delete fd;
  CATCH(fd->context, fclose)
}



int dm_fseek(dm_fd* fd, long offset, int whence)
{
  TRY(fd->context, fseek)
  NOT_NULL(fd);

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
  
  fd->stream->seek(offset, seek);
  CATCH(fd->context, fseek)
}



long dm_ftell(dm_fd* fd)
{
  TRY(fd->context, ftell)
  NOT_NULL(fd);
  return fd->stream->tell();
  CATCH(fd->context, ftell)
}



size_t dm_fread(dm_fd* fd, void* buffer, size_t count)
{
  TRY(fd->context, fread)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->read((char*)buffer, count);
  CATCH(fd->context, fread)
}



size_t dm_fwrite(dm_fd* fd, const void* buffer, size_t count)
{
  TRY(fd->context, fwrite)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->write((char*)buffer, count);
  CATCH(fd->context, fwrite)
}



int dm_feof(dm_fd* fd)
{
  TRY(fd->context, feof)
  NOT_NULL(fd);
  return fd->stream->eof();
  CATCH(fd->context, feof)
}
