/// @file   core/IO-C.cpp
/// @brief  C wrapper for dmlite::IOFactory.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/io.h>
#include <dmlite/c/dmlite.h>
#include <dmlite/cpp/io.h>
#include <fcntl.h>
#include "Private.h"

struct dmlite_fd {
  dmlite_context*        context;
  dmlite::IOHandler* stream;
};



int dmlite_pstat(dmlite_context* context, const char* rfn, struct stat* st)
{
  TRY(context, pstat)
  NOT_NULL(rfn);
  NOT_NULL(st);
  
  *st = context->stack->getIODriver()->pStat(rfn);
  
  CATCH(context, pstat)
}



dmlite_fd* dmlite_fopen(dmlite_context* context, const char* path, int flags,
                        dmlite_any_dict* extra)
{
  TRY(context, fopen)
  NOT_NULL(path);
  dmlite::IODriver::OpenMode mode = static_cast<dmlite::IODriver::OpenMode>(flags);
  
  dmlite::IOHandler* stream;
  if (extra != NULL)
    stream = context->stack->getIODriver()->
                              createIOHandler(path, mode, extra->extensible);
  else
    stream = context->stack->getIODriver()->
                              createIOHandler(path, mode, dmlite::Extensible());
  
  dmlite_fd* iofd = new dmlite_fd();
  iofd->context = context;
  iofd->stream  = stream;
  return iofd;
  CATCH_POINTER(context, fopen)
}



int dmlite_fclose(dmlite_fd* fd)
{
  TRY(fd->context, fclose)
  NOT_NULL(fd);
  fd->stream->close();
  delete fd->stream;
  delete fd;
  CATCH(fd->context, fclose)
}



int dmlite_fseek(dmlite_fd* fd, long offset, int whence)
{
  TRY(fd->context, fseek)
  NOT_NULL(fd);
  fd->stream->seek(offset, static_cast<dmlite::IOHandler::Whence>(whence));
  CATCH(fd->context, fseek)
}



long dmlite_ftell(dmlite_fd* fd)
{
  TRY(fd->context, ftell)
  NOT_NULL(fd);
  return fd->stream->tell();
  CATCH(fd->context, ftell)
}



size_t dmlite_fread(dmlite_fd* fd, void* buffer, size_t count)
{
  TRY(fd->context, fread)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->read((char*)buffer, count);
  CATCH(fd->context, fread)
}



size_t dmlite_fwrite(dmlite_fd* fd, const void* buffer, size_t count)
{
  TRY(fd->context, fwrite)
  NOT_NULL(fd);
  NOT_NULL(buffer);
  return fd->stream->write((char*)buffer, count);
  CATCH(fd->context, fwrite)
}



int dmlite_feof(dmlite_fd* fd)
{
  TRY(fd->context, feof)
  NOT_NULL(fd);
  return fd->stream->eof();
  CATCH(fd->context, feof)
}
