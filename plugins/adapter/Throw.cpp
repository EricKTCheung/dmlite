/// @file   plugins/adapter/Serrno2Code.cpp
/// @brief  Maps the serrno codes to DM_ERROR_*
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/dm_exceptions.h>
#include <errno.h>
#include <serrno.h>

#include "Adapter.h"

static int Serrno2Code(int serr)
{
  switch (serr)
  {
    case EBADF:
      return DM_BAD_DESCRIPTOR;
    case EINVAL:
      return DM_INVALID_VALUE;
    case ENOMEM:
      return DM_OUT_OF_MEMORY;
    case EPERM:
      return DM_BAD_OPERATION;
    case ENOENT:
      return DM_NO_SUCH_FILE;
    case EACCES:
      return DM_FORBIDDEN;
    case EFAULT:
      return DM_NULL_POINTER;
    case EEXIST:
      return DM_EXISTS;
    case EISDIR:
      return DM_IS_DIRECTORY;
    case ENOTDIR:
      return DM_NOT_DIRECTORY;
    case ENOSPC:
      return DM_NO_SPACE_LEFT;
    case ENAMETOOLONG:
      return DM_NAME_TOO_LONG;
    case SENOSHOST:
      return DM_UNKNOWN_HOST;
    case SENOSSERV: case SECOMERR: case ENSNACT:
      return DM_CONNECTION_ERROR;
    case SELOOP:
      return DM_TOO_MANY_SYMLINKS;
    case SETIMEDOUT:
      return DM_SERVICE_UNAVAILABLE;
    default:
      return DM_UNKNOWN_ERROR;
  }
}



void dmlite::ThrowExceptionFromSerrno(int serr) throw(DmException)
{
  throw DmException(Serrno2Code(serr), sstrerror(serr));
}



int dmlite::wrapCall(int r) throw (DmException)
{
  if (r < 0)
    ThrowExceptionFromSerrno(serrno);
  return r;
}



void* dmlite::wrapCall(void* p) throw (DmException)
{
  if (p == NULL && serrno != 0)
    ThrowExceptionFromSerrno(serrno);
  return p;
}
