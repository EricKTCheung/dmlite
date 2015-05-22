/// @file   FunctionWrapper.cpp
/// @brief  Function wrapper.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <cstdlib>
#include <sys/stat.h>
#include <dpm_api.h>
#include <dpns_api.h>
#include "FunctionWrapper.h"



static int Serrno2Code(int serr)
{
  
  switch (serr)
  {
    case SENOSHOST:
      return EHOSTUNREACH;
    case SENOSSERV: case SECOMERR: case ENSNACT:
      return ECOMM;
    case SELOOP:
      return ELOOP;
    case SETIMEDOUT:
      return ETIMEDOUT;
    default:
      return serr;
  }
}



void dmlite::ThrowExceptionFromSerrno(int serr, const char* extra) throw(DmException)
{
  if (extra == 0) extra = "";
  throw DmException(Serrno2Code(serr), "%s: %s", sstrerror(serr), extra);
}




static pthread_once_t createKeyOnce = PTHREAD_ONCE_INIT;
static pthread_key_t bufferKey;
static const size_t BUFFER_SIZE = 128;


static void createKey(void)
{
  pthread_key_create(&bufferKey, ::free);
}


void dmlite::wrapperSetBuffers(void)
{
  pthread_once(&createKeyOnce, createKey);
  char* buffer = static_cast<char*>(pthread_getspecific(bufferKey));
  if (!buffer) {
    buffer = static_cast<char*>(::malloc(BUFFER_SIZE));
    dpns_seterrbuf(buffer, BUFFER_SIZE);
    dpm_seterrbuf(buffer, BUFFER_SIZE);
    pthread_setspecific(bufferKey, buffer);
  }
}
