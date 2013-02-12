/// @file   c/Checksums-C.cpp
/// @brief  C wrapper for checksum methods
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch
#include <dmlite/cpp/utils/checksums.h>
#include <dmlite/c/checksums.h>
#include "Private.h"



char* dmlite_checksum_full_name(const char* shortName, char* output,
                                size_t osize)
{
  std::string fullName = dmlite::checksums::fullChecksumName(shortName);
  return ::strncpy(output, fullName.c_str(), osize);
}



char* dmlite_checksum_short_name(const char* longName, char* output,
                                 size_t osize)
{
  std::string shortName = dmlite::checksums::shortChecksumName(longName);
  return ::strncpy(output, shortName.c_str(), osize);
}



int dmlite_checksum_md5(dmlite_fd* fd, off_t offset, off_t size,
                        char* output, size_t outsize)
{
  TRY(fd, checksum_md5)
  std::string digest = dmlite::checksums::md5(fd->stream, offset, size);
  ::strncpy(output, digest.c_str(), outsize);
  return 0;
  CATCH(fd, checksum_md5)
}



int dmlite_checksum_crc32(dmlite_fd* fd, off_t offset, off_t size,
                          char* output, size_t outsize)
{
  TRY(fd, checksum_md5)
  std::string digest = dmlite::checksums::crc32(fd->stream, offset, size);
  ::strncpy(output, digest.c_str(), outsize);
  return 0;
  CATCH(fd, checksum_md5)
}



int dmlite_checksum_adler32(dmlite_fd* fd, off_t offset, off_t size,
                            char* output, size_t outsize)
{
  TRY(fd, checksum_md5)
  std::string digest = dmlite::checksums::adler32(fd->stream, offset, size);
  ::strncpy(output, digest.c_str(), outsize);
  return 0;
  CATCH(fd, checksum_md5)
}
