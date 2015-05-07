/** @file   include/dmlite/c/utils.h
 *  @brief  C wrapper for DMLite utils.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_CHECKSUMS_H
#define DMLITE_CHECKSUMS_H

#include <stddef.h>
#include "io.h"

#ifdef  __cplusplus
extern "C" {
#endif

/**
 * @brief           Puts into output the full name of the checksum algorithm
 *                  specified with shortName.
 * @param shortName The checksum short name (CS, AD, MD)
 * @param output    The full name will be put here.
 * @param osize     The size of the buffer pointed by output.
 * @return          The same value as the pointer output
 */
char* dmlite_checksum_full_name(const char* shortName, char* output,
                                size_t osize);
/**
 * @brief           Puts into output the short name of the checksum algorithm
 *                  specified with longName.
 * @param shortName The checksum long name (MD5, ADLER32, ...)
 * @param output    The short name will be put here.
 * @param osize     The size of the buffer pointed by output.
 * @return          The same value as the pointer output
 */
char* dmlite_checksum_short_name(const char* longName, char* output,
                                 size_t osize);

/**
 * @brief         Generated the MD5 checksum of the given file.
 * @param fd      The file descriptor where to read the data to digest.
 * @param offset  Where to start to digest.
 * @param size    The number of bytes to digest. 0 means the whole file.
 * @param output  Where to put the resulting checksum (in hexadecimal)
 * @param outsize The size of the memory area pointed by output.
 * @return        0 on success, error code otherwise.
 */
int dmlite_checksum_md5(dmlite_fd* fd, off_t offset, off_t size,
                        char* output, size_t outsize);

/**
 * @brief         Generated the CRC32 checksum of the given file.
 * @param fd      The file descriptor where to read the data to digest.
 * @param offset  Where to start to digest.
 * @param size    The number of bytes to digest. 0 means the whole file.
 * @param output  Where to put the resulting checksum (in decimal)
 * @param outsize The size of the memory area pointed by output.
 * @return        0 on success, error code otherwise.
 */
int dmlite_checksum_crc32(dmlite_fd* fd, off_t offset, off_t size,
                          char* output, size_t outsize);

/**
 * @brief         Generated the Adler32 checksum of the given file.
 * @param fd      The file descriptor where to read the data to digest.
 * @param offset  Where to start to digest.
 * @param size    The number of bytes to digest. 0 means the whole file.
 * @param output  Where to put the resulting checksum (in hexadecimal)
 * @param outsize The size of the memory area pointed by output.
 * @return        0 on success, error code otherwise.
 */
int dmlite_checksum_adler32(dmlite_fd* fd, off_t offset, off_t size,
                            char* output, size_t outsize);

#ifdef  __cplusplus
}
#endif

#endif /* DMLITE_CHECKSUMS_H */
