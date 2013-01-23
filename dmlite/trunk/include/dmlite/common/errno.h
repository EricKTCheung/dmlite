/** @file   include/dmlite/common/errno.h
 *  @brief  Error codes.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_COMMON_ERRNO_H
#define DMLITE_COMMON_ERRNO_H

/* For easy of use, some error codes are reused for lower bytes.
 * Plugins may use error codes from these headers ORing the type byte
 * (i.e. DMLITE_SYSTEM_ERROR | EDQUOT), even though there are no macros
 * for all of them.
 */
#include <errno.h>

#define DMLITE_SUCCESS 0

/* Error codes need to be stored in an integer type
 * of at least 4 bytes.
 * Highest byte categorizes the error type */
#define DMLITE_USER_ERROR          0x00000000
#define DMLITE_SYSTEM_ERROR        0x01000000
#define DMLITE_CONFIGURATION_ERROR 0x02000000
#define DMLITE_DATABASE_ERROR      0x03000000

/* Macros to extract error type and errno*/
#define DMLITE_ETYPE(e) ((e) & 0xFF000000)
#define DMLITE_ERRNO(e) ((e) & 0x00FFFFFF)

/* Macros to generate a dmlite-like error code from POSIX error code
 * Pass user errors directly as the POSIX value (or dmlite additional error codes)
 */
#define DMLITE_SYSERR(e) ((e) | DMLITE_SYSTEM_ERROR)
#define DMLITE_CFGERR(e) ((e) | DMLITE_CONFIGURATION_ERROR)
#define DMLITE_FCTERR(e) ((e) | DMLITE_FACTORY_ERROR)
#define DMLITE_DBERR(e)  ((e) | DMLITE_DATABASE_ERROR)

/* Aditional error codes */

#define DMLITE_UNKNOWN_ERROR          256
#define DMLITE_UNEXPECTED_EXCEPTION   257
#define DMLITE_INTERNAL_ERROR         258
/* 259 - 269 reserved for future use */
#define DMLITE_NO_SUCH_SYMBOL         270
#define DMLITE_API_VERSION_MISMATCH   271
#define DMLITE_NO_POOL_MANAGER        272
#define DMLITE_NO_CATALOG             273
#define DMLITE_NO_INODE               274
#define DMLITE_NO_AUTHN               275
#define DMLITE_NO_IO                  276
/* 278 - 299 reserved for future use */
#define DMLITE_NO_SECURITY_CONTEXT    300
#define DMLITE_EMPTY_SECURITY_CONTEXT 301
/* 302 - 349 reserved for future use */
#define DMLITE_MALFORMED              350
#define DMLITE_UNKNOWN_KEY            351
/* 353 - 399 reserved for future use */
#define DMLITE_NO_COMMENT             400
#define DMLITE_NO_REPLICAS            401
#define DMLITE_NO_SUCH_REPLICA        402
/* 403 - 499 reserved for future use */
#define DMLITE_NO_USER_MAPPING        500
#define DMLITE_NO_SUCH_USER           501
#define DMLITE_NO_SUCH_GROUP          502
#define DMLITE_INVALID_ACL            504
/* 505 - 599 reserved for future use */
#define DMLITE_UNKNOWN_POOL_TYPE      600
#define DMLITE_NO_SUCH_POOL           601

#endif /* DMLITE_COMMON_ERRNO_H */
