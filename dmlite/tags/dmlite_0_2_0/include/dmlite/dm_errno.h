/** @file   include/dmlite/dm_errno.h
 *  @brief  Error codes.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_ERRNO_H
#define	DMLITE_ERRNO_H

#define DM_NO_ERROR             0x0000

#define DM_UNKNOWN_ERROR        0x0001
#define DM_UNEXPECTED_EXCEPTION 0x0002
#define DM_INTERNAL_ERROR       0x0003
#define DM_NO_SUCH_SYMBOL       0x0004
#define DM_API_VERSION_MISMATCH 0x0005
#define DM_NO_FACTORY           0x0006
#define DM_NO_POOL_MANAGER      0x0007
#define DM_NO_CATALOG           0x0008

#define DM_MALFORMED_CONF       0x0100
#define DM_UNKNOWN_OPTION       0x0101

#define DM_UNKNOWN_HOST         0x0200
#define DM_CONNECTION_ERROR     0x0201
#define DM_SERVICE_UNAVAILABLE  0x0202
#define DM_QUERY_FAILED         0x0203

#define DM_NOT_IMPLEMENTED      0x1001
#define DM_NULL_POINTER         0x1002
#define DM_BAD_OPERATION        0x1003
#define DM_NO_SUCH_FILE         0x1004
#define DM_BAD_DESCRIPTOR       0x1005
#define DM_RESOURCE_UNAVAILABLE 0x1006
#define DM_OUT_OF_MEMORY        0x1007
#define DM_FORBIDDEN            0x1008
#define DM_EXISTS               0x1009
#define DM_NOT_DIRECTORY        0x100A
#define DM_IS_DIRECTORY         0x100B
#define DM_INVALID_VALUE        0x100C
#define DM_NO_SPACE_LEFT        0x100D
#define DM_NAME_TOO_LONG        0x100E
#define DM_TOO_MANY_SYMLINKS    0x100F
#define DM_NO_COMMENT           0x1010
#define DM_NO_REPLICAS          0x1011
#define DM_PUT_ERROR            0x1012
#define DM_IS_CWD               0x1013

#define DM_NO_USER_MAPPING      0x1101
#define DM_NO_SUCH_USER         0x1102
#define DM_NO_SUCH_GROUP        0x1103

#define DM_INVALID_ACL          0x1201

#define DM_NO_SUCH_FS           0x2001

#endif	/* DMLITE_ERRNO_H */
