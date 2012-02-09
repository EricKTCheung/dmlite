/** @file   include/dm/dm_errno.h
 *  @brief  Error codes.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_ERRNO_H
#define	DMLITE_ERRNO_H

#define DM_NO_ERROR             0x0000
#define DM_UNKNOWN_ERROR        0x0001
#define DM_INTERNAL_ERROR       0x0002
#define DM_NO_SUCH_SYMBOL       0x0003
#define DM_NO_FACTORY           0x0004
#define DM_UNEXPECTED_EXCEPTION 0x0005
#define DM_RESOURCE_UNAVAILABLE 0x0006
#define DM_NO_POOL_MANAGER      0x0007
#define DM_NULL_POINTER         0x1000
#define DM_OUT_OF_MEMORY        0x1001
#define DM_NO_CATALOG           0x1002
#define DM_API_VERSION_MISMATCH 0x1003
#define DM_MALFORMED_CONF       0x1004
#define DM_UNKNOWN_HOST         0x1010
#define DM_CONNECTION_ERROR     0x1011
#define DM_NO_SPACE_LEFT        0x1012
#define DM_SERVICE_UNAVAILABLE  0x1013
#define DM_QUERY_FAILED         0x1020
#define DM_UNKNOWN_OPTION       0x1021
#define DM_NOT_IMPLEMENTED      0x1022
#define DM_NO_REPLICAS          0x1023
#define DM_PUT_ERROR            0x1024
#define DM_BAD_OPERATION        0x1025
#define DM_NO_SUCH_FILE         0x1026
#define DM_FORBIDDEN            0x1027
#define DM_EXISTS               0x1028
#define DM_NAME_TOO_LONG        0x1029
#define DM_INVALID_VALUE        0x1030
#define DM_IS_DIRECTORY         0x1031
#define DM_NOT_DIRECTORY        0x1032
#define DM_BAD_DESCRIPTOR       0x1033
#define DM_TOO_MANY_SYMLINKS    0x1034
#define DM_NO_USER_MAPPING      0x1035
#define DM_NO_SUCH_USER         0x1036
#define DM_NO_SUCH_GROUP        0x1037
#define DM_NO_COMMENT           0x1038
#define DM_NO_SUCH_FS           0x1039
#define DM_IS_CWD               0x103A

#endif	/* DMLITE_ERRNO_H */
