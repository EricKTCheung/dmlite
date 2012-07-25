/** @file   include/dmlite/c/inode.h
 *  @brief  C wrapper for DMLite INode API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_INODE_H
#define DMLITE_INODE_H

#include <stdint.h>
#include "any.h"
#include "dmlite.h"
#include "utils.h"

#ifdef	__cplusplus
extern "C" {
#endif

/** Possible replica statuses */
enum dmlite_replica_status { kAvailable      = '-',
                             kBeingPopulated = 'P',
                             kToBeDeleted    = 'D'
                           };
/** Possible replica types */
enum dmlite_replica_type   { kVolatile  = 'V',
                             kPermanent = 'P'
                           };
/** A replica of a file */
typedef struct dmlite_replica {
  int64_t    replicaid;
  int64_t    fileid;

  int64_t    nbaccesses;
  time_t     atime;
  time_t     ptime;
  time_t     ltime;

  enum dmlite_replica_status status;
  enum dmlite_replica_type   type;

  char server[HOST_NAME_MAX];
  char rfn   [URL_MAX];
  
  dmlite_any_dict* extra; /**< If != NULL, additional metadata will be put here.
                           *   Caller is responsible for allocating and freeing*/
} dmlite_replica;

/** Posible file statuses */
enum dmlite_file_status { kOnline = '-',
                          kMigrated = 'm'
                        };
                        
/** File metadata */
typedef struct dmlite_xstat {
  ino_t              parent;
  struct stat        stat;
  dmlite_file_status status;
  char               name     [NAME_MAX];
  char               guid     [GUID_MAX];
  char               csumtype [CSUMTYPE_MAX];
  char               csumvalue[CSUMVALUE_MAX];
  char               acl      [ACL_ENTRIES_MAX * ACL_SIZE];
  
  dmlite_any_dict* extra; /**< If != NULL, additional metadata will be put here.
                           *   Caller is responsible for allocating and freeing*/
} dmlite_xstat;

/**
 * Do a stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dmlite_stati(dmlite_context* context, ino_t inode, struct stat* buf);

/**
 * Do an extended stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dmlite_statix(dmlite_context* context, ino_t inode, dmlite_xstat* buf);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_INODE_H */
