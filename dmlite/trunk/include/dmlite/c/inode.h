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
  ino_t                   parent;
  struct stat             stat;
  enum dmlite_file_status status;
  char                    name     [NAME_MAX];
  char                    guid     [GUID_MAX];
  char                    csumtype [CSUMTYPE_MAX];
  char                    csumvalue[CSUMVALUE_MAX];
  char                    acl      [ACL_ENTRIES_MAX * ACL_SIZE];
  
  dmlite_any_dict* extra; /**< If != NULL, additional metadata will be put here.
                           *   Caller is responsible for allocating and freeing*/
} dmlite_xstat;

/** Opaque directory handler */
typedef struct dmlite_idir dmlite_idir;

int dmlite_icreate(dmlite_context* context, const dmlite_xstat* f);

int dmlite_isymlink(dmlite_context* context, ino_t inode, const char* link);

int dmlite_iunlink(dmlite_context* context, ino_t inode);

int dmlite_imove(dmlite_context* context, ino_t inode, ino_t dest);

int dmlite_irename(dmlite_context* context, ino_t inode, const char* name);

/**
 * Do a stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dmlite_istat(dmlite_context* context, ino_t inode, struct stat* buf);

/**
 * Do an extended stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dmlite_istatx(dmlite_context* context, ino_t inode, dmlite_xstat* buf);

int dmlite_istatx_by_name(dmlite_context* context, ino_t parent, const char* name,
                          dmlite_xstat* buf);

int dmlite_ireadlink(dmlite_context* context, ino_t inode,
                     char* path, size_t bufsize);

int dmlite_iaddreplica(dmlite_context* context, const dmlite_replica* replica);

int dmlite_ideletereplica(dmlite_context* context, const dmlite_replica* replica);

int dmlite_igetreplica(dmlite_context* context, int64_t rid, dmlite_replica* buf);

int dmlite_igetreplicas(dmlite_context* context, ino_t inode,
                        unsigned* nreplicas, dmlite_replica** replicas);

int dmlite_iutime(dmlite_context* context, ino_t inode,
                  const struct utimbuf* buf);

int dmlite_isetmode(dmlite_context* context, ino_t inode, uid_t uid, gid_t gid,
                    mode_t mode, unsigned nentries, dmlite_aclentry* acl);

int dmlite_isetsize(dmlite_context* context, ino_t inode, size_t size);

int dmlite_isetchecksum(dmlite_context* context, ino_t inode,
                        const char* csumtype, const char* csumvalue);

int dmlite_igetcomment(dmlite_context* context, ino_t inode,
                       char* comment, size_t bufsize);

int dmlite_isetcomment(dmlite_context* context,  ino_t inode,
                       const char* comment);

int dmlite_ideletecomment(dmlite_context* context, ino_t inode);

int dmlite_isetguid(dmlite_context* context, ino_t inode, const char* guid);

dmlite_idir* dmlite_iopendir(dmlite_context* context, ino_t inode);

int dmlite_iclosedir(dmlite_context* context, dmlite_idir* dir);

dmlite_xstat* dmlite_ireaddirx(dmlite_context* context, dmlite_idir* dir);

struct dirent* dmlite_ireaddir(dmlite_context* context, dmlite_idir* dir);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_INODE_H */
