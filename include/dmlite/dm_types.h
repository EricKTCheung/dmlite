/** @file   include/dmlite/dm_types.h
 *  @brief  Types used by the dm API, common between C and C++.
 *  @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
 */
#ifndef DMLITE_TYPES_H
#define	DMLITE_TYPES_H

#include <dirent.h>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>

#ifndef HOST_NAME_MAX
# define HOST_NAME_MAX _POSIX_HOST_NAME_MAX
#endif
#define SCHEME_MAX        7
#define TOKEN_MAX        36
#define COMMENT_MAX     255
#define GROUP_NAME_MAX  255
#define USER_NAME_MAX   255
#define URI_MAX        8192
#define GUID_MAX         36
#define ACL_ENTRIES_MAX 300
#define ACL_SIZE         13
#define POLICY_MAX       16
#define POOL_MAX         16
#define FILESYSTEM_MAX   80
#define SUMTYPE_MAX       3
#define SUMVALUE_MAX     33

#define TYPE_EXPERIMENT 1
#define TYPE_USER       2

#define STATUS_ONLINE   '-'
#define STATUS_MIGRATED 'm'

/** Struct with basic information and detailed stats */
struct direntstat {
  struct dirent dirent; /**< Basic information about the directory */
  struct stat   stat;   /**< Detailed stats                        */
};

/** Struct with extended status information
 * @note It can be safely typecasted to struct stat.
 */
struct xstat {
  struct stat stat;
  ino_t       parent;
  short       type;
  char        status;
  char        name     [NAME_MAX];
  char        guid     [GUID_MAX];
  char        csumtype [SUMTYPE_MAX];
  char        csumvalue[SUMVALUE_MAX];
  char        acl      [ACL_ENTRIES_MAX * ACL_SIZE];
};
typedef struct xstat ExtendedStat;

/** Symbolic links */
struct symlink {
  uint64_t fileId;         /**< The file unique ID. */
  char     link[PATH_MAX]; /**< Where the link is pointing to. */
};
typedef struct symlink SymLink;

/** Simplified URI struct */
struct uri {
  char     scheme[SCHEME_MAX];
  char     host  [HOST_NAME_MAX];
  unsigned port;
  char     path  [PATH_MAX];
};
typedef struct uri Uri;

/** File replica */
struct filereplica {
  int64_t    replicaid;
  int64_t    fileid;
  char       status;
  struct uri location;
  char       unparsed_location[URI_MAX];
};
typedef struct filereplica FileReplica;

/** File system */
struct filesystem {
  char    poolname[POOL_MAX];
  char    server  [HOST_NAME_MAX];
  char    fs      [FILESYSTEM_MAX];
  int64_t capacity;
  int64_t free;
  int     status;
};
typedef struct filesystem FileSystem;

/** Pool */
struct pool {
  char               poolname[POOL_MAX];
  uint64_t           defsize;
  int                gc_start_thresh;
  int                gc_stop_thresh;
  int                def_lifetime;
  int                defpintime;
  int                max_lifetime;
  int                maxpintime;
  char               fss_policy[POLICY_MAX];
  char               gc_policy [POLICY_MAX];
  char               mig_policy[POLICY_MAX];
  char               rs_policy [POLICY_MAX];
  int                nbgids;
  gid_t             *gids;          /** restrict the pool to given group(s) */
  char               ret_policy;     /** retention policy: 'R', 'O' or 'C' */
  char               s_type;         /** space type: 'V', 'D' or 'P' */
  uint64_t           capacity;
  int64_t            free;
  struct filesystem *elemp;
  int                nbelem;
  int                next_elem;      /** next pool element to be used */
};
typedef struct pool Pool;

/** Struct to hold information about a user. */
struct userinfo {
  uid_t uid;
  char  name[255];
  char  ca  [255];
  int   banned;
};
typedef struct userinfo UserInfo;

/** Struct to hold information about a group. */
struct groupinfo {
  gid_t gid;
  char  name[255];
  int   banned;
};
typedef struct groupinfo GroupInfo;

#ifdef __cplusplus
/// Operator < for UserInfo (needed for sets)
inline bool operator < (const userinfo &a, const userinfo &b)
{
  return a.uid < b.uid;
}

/// Operator < for GroupInfo (needed for sets)
inline bool operator < (const groupinfo &a, const groupinfo &b)
{
  return a.gid < b.gid;
}
#endif

#endif	/* DMLITE_TYPES_H */
