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
#define POOL_TYPE_MAX    16
#define POOL_MAX         16
#define FILESYSTEM_MAX   80
#define SUMTYPE_MAX       3
#define SUMVALUE_MAX     33
#define SETNAME_MAX      36
#define QUERY_MAX      1024

#define TYPE_EXPERIMENT 1
#define TYPE_USER       2

#define STATUS_ONLINE   '-'
#define STATUS_MIGRATED 'm'


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
  ino_t inode;         /**< The file unique ID. */
  char  link[PATH_MAX]; /**< Where the link is pointing to. */
};
typedef struct symlink SymLink;

/** File replica */
struct filereplica {
  int64_t    replicaid;
  int64_t    fileid;
  int64_t    nbaccesses;
  time_t     atime;
  time_t     ptime;
  time_t     ltime;
  char       status;
  char       type;
  char       pool      [POOL_MAX];
  char       server    [HOST_NAME_MAX];
  char       filesystem[FILESYSTEM_MAX]; /**< Do we still want this? */
  char       rfn       [URI_MAX];        /**< Replica file name*/
  uint8_t    priority;                   /**< 0 highest priority. */
};
typedef struct filereplica FileReplica;

/** Used to hold a key-value pair */
struct keyvalue {
  const char *key;
  const char *value;
};

/** Physical location of a replica
 * When used from C, do not free it directly!
 * Check dm_location_free
 */
struct location {
  char             host [HOST_NAME_MAX];
  char             path [PATH_MAX];
  unsigned         nextra;
  struct keyvalue *extra;
  uint8_t          available;
  uint8_t          priority;
};

/** Simplified URL struct */
struct url {
  char     scheme[SCHEME_MAX];
  char     host  [HOST_NAME_MAX];
  unsigned port;
  char     path  [PATH_MAX];
  char     query [QUERY_MAX];
};
typedef struct url Url;

/** Pool */
struct pool {
  char      pool_type[POOL_TYPE_MAX];
  char      pool_name[POOL_MAX];
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

/* Macros to keep strings coherent */
#define CRED_MECH_NONE "NONE"
#define CRED_MECH_X509 "X509"

/** Security credentials */
struct credentials {
  const char*  mech;
  const char*  client_name;
  const char*  remote_addr;
  const char** fqans;
  unsigned     nfqans;
  const char*  session_id;

  void*    cred_data;
};
typedef struct credentials Credentials;

/* ACL masks */
#define ACL_USER_OBJ  1
#define ACL_USER      2
#define ACL_GROUP_OBJ 3
#define ACL_GROUP     4
#define ACL_MASK      5
#define ACL_OTHER     6
#define ACL_DEFAULT   0x20

/** ACL struct. */
struct dm_acl {
  unsigned char type;
  unsigned char perm;
  uint32_t      id;
};
typedef struct dm_acl Acl;

/** Union to handle different types of values.
 * @note Pointers are owned by caller!
 */
union value {
  char        c;
  uint64_t    u64;
  int64_t     i64;
  struct {
    size_t      size;
    uint64_t   *u64v;
    int64_t    *i64v;
    char       *str;
    const char *cstr;
  } array;
};
typedef union value Value;

/** C++ helpers */
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

// Wrap some structs to make handling easier
class Location: public location {
public:
  Location();
  Location(const Location&);
  Location(const struct location&);
  Location(const Url& url);
  
  /// @param npairs Number of key-value pairs following
  /// @note The key-value pairs must be passed as key1, value1, key2, value2...
  /// @example Location("localhost", "/", true, 2, "Key", "Value", "Key2", "Value2");
  Location(const char* host, const char* path, bool available, unsigned npairs, ...);
  ~Location();
  
  /// Operator =
  const Location& operator = (const Location&);
};
#endif

#endif	/* DMLITE_TYPES_H */
