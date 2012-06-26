/** @file   include/dmlite/c/dm_catalog.h
 *  @brief  C wrapper for DMLite Catalog API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_CATALOG_H
#define	DM_CATALOG_H

#include "dmlite.h"

#ifdef	__cplusplus
extern "C" {
#endif

/**
 * Change the working dir.
 * @param context The DM context.
 * @param path    The new working dir.
 * @return        0 on success, error code otherwise.
 */
int dm_chdir(dm_context* context, const char* path);

/**
 * Get the current working directory.
 * @param context The DM context.
 * @param buffer  If not NULL, the path will be stored here. <b>malloc</b> will be used otherwise.
 * @param size    The buffer size.
 * @return        A pointer to a string with the current working dir.
 */
char* dm_getcwd(dm_context* context, char* buffer, size_t size);

/**
 * Do a stat of a file or directory.
 * @param context The DM context.
 * @param path    The path.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 */
int dm_stat(dm_context* context, const char* path, struct stat* buf);

/**
 * Do a stat of a file, directory, or symbolic link (does not follow).
 * @param context The DM context.
 * @param path    The path.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 */
int dm_lstat(dm_context* context, const char* path, struct stat* buf);

/**
 * Do an extended stat of a file, directory or symbolic link.
 * @param context The DM context.
 * @param path    The path.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 */
int dm_xstat(dm_context* context, const char* path, struct xstat* buf);

/**
 * Add a new replica to an entry.
 * @param context    The DM context.
 * @param guid       The Grid Unique IDentifier of the file. It can be NULL.
 * @param id         The file unique ID within the server.
 * @param server     The SE or disk hostname where the replica is (if NULL, it will be retrieved from the surl).
 * @param surl       Site URL (SE) or physical (Disk) path of the replica.
 * @param status     '-' available; 'P' being populated; 'D' being deleted.
 * @param fileType   'V' volatile; 'D' durable; 'P' permanent.
 * @param poolName   The disk pool (only makes sense for DPM hosts).
 * @param fileSystem The filesystem (only makes sense for DPM hosts).
 * @return           0 on success, error code otherwise.
 */
int dm_addreplica(dm_context* context, const char* guid, int64_t id,
                   const char* server, const char* surl, const char status,
                   const char fileType, const char* poolName,
                   const char* fileSystem);

/**
 * Delete a replica.
 * @param context The DM context.
 * @param guid    The Grid Unique IDentifier of the file.
 * @param id      The file unique ID within the server.
 * @param surl    Site URL (SE) or physical (Disk) path of the replica.
 * @return        0 on success, error code otherwise.
 */
int dm_delreplica(dm_context* context, const char* guid, int64_t id,
                  const char* surl);

/**
 * Get the replicas of a file.
 * @param context      The DM context.
 * @param path         The logical file name.
 * @param nReplicas    The number of entries will be put here.
 * @param fileReplicas An array with nEntries elements will be stored here. <b>Use dm_freereplicas to free it.</b>
 * @return             0 on success, error code otherwise.
 */
int dm_getreplicas(dm_context* context, const char* path, int *nReplicas,
                   struct filereplica** fileReplicas);

/**
 * Free a replica list.
 * @param context      The DM context.
 * @param nReplicas    The number of replicas contained in the array.
 * @param fileReplicas The array to free.
 * @return             0 on success, error code otherwise.
 */
int dm_freereplicas(dm_context* context, int nReplicas, struct filereplica* fileReplicas);

/**
 * Get a single replica (synchronous).
 * @param context The DM context.
 * @param path    The logical file name.
 * @param loc     The pointer will be set to a struct location. Call dm_freelocation to free.
 * @return        0 on success, error code otherwise.
 */
int dm_get(dm_context* context, const char* path, struct location** loc);

/**
 * Get the location of a replica.
 * @param context The DM context.
 * @param replica The replica to translate.
 * @param loc     The pointer will be set to a struct location. Call dm_freelocation to free.
 * @return        0 on success, error code otherwise.
 */
int dm_getlocation(dm_context* context, const FileReplica* replica, struct location** loc);

/**
 * Free a location struct.
 * @param context The DM context.
 * @param loc     The struct to free.
 * @return        0 on success, error code otherwise.
 */
int dm_freelocation(dm_context* context, struct location* loc);

/**
 * Remove a file.
 * @param context The DM context.
 * @param path    The logical file name.
 * @return        0 on success, error code otherwise.
 */
int dm_unlink(dm_context* context, const char* path);


/**
 * Create a file in the catalog (no replicas).
 * @param context The DM context.
 * @param path    The logical file name.
 * @param mode    The creation mode.
 * @return        0 on success, error code otherwise.
 */
int dm_create(dm_context* context, const char* path, mode_t mode);

/**
 * Put a file (synchronous).
 * @param context The DM context.
 * @param path    The logical file name to put.
 * @param loc     The pointer will be set to a struct location. Call dm_freelocation to free.
 * @return        0 on success, error code otherwise.
 */
int dm_put(dm_context* context, const char* path, struct location** loc);

/**
 * Put a file (synchronous).
 * @param context The DM context.
 * @param path    The logical file name to put.
 * @param loc     The pointer will be set to a struct location. Call dm_freelocation to free.
 * @param guid    The Grid Unique ID.
 * @return        0 on success, error code otherwise.
 */
int dm_putg(dm_context* context, const char* path, struct location** loc, const char* guid);

/**
 * Finish a PUT request.
 * @param context The DM context.
 * @param host    The host where the replica is.
 * @param rfn     The replica file name.
 * @param nextras The number of extra parameters returned by put.
 * @param extras  The extra parameters returned by put.
 * @return        0 on success, error code otherwise.
 */
int dm_putdone(dm_context* context, const char*host, const char* rfn, unsigned nextras, struct keyvalue* extras);

/**
 * Change the mode of a file or directory.
 * @param context The DM context.
 * @param path    The logical path.
 * @param mode    The new mode.
 * @return        0 on success, error code otherwise.
 */
int dm_chmod(dm_context* context, const char* path, mode_t mode);

/**
 * Change the owner of a file or directory.
 * @param context The DM context.
 * @param path    The logical path.
 * @param newUid  The new owner.
 * @param newGid  The new group.
 * @return        0 on success, error code otherwise.
 */
int dm_chown(dm_context* context, const char* path, uid_t newUid, gid_t newGid);

/**
 * Change the owner of a file, directory or symlink (does not follow).
 * @param context The DM context.
 * @param path    The logical path.
 * @param newUid  The new owner.
 * @param newGid  The new group.
 * @return        0 on success, error code otherwise.
 */
int dm_lchown(dm_context* context, const char* path, uid_t newUid, gid_t newGid);

/**
 * Change the ACL of a file.
 * @param context  The DM context.
 * @param path     The logical path.
 * @param nEntries The number of entries in the acl array.
 * @param acl      An ACL array.
 * @return         0 on success, error code otherwise.
 */
int dm_setacl(dm_context* context, const char* path, int nEntries, struct dm_acl* acl);

/**
 * Change access and/or modification time
 * @param context The DM context.
 * @param path    The file path.
 * @param buf     A struct holding the new times.
 * @return        0 on success, error code otherwise.
 */
int dm_utime(dm_context* context, const char* path, const struct utimbuf* buf);

/**
 * Get the comment associated with a file.
 * @param context The DM context.
 * @param path    The logical path.
 * @param comment Where to put the retrieved comment. It must be at least of size COMMENT_MAX.
 * @return        0 on success, error code otherwise.
 */
int dm_getcomment(dm_context* context, const char* path, char* comment);

/**
 * Set the comment associated with a file.
 * @param context The DM context.
 * @param path    The logical path.
 * @param comment The comment to associate. '\\0' terminated string.
 * @return        0 on success, error code otherwise.
 */
int dm_setcomment(dm_context* context, const char* path, const char* comment);

/**
 * Get the name of a group.
 * @param context   The DM context.
 * @param gid       The group ID.
 * @param groupName Where to put the group name. It must be at least of size GROUP_NAME_MAX
 * @return          0 on success, error code otherwise.
 */
int dm_getgrpbygid(dm_context* context, gid_t gid, char* groupName);

/**
 * Get the id of a group.
 * @param context   The DM context.
 * @param groupName The group name.
 * @param gid       Where to put the group ID.
 * @return          0 on success, error code otherwise.
 */
int dm_getgrpbynam(dm_context* context, const char* groupName, gid_t* gid);

/**
 * Get the user id.
 * @param context  The DM context.
 * @param userName The user name.
 * @param uid      Where to put the user ID.
 * @return         0 on success, error code otherwise.
 */
int dm_getusrbynam(dm_context* context, const char* userName, uid_t* uid);

/**
 * Get the user name.
 * @param context  The DM context.
 * @param uid      The user ID.
 * @param userName Where to put the user name. It must be at least of size USER_NAME_MAX.
 * @return        0 on success, error code otherwise.
 */
int dm_getusrbyuid(dm_context* context, uid_t uid, char* userName);

/**
 * Open a directory to read it later.
 * @param context The DM context.
 * @param path    The directory to open.
 * @return        A pointer to an internal structure, or NULL on failure.
 */
void* dm_opendir(dm_context* context, const char* path);

/**
 * Close a directory and free the internal structures.
 * @param context The DM context.
 * @param dir     The pointer returned by dm_opendir.
 * @return        0 on success, error code otherwise.
 */
int dm_closedir(dm_context* context, void* dir);

/**
 * Read an entry from a directory.
 * @param context The DM context.
 * @param dir     The pointer returned by dm_opendir.
 * @return        A pointer to a struct with the recovered data, or NULL on failure or end of directory. Do NOT free it.
  */
struct dirent *dm_readdir(dm_context* context, void* dir);

/**
 * Read an entry from a directory (extended data).
 * @param context The DM context.
 * @param dir     The pointer returned by dm_opendir.
 * @return        A pointer to a struct with the recovered data, or NULL on failure or end of directory. Do NOT free it.
 */
struct xstat *dm_readdirx(dm_context* context, void* dir);

/**
 * Create a new directory.
 * @param context The DM context.
 * @param path    The directory for the new path. All the precedent folders must exist.
 * @param mode    Permissions to use for the creation.
 * @return        0 on success, error code otherwise.
 */
int dm_mkdir(dm_context* context, const char* path, mode_t mode);

/**
 * Rename a file, directory or symlink.
 * @param context The DM context.
 * @param oldPath The old name.
 * @param newPath The new name.
 * @return        0 on success, error code otherwise.
 */
int dm_rename(dm_context* context, const char* oldPath, const char* newPath);

/**
 * Delete a directory. It must be empty.
 * @param context The DM context.
 * @param path    The directory to remove.
 * @return        0 on success, error code otherwise.
 */
int dm_rmdir(dm_context* context, const char* path);

/**
 * Set the life time of a replica
 * @param context The DM context.
 * @param replica The replica to modify.
 * @param ltime   The new life time.
 * @return        0 on success, error code otherwise.
 */
int dm_replica_setltime(dm_context* context, const char* replica, time_t ltime);

/**
 * Set the access time of a replica
 * @param context The DM context.
 * @param replica The replica to modify.
 * @return        0 on success, error code otherwise.
 */
int dm_replica_setatime(dm_context* context, const char* replica);

/**
 * Set the type of a replica
 * @param context The DM context.
 * @param replica The replica to modify.
 * @param ftype   The new type ('V', 'D' or 'P')
 * @return        0 on success, error code otherwise.
 */
int dm_replica_settype(dm_context* context, const char* replica, char ftype);

/**
 * Set the status of a replica.
 * @param context The DM context.
 * @param replica The replica to modify.
 * @param status  The new status ('-', 'P', 'D')
 * @return        0 on success, error code otherwise.
 */
int dm_replica_setstatus(dm_context* context, const char* replica, char status);

#ifdef	__cplusplus
}
#endif

#endif	/* DM_CATALOG_H */

