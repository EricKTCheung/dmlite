/** @file   include/dmlite/dmlite.h
 *  @brief  C wrapper for dm::PluginManager.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_H
#define	DMLITE_H

#include <sys/stat.h>
#include <utime.h>
#include "dm_errno.h"
#include "dm_types.h"

#ifdef	__cplusplus
extern "C" {
#endif

/** Handle for the plugin manager. */
typedef struct dm_manager dm_manager;
/** Handle for a initialized context. */
typedef struct dm_context dm_context;
/** Handle for a file descriptor. */
typedef struct dm_fd dm_fd;

/**
 * Get the API version.
 */
unsigned dm_api_version(void);

/**
 * Initialize a dm_manager.
 * @return NULL on failure.
 */
dm_manager* dm_manager_new(void);

/**
 * Destroy the manager.
 * @param manager The manager to be destroyed.
 */
int dm_manager_free(dm_manager* manager);

/**
 * Load a library.
 * @param manager The plugin manager.
 * @param lib     The .so file. Usually, (path)/plugin_name.so.
 * @param id      The plugin ID. Usually, plugin_name.
 * @return        0 on success, error code otherwise.
 */
int dm_manager_load_plugin(dm_manager *manager, const char* lib, const char* id);

/**
 * Set a configuration parameter.
 * @param manager The plugin manager.
 * @param key     The parameter to set.
 * @param value   The value.
 * @return        0 on success, error code otherwise.
 */
int dm_manager_set(dm_manager* manager, const char* key, const char* value);

/**
 * Load a configuration file.
 * @param manager The plugin manager.
 * @param file    The configuration file
 * @return        0 on success, error code otherwise.
 */
int dm_manager_load_configuration(dm_manager* manager, const char* file);

/**
 * Return the string that describes the last error.
 * @param manager The plugin manager used in the failing function.
 * @return        A pointer to the error string. Do NOT free it.
 */
const char* dm_manager_error(dm_manager* manager);

/**
 * Return a usable context from the loaded libraries.
 * @param manager The plugin manager.
 * @return        NULL on failure. The error code can be checked with dm_manager_error.
 */
dm_context* dm_context_new(dm_manager* manager);

/**
 * Destroy the context.
 * @param context The context to free.
 * @return        0 on success, error code otherwise.
 */
int dm_context_free(dm_context* context);

/**
 * Set a configuration parameter.
 * @param context The DM context.
 * @param key     The configuration parameter.
 * @param ...     Variable arguments depending on the configuration parameter.
 * @return        0 on success, error code otherwise.
 */
int dm_set(dm_context* context, const char* key, ...);

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
 * Do a stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dm_istat(dm_context* context, ino_t inode, struct stat* buf);

/**
 * Do an extended stat of an entry using the inode instead of the path.
 * @param context The DM context.
 * @param inode   The entry inode.
 * @param buf     Where to put the retrieved information.
 * @return        0 on success, error code otherwise.
 * @note          Security checks won't be done if you use this function,
 *                so keep in mind doing it yourself.
 */
int dm_ixstat(dm_context* context, ino_t inode, struct xstat* buf);

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
 * @param nEntries     The number of entries will be put here.
 * @param fileReplicas An array with nbentries elements will be stored here. <b>Use dm_freereplicas to free it.</b>
 * @return             0 on success, error code otherwise.
 */
int dm_getreplicas(dm_context* context, const char* path, int *nEntries,
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
 * @param replica Where to put the retrieved replica.
 * @return        0 on success, error code otherwise.
 */
int dm_get(dm_context* context, const char* path, struct filereplica* replica);

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
 * @param uri     Where to put the final destination.
 * @param token   Where to put the token identifying the request. It must be at least of size TOKEN_MAX.
 * @return        0 on success, error code otherwise.
 */
int dm_put(dm_context* context, const char* path, struct uri* uri, char* token);

/**
 * Put a file (synchronous).
 * @param context The DM context.
 * @param path    The logical file name to put.
 * @param uri     Where to put the final destination.
 * @param guid    The Grid Unique ID.
 * @param token   Where to put the token identifying the request. It must be at least of size TOKEN_MAX.
 * @return        0 on success, error code otherwise.
 */
int dm_putg(dm_context* context, const char* path, struct uri* uri, const char* guid, char* token);


/**
 * Retrieve the final destination of a PUT request previously done.
 * @param context The DM context.
 * @param path    The logical filename that was put.
 * @param token   The token identifying the request.
 * @param uri     Where to put the final detination.
 * @return        0 on success, error code otherwise.
 */
int dm_putstatus(dm_context* context, const char* path, const char* token, struct uri* uri);

/**
 * Finish a PUT request.
 * @param context The DM context.
 * @param path    The logical filename that was put.
 * @param token   The token identifying the request.
 * @return        0 on success, error code otherwise.
 */
int dm_putdone(dm_context* context, const char* path, const char* token);

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

/**
 * Set the user ID
 * @param context The DM context.
 * @param cred    The security credentials.
 * @return        0 on success, error code otherwise.
 */
int dm_setcredentials(dm_context* context, struct credentials* cred);

/**
 * Get the list of pools.
 * @param context The DM context.
 * @param nPools  The number of pools.
 * @param pools   An array with the pools. <b>Use dm_freepools to free</b>.
 * @return        0 on succes, -1 on failure.
 */
int dm_getpools(dm_context* context, int* nPools, struct pool** pools);

/**
 * Free an array of pools.
 * @param context The DM context.
 * @param nPools  The number of pools in the array.
 * @param pools   The array to free.
 * @return        0 on succes, -1 on failure.
 */
int dm_freepools(dm_context* context, int nPools, struct pool* pools);

/**
 * Get the list of filesystems in a pool.
 * @param context  The DM context.
 * @param poolname The pool name.
 * @param nFs      The number of file systems returned in dpm_fs.
 * @param fs       An array with the filesystems. <b>Use dm_freefs to free</b>.
 * @return         0 on succes, -1 on failure.
 */
int dm_getpoolfs(dm_context* context, const char* poolname, int* nFs, struct filesystem** fs);

/**
 * Free the list returned by dm_getpoolfs.
 * @param context The DM context.
 * @param nFs     The number of filesystems in the array.
 * @param fs      The array to free.
 * @return        0 on sucess,  error code otherwise.
 */
int dm_freefs(dm_context* context, int nFs, struct filesystem* fs);

/**
 * Open a file.
 * @param context The DM context.
 * @param path    The path to open.
 * @param flags   See open()
 * @return        An opaque handler for the file, NULL on failure.
 */
dm_fd* dm_fopen(dm_context* context, const char* path, int flags);

/**
 * Close a file.
 * @param fd The file descriptor as returned by dm_open.
 * @return   0 on sucess,  error code otherwise.
 */
int dm_fclose(dm_fd* fd);

/**
 * Set the file position.
 * @param fd     The file descriptor.
 * @param offset The offset.
 * @param whence See fseek()
 * @return       0 on sucess,  error code otherwise.
 */
int dm_fseek(dm_fd* fd, long offset, int whence);

/**
 * Return the cursor position.
 * @param fd The file descriptor.
 * @return   The cursor position, or -1 on error.
 */
long dm_ftell(dm_fd* fd);

/**
 * Read from a file.
 * @param fd     The file descriptor.
 * @param buffer Where to put the data.
 * @param count  Number of bytes to read.
 * @return       Number of bytes actually read on success. -1 on failure.
 */
size_t dm_fread(dm_fd* fd, void* buffer, size_t count);

/**
 * Write to a file.
 * @param fd     The file descriptor.
 * @param buffer A pointer to the data.
 * @param count  Number of bytes to write.
 * @return       Number of bytes actually written. -1 on failure.
 */
size_t dm_fwrite(dm_fd* fd, const void* buffer, size_t count);

/**
 * Return 1 if EOF.
 * @param fd The file descriptor.
 * @return   0 if there is more to read. 1 if EOF.
 */
int dm_feof(dm_fd* fd);

/**
 * Return the error code from the last failure.
 * @param context The context that was used in the failed function.
 * @return        The error code.
 */
int dm_errno(dm_context* context);

/**
 * Error string from the last failed function.
 * @param context The context that was used in the failed function.
 * @return        A string with the error description. Do NOT free it.
 */
const char* dm_error(dm_context* context);

/**
 * Parses a URI.
 * @param source Original URI.
 * @param dest   Parsed URI.
 */
void dm_parse_uri(const char* source, struct uri* dest);

/**
 * Serialize into a string a set of ACL entries
 * @param nAcls  The number of ACLs in the array.
 * @param acls   The ACL array.
 * @param buffer Where to put the resulting string.
 * @param bsize  The buffer size.
 */
void dm_serialize_acls(int nAcls, struct dm_acl* acls, char* buffer, size_t bsize);

/**
 * Deserialize a string into an array of ACLs.
 * @param buffer The string.
 * @param nAcls  The resulting number of ACLs.
 * @param acls   The resulting ACLs.
 */
void dm_deserialize_acls(const char* buffer, int* nAcls, struct dm_acl** acls);

/**
 * Free an array of ACLs as returned by dm_deserialize_acls
 * @param nAcls The number of entries in the array.
 * @param acls  The ACL array.
 */
void dm_freeacls(int nAcls, struct dm_acl* acls);


#ifdef	__cplusplus
}
#endif

#endif	/* DMLITE_H */

