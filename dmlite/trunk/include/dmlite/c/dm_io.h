/** @file   include/dmlite/c/dm_io.h
 *  @brief  C wrapper for I/O interfaces.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_IO_H
#define	DM_IO_H

#include "dmlite.h"

#ifdef	__cplusplus
extern "C" {
#endif

/** Handle for a file descriptor. */
typedef struct dm_fd dm_fd;
  
/**
 * Do a stat over a physical file.
 * @param context The DM context.
 * @param rfn     The path to stat.
 * @param st      Where to put the stat.
 * @return        0 on sucess,  error code otherwise.
 */
int dm_pstat(dm_context* context, const char* rfn, struct stat* st);

/**
 * Open a file.
 * @param context The DM context.
 * @param path    The path to open.
 * @param flags   See open()
 * @param nextras Number of key-value parameters, usually as provided by dm_get
 * @param extras  The key-value pairs.
 * @return        An opaque handler for the file, NULL on failure.
 */
dm_fd* dm_fopen(dm_context* context, const char* path, int flags,
                unsigned nextras, struct keyvalue* extras);

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

#ifdef	__cplusplus
}
#endif

#endif	/* DM_IO_H */

