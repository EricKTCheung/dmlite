/** @file   include/dmlite/c/io.h
 *  @brief  C wrapper for I/O interfaces.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_IO_H
#define DMLITE_IO_H

#include "any.h"
#include "dmlite.h"

#ifdef	__cplusplus
extern "C" {
#endif

/** Handle for a file descriptor. */
typedef struct dmlite_fd dmlite_fd;

/**
 * Open a file.
 * @param context The DM context.
 * @param path    The path to open.
 * @param flags   See open()
 * @param extra   The key-value pairs.
 * @return        An opaque handler for the file, NULL on failure.
 */
dmlite_fd* dmlite_fopen(dmlite_context* context, const char* path, int flags,
                        const dmlite_any_dict* extra);

/**
 * Close a file.
 * @param fd The file descriptor as returned by dmlite_open.
 * @return   0 on sucess,  error code otherwise.
 */
int dmlite_fclose(dmlite_fd* fd);

/**
 * Set the file position.
 * @param fd     The file descriptor.
 * @param offset The offset.
 * @param whence See fseek()
 * @return       0 on sucess,  error code otherwise.
 */
int dmlite_fseek(dmlite_fd* fd, long offset, int whence);

/**
 * Return the cursor position.
 * @param fd The file descriptor.
 * @return   The cursor position, or -1 on error.
 */
long dmlite_ftell(dmlite_fd* fd);

/**
 * Read from a file.
 * @param fd     The file descriptor.
 * @param buffer Where to put the data.
 * @param count  Number of bytes to read.
 * @return       Number of bytes actually read on success. -1 on failure.
 */
size_t dmlite_fread(dmlite_fd* fd, void* buffer, size_t count);

/**
 * Write to a file.
 * @param fd     The file descriptor.
 * @param buffer A pointer to the data.
 * @param count  Number of bytes to write.
 * @return       Number of bytes actually written. -1 on failure.
 */
size_t dmlite_fwrite(dmlite_fd* fd, const void* buffer, size_t count);

/**
 * Return 1 if EOF.
 * @param fd The file descriptor.
 * @return   0 if there is more to read. 1 if EOF.
 */
int dmlite_feof(dmlite_fd* fd);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_IO_H */
