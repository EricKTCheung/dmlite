/** @file   include/dmlite/c/io.h
 *  @brief  C wrapper for I/O interfaces.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_IO_H
#define DMLITE_IO_H

#include "dmlite.h"
#include "any.h"
#include <sys/uio.h>
#include <unistd.h>


#ifdef	__cplusplus
extern "C" {
#endif

/** Use this flag in addition to the standard ones to skip any
 *  security check (i.e. token validation)
 */
#define O_INSECURE 010

/** Handle for a file descriptor. */
typedef struct dmlite_fd dmlite_fd;

/**
 * @brief         Opens a file.
 * @param context The DM context.
 * @param path    The path to open.
 * @param flags   See open()
 * @param extra   The key-value pairs.
 * @param ...     Should be mode_t when called with O_CREAT.
 * @return        An opaque handler for the file, NULL on failure.
 */
dmlite_fd* dmlite_fopen(dmlite_context* context, const char* path, int flags,
                        const dmlite_any_dict* extra, ...);

/**
 * @brief    Closes a file.
 * @param fd The file descriptor as returned by dmlite_open.
 * @return   0 on success, error code otherwise.
 */
int dmlite_fclose(dmlite_fd* fd);

/**
 * @brief     Gets information about a file descriptor.
 * @param fd  The file descriptor.
 * @param buf Where to put the information.
 * @return    0 on success, error code otherwise.
 * @note      Not all plug-ins will fill all the fields, but st_size is
 *            a reasonable expectation.
 */
int dmlite_fstat(dmlite_fd* fd, struct stat* buf);

/**
 * @brief        Sets the file position.
 * @param fd     The file descriptor.
 * @param offset The offset.
 * @param whence See fseek()
 * @return       0 on success, error code otherwise.
 */
int dmlite_fseek(dmlite_fd* fd, off_t offset, int whence);

/**
 * @brief    Returns the cursor position.
 * @param fd The file descriptor.
 * @return   The cursor position, or -1 on error.
 */
off_t dmlite_ftell(dmlite_fd* fd);

/**
 * @brief        Reads from a file.
 * @param fd     The file descriptor.
 * @param buffer Where to put the data.
 * @param count  Number of bytes to read.
 * @return       Number of bytes actually read on success. -1 on failure.
 */
ssize_t dmlite_fread(dmlite_fd* fd, void* buffer, size_t count);

/**
 * @brief        Writes to a file.
 * @param fd     The file descriptor.
 * @param buffer A pointer to the data.
 * @param count  Number of bytes to write.
 * @return       Number of bytes actually written. -1 on failure.
 */
ssize_t dmlite_fwrite(dmlite_fd* fd, const void* buffer, size_t count);

/**
 * @brief        Reads from a file into multiple buffers.
 * @param fd     The file descriptor.
 * @param vector Array of buffers.
 * @param count  Number of elements in the array of buffers.
 * @return       Number of bytes actually read on success. -1 on failure.
 */
ssize_t dmlite_freadv(dmlite_fd* fd, const struct iovec* vector, size_t count);

/**
* @brief        Reads from a file into multiple buffers.
* @param fd     The file descriptor.
* @param vector Array of buffers.
* @param count  Number of elements in the array of buffers.
* @return       Number of bytes actually read on success. -1 on failure.
*/
ssize_t dmlite_fwritev(dmlite_fd* fd, const struct iovec* vector, size_t count);

/**
 * @brief        Reads up to count bytes starting at the given offset.
 *               Does not change internal offset.
 * @param fd     File descriptor.
 * @param buffer Buffer where to put the data.
 * @param count  Number of bytes to read.
 * @param offset Read offset.
 * @return       Number of bytes actually read on success. -1 on failure.
 */
ssize_t dmlite_fpread(dmlite_fd* fd, void* buffer, size_t count, off_t offset);

/**
 * @brief        Writes count bytes starting at the given offset.
 *               Does not change internal offset.
 * @param fd     File descriptor.
 * @param buffer Data to write.
 * @param count  Number of bytes to read.
 * @param offset Write offset.
 * @return       Number of bytes actually write on success. -1 on failure.
 */
ssize_t dmlite_fpwrite(dmlite_fd* fd, const void* buffer, size_t count, off_t offset);

/**
 * @brief    Returns 1 if EOF.
 * @param fd The file descriptor.
 * @return   0 if there is more to read. 1 if EOF.
 */
int dmlite_feof(dmlite_fd* fd);

/**
 * @brief    Returns the last errror code.
 * @param fd The file descriptor.
 * @return   The last error code.
 */
int dmlite_ferrno(dmlite_fd* fd);

/**
 * @brief    Returns the last error message.
 * @param fd The file descriptor.
 * @return   A pointer to an internal buffer with the last error message.
 * @note     This buffer is specific to each file descriptor.
 */
const char* dmlite_ferrror(dmlite_fd* fd);

/**
 * @brief         Finishes a PUT.
 * @param context The DM context.
 * @param pfn     The replica file name.
 * @param extra   The extra parameters as returned by dmlite_put.
 * @return        0 on success, error code otherwise.
 */
int dmlite_donewriting(dmlite_context* context,
                       const char* pfn,
                       const dmlite_any_dict* extra);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_IO_H */
