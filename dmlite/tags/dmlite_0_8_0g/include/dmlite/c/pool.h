/** @file   include/dmlite/c/pool.h
 *  @brief  C wrapper for DMLite Pool API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_POOL_H
#define DMLITE_POOL_H

#include "dmlite.h"
#include "any.h"
#include "inode.h"
#include "utils.h"

#define POOL_TYPE_MAX 16
#define POOL_MAX      16

#ifdef	__cplusplus
extern "C" {
#endif
  
/** @brief Pool data */
typedef struct dmlite_pool {
  char pool_type[POOL_TYPE_MAX];
  char pool_name[POOL_MAX];
  
  dmlite_any_dict* extra;
} dmlite_pool;

/** @brief Chunk of data */
typedef struct dmlite_chunk {
  off_t     offset;
  size_t    size;
  dmlite_url url;
} dmlite_chunk;

/** @brief   Collection of chunks that form a replica
 *  @details On read, there may be duplicated chunks.
 */
typedef struct dmlite_location {
  dmlite_chunk* chunks;
  unsigned      nchunks;
} dmlite_location;

/**
 * @brief         Gets the list of pools.
 * @param context The DM context.
 * @param nPools  The number of pools.
 * @param pools   An array with the pools. <b>Use dmlite_freepools to free</b>.
 * @return        0 on success, error code otherwise.
 */
int dmlite_getpools(dmlite_context* context, unsigned* nPools, dmlite_pool** pools);

/**
 * @brief         Frees an array of pools.
 * @param nPools  The number of pools in the array.
 * @param pools   The array to free.
 * @return        0 on success, error code otherwise.
 */
int dmlite_pools_free(unsigned nPools, dmlite_pool* pools);

/**
 * @brief         Gets a single replica (synchronous).
 * @param context The DM context.
 * @param path    The logical file name.
 * @return        A pointer to a dmlite_location struct, or NULL on error.
 */
dmlite_location* dmlite_get(dmlite_context* context, const char* path);

/**
 * @brief         Gets a single replica (synchronous).
 * @param context The DM context.
 * @param inode   The file inode.
 * @return        A pointer to a dmlite_location struct, or NULL on error.
 */
dmlite_location* dmlite_iget(dmlite_context* context, ino_t inode);

/**
 * @brief         Gets the location of a replica.
 * @param context The DM context.
 * @param replica The replica to translate.
 * @return        A pointer to a dmlite_location struct, or NULL on error.
 */
dmlite_location* dmlite_getlocation(dmlite_context* context, const dmlite_replica* replica);

/**
 * @brief         Puts a file (synchronous).
 * @param context The DM context.
 * @param path    The logical file name to put.
 * @return        A pointer to a dmlite_location struct, or NULL on error.
 */
dmlite_location* dmlite_put(dmlite_context* context, const char* path);

/**
 * @brief         Aborts a put request.
 * @param context The DM context.
 * @param loc     As returned by dmlite_put.
 * @return        0 on success, error code otherwise.
 */
int dmlite_put_abort(dmlite_context* context, const dmlite_location* loc);

/**
 * @brief         Frees a location struct.
 * @param loc     The struct to free.
 * @return        0 on success, error code otherwise.
 */
int dmlite_location_free(dmlite_location* loc);

/**
 * @brief            Get the estimation of the free/used space for writing into a directory
 * @param path               The path of the directory to query
 * @param totalfree          The total number of free bytes (may not be contiguous)
 * @param used               The total number of used bytes
 * @return        0 on success, error code otherwise.
 */
int dmlite_getdirspaces(dmlite_context* context, const char *logicaldir, int64_t *freespace, int64_t *used);


#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_POOL_H */
