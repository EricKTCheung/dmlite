/** @file   include/dmlite/c/dml_pool.h
 *  @brief  C wrapper for DMLite Pool API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_POOL_H
#define	DM_POOL_H

#include "dmlite.h"

#ifdef	__cplusplus
extern "C" {
#endif

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
 * Put a file (synchronous).
 * @param context The DM context.
 * @param path    The logical file name to put.
 * @param loc     The pointer will be set to a struct location. Call dm_freelocation to free.
 * @return        0 on success, error code otherwise.
 */
int dm_put(dm_context* context, const char* path, struct location** loc);

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
 * Free a location struct.
 * @param context The DM context.
 * @param loc     The struct to free.
 * @return        0 on success, error code otherwise.
 */
int dm_freelocation(dm_context* context, struct location* loc);

#ifdef	__cplusplus
}
#endif

#endif	/* DM_POOL_H */
