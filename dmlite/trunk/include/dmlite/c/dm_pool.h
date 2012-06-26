/** @file   include/dmlite/c/dml_pool.h
 *  @brief  C wrapper for DMLite Pool API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_POOL_H
#define	DM_POOL_H

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


#ifdef	__cplusplus
}
#endif

#endif	/* DM_POOL_H */
