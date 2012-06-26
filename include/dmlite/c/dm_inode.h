/** @file   include/dmlite/c/dm_inode.h
 *  @brief  C wrapper for DMLite INode API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_INODE_H
#define	DM_INODE_H

#ifdef	__cplusplus
extern "C" {
#endif

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

#ifdef	__cplusplus
}
#endif

#endif	/* DM_INODE_H */

