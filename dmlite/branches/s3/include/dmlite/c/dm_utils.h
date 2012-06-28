/** @file   include/dmlite/c/dm_utils
 *  @brief  C wrapper for DMLite utils.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DM_UTILS_H
#define	DM_UTILS_H

#include "../common/dm_types.h"

#ifdef	__cplusplus
extern "C" {
#endif

/**
 * Parses a URL.
 * @param source Original URL.
 * @param dest   Parsed URL.
 */
void dm_parse_url(const char* source, struct url* dest);

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

#endif	/* DM_UTILS_H */

