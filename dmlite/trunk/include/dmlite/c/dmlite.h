/** @file   include/dmlite/c/dmlite.h
 *  @brief  C wrapper for DMLite
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_H
#define	DMLITE_H

#include <stdlib.h>
#include <sys/stat.h>
#include <utime.h>
#include "../common/dm_errno.h"
#include "../common/dm_types.h"

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
 * Set a configuration parameter tied to a context.
 * This can be used to pass advanced parameters to a plugin.
 * @param context The DM context.
 * @param k The configuration key.
 * @param v Value.
 * @return  0 on success, error code otherwise.
 */
int dm_set(dm_context* context, const char* k, union value v);

/**
 * Set the user ID
 * @param context The DM context.
 * @param cred    The security credentials.
 * @return        0 on success, error code otherwise.
 */
int dm_setcredentials(dm_context* context, struct credentials* cred);


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


#ifdef	__cplusplus
}
#endif

#endif	/* DMLITE_H */
