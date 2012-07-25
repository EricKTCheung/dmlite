/** @file   include/dmlite/c/dmlite.h
 *  @brief  C wrapper for DMLite
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 */
#ifndef DMLITE_DMLITE_H
#define DMLITE_DMLITE_H

#include <stdlib.h>
#include <sys/stat.h>
#include <utime.h>
#include "any.h"
#include "../common/errno.h"

#ifdef	__cplusplus
extern "C" {
#endif

/** Handle for the plugin manager. */
typedef struct dmlite_manager dmlite_manager;
/** Handle for a initialized context. */
typedef struct dmlite_context dmlite_context;

/** Security credentials
 * It is up to the caller to allocate and free this pointers.
 * DMLite will keep a copy internaly.
 * Non used values MUST be NULL.
 */
typedef struct dmlite_credentials {
  const char* mech;
  const char* client_name;
  const char* remote_address;
  const char* session_id;

  unsigned nfqans;
  const char** fqans;
  
  dmlite_any_dict* extra;
} dmlite_credentials;

/**
 * Get the API version.
 */
unsigned dmlite_api_version(void);

/**
 * Initialize a dmlite_manager.
 * @return NULL on failure.
 */
dmlite_manager* dmlite_manager_new(void);

/**
 * Destroy the manager.
 * @param manager The manager to be destroyed.
 */
int dmlite_manager_free(dmlite_manager* manager);

/**
 * Load a library.
 * @param manager The plugin manager.
 * @param lib     The .so file. Usually, (path)/plugin_name.so.
 * @param id      The plugin ID. Usually, plugin_name.
 * @return        0 on success, error code otherwise.
 */
int dmlite_manager_load_plugin(dmlite_manager *manager, const char* lib, const char* id);

/**
 * Set a configuration parameter.
 * @param manager The plugin manager.
 * @param key     The parameter to set.
 * @param value   The value.
 * @return        0 on success, error code otherwise.
 */
int dmlite_manager_set(dmlite_manager* manager, const char* key, const char* value);

/**
 * Load a configuration file.
 * @param manager The plugin manager.
 * @param file    The configuration file
 * @return        0 on success, error code otherwise.
 */
int dmlite_manager_load_configuration(dmlite_manager* manager, const char* file);

/**
 * Return the last error code.
 * @param manager The plugin manager used in the failing function.
 * @return        The last error code.
 */
int dmlite_manager_errno(dmlite_manager* manager);

/**
 * Return the string that describes the last error.
 * @param manager The plugin manager used in the failing function.
 * @return        A pointer to the error string. Do NOT free it.
 */
const char* dmlite_manager_error(dmlite_manager* manager);

/**
 * Return a usable context from the loaded libraries.
 * @param manager The plugin manager.
 * @return        NULL on failure. The error code can be checked with dmlite_manager_error.
 * @note          A context is NOT thread safe.
 */
dmlite_context* dmlite_context_new(dmlite_manager* manager);

/**
 * Destroy the context.
 * @param context The context to free.
 * @return        0 on success, error code otherwise.
 */
int dmlite_context_free(dmlite_context* context);

/**
 * Return the error code from the last failure.
 * @param context The context that was used in the failed function.
 * @return        The error code.
 */
int dmlite_errno(dmlite_context* context);

/**
 * Error string from the last failed function.
 * @param context The context that was used in the failed function.
 * @return        A string with the error description. Do NOT free it.
 */
const char* dmlite_error(dmlite_context* context);

/**
 * Set the user ID
 * @param context The DM context.
 * @param cred    The security credentials.
 * @return        0 on success, error code otherwise.
 */
int dmlite_setcredentials(dmlite_context* context, dmlite_credentials* cred);

/**
 * Set a configuration parameter tied to a context.
 * This can be used to pass advanced parameters to a plugin.
 * @param context The DM context.
 * @param k The configuration key.
 * @param v Value.
 * @return  0 on success, error code otherwise.
 */
int dmlite_set(dmlite_context* context, const char* k, const dmlite_any* v);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_DMLITE_H */
