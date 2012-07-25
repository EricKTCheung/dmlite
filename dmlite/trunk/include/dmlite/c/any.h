/** @file   include/dmlite/c/any.h
 *  @brief  Opaque handler to pass different types of values to the API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 *  @note   Basically it wraps boost::any and dmlite::Extensible.
 */
#ifndef DMLITE_ANY_H
#define DMLITE_ANY_H

#include <stddef.h>

#ifdef	__cplusplus
extern "C" {
#endif

/** Used to pass configuration values.
 */
typedef struct dmlite_any dmlite_any;

/** Handles key->value pairs
 */
typedef struct dmlite_any_dict dmlite_any_dict;

/**
 * Creates a new dmlite_any.
 * @param  str The string that will be wrapped. It is safe to free afterwards.
 * @return     A newly allocated dmlite_any.
 */
dmlite_any* dmlite_any_new_string(const char* str);

/**
 * Creates a new dmlite_any.
 * @param  n    The number of elements.
 * @param  strv The strings that will be wrapped. It is safe to free afterwards.
 * @return      A newly allocated dmlite_any.
 */
dmlite_any* dmlite_any_new_string_array(unsigned n, const char** strv);

/**
 * Creates a new dmlite_any.
 * @param  l The long that will be wrapped.
 * @return   A newly allocated dmlite_any.
 */
dmlite_any* dmlite_any_new_long(long l);

/**
 * Creates a new dmlite_any.
 * @param  n  The number of elements.
 * @param  lv The longs that will be wrapped.
 * @return    A newly allocated dmlite_any.
 */
dmlite_any* dmlite_any_new_long_array(unsigned n, long* lv);

/**
 * Frees a dmlite_any.
 * @param any The dmlite_any to destroy.
 */
void dmlite_any_free(dmlite_any* any);

/**
 * Gets the string interpretation of the dmlite_any. Defaults to "".
 */
void dmlite_any_to_string(const dmlite_any* any, char* buffer, size_t bsize);

/**
 * Returns the long interpretation of they dmlite_any. Defaults to 0.
 */
long dmlite_any_to_long(const dmlite_any* any);


/**
 * Created a new generic dictionary.
 * @return A newly allocated dmlite_any_dict.
 */
dmlite_any_dict* dmlite_any_dict_new();

/**
 * Frees a dmlite_any_dict
 */
void dmlite_any_dict_free(dmlite_any_dict* d);

/**
 * Insert a new dmlite_any value into the dictionary. Replaces if already present.
 * @param d The dictionary.
 * @param k The key.
 * @param v The value.
 */
void dmlite_any_dict_insert(dmlite_any_dict* d, const char* k, const dmlite_any* v);

/**
 * Returns how many elements there are in a specific dictionary.
 */
unsigned long dmlite_any_dict_count(const dmlite_any_dict* d);

/**
 * Returns the value associated with the key k.
 * @return NULL if not found.
 */
const dmlite_any* dmlite_any_dict_get(const dmlite_any_dict* d, const char* k);

/**
 * Generates a JSON serialization of the dictionary.
 */
void dmlite_any_dict_to_json(const dmlite_any_dict* d, char* buffer, size_t bsize);

/**
 * Populates a dmlite_any_dict from a simple JSON.
 */
dmlite_any_dict* dmlite_any_dict_from_json(const char* json);

#ifdef	__cplusplus
}
#endif

#endif /* DMLITE_ANY_H */

