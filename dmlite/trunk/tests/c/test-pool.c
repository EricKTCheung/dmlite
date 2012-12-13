#include <dmlite/c/any.h>
#include <dmlite/c/dmlite.h>
#include <dmlite/c/io.h>
#include <dmlite/c/pool.h>
#include "utils.h"



void testGetPools(dmlite_context* context)
{
  dmlite_pool* pools;
  unsigned     nPools;
  char         buffer[64];
  
  SECTION("Get pools");
  
  /* Mock should return 1 */
  TEST_CONTEXT_CALL(context, dmlite_getpools, &nPools, &pools);
  TEST_ASSERT_EQUAL(1, nPools);
  
  TEST_ASSERT_STR_EQUAL("mock", pools[0].pool_type);
  TEST_ASSERT_STR_EQUAL("hardcoded", pools[0].pool_name);
  
  /* There is one dummy extra */
  dmlite_any* extra = dmlite_any_dict_get(pools[0].extra, "extra");
  dmlite_any_to_string(extra, buffer, sizeof(buffer));
  TEST_ASSERT_STR_EQUAL("something", buffer);
  dmlite_any_free(extra);
  
  /* Free */
  TEST_ASSERT_EQUAL(0, dmlite_pools_free(nPools, pools));
}



void testGet(dmlite_context* context)
{
  SECTION("GET");
  
  /* Mock returns always a location with two chunks */
  dmlite_location* loc;
  TEST_CONTEXT_CALL_PTR(loc, context, dmlite_get, "/file");
  
  TEST_ASSERT_EQUAL(2, loc->nchunks);
  
  TEST_ASSERT_STR_EQUAL("host1.cern.ch", loc->chunks[0].url.domain);
  TEST_ASSERT_STR_EQUAL("/storage/chunk01", loc->chunks[0].url.path);
  TEST_ASSERT_EQUAL(0, loc->chunks[0].offset);
  TEST_ASSERT_EQUAL(100, loc->chunks[0].size);
          
  TEST_ASSERT_STR_EQUAL("host2.cern.ch", loc->chunks[1].url.domain);
  TEST_ASSERT_STR_EQUAL("/storage/chunk02", loc->chunks[1].url.path);
  TEST_ASSERT_EQUAL(101, loc->chunks[1].offset);
  TEST_ASSERT_EQUAL(50, loc->chunks[1].size);
  
  /* Second has an extra */
  TEST_ASSERT_STR_EQUAL("token=123456789", loc->chunks[1].url.query);
  
  dmlite_location_free(loc);
}



void testPut(dmlite_context* context)
{
  char buffer[64];
  
  SECTION("PUT");
  
  /* Mock returns always one single location with one token */
  dmlite_location* loc;
  TEST_CONTEXT_CALL_PTR(loc, context, dmlite_put, "/file");
  
  TEST_ASSERT_EQUAL(1, loc->nchunks);
  
  TEST_ASSERT_STR_EQUAL("host1.cern.ch", loc->chunks[0].url.domain);
  TEST_ASSERT_STR_EQUAL("/storage/chunk01", loc->chunks[0].url.path);
  TEST_ASSERT_EQUAL(0, loc->chunks[0].offset);
  TEST_ASSERT_EQUAL(0, loc->chunks[0].size);
  
  TEST_ASSERT_STR_EQUAL("token=987654321", loc->chunks[0].url.query);
  
  dmlite_location_free(loc);
  
  /* A donewriting without token will fail */
  dmlite_any_dict* dict = dmlite_any_dict_new();
  TEST_ASSERT_EQUAL(EACCES, dmlite_donewriting(context,
                                               "/storage/chunk01", dict));
  
  /* With token */
  dmlite_any* token = dmlite_any_new_string("987654321");
  dmlite_any_dict_insert(dict, "token", token);
  
  TEST_CONTEXT_CALL(context, dmlite_donewriting,
                    "/storage/chunk01", dict);
  
  dmlite_any_free(token);
  dmlite_any_dict_free(dict);
}



int main(int argn, char** argv)
{
  dmlite_manager* manager;
  dmlite_context* context;
  
  SECTION("Instantiation");
  
  /* Load the mock plug-in */
  manager = dmlite_manager_new();
  TEST_MANAGER_CALL(manager, dmlite_manager_load_plugin, "./plugin_mock.so", "plugin_mock");
  
  /* Instantiate */
  context = dmlite_context_new(manager);
  
  /* Set credentials (mock will ignore anyway) */
  dmlite_credentials creds;
  memset(&creds, 0, sizeof(creds));
  creds.client_name = "root";
  TEST_CONTEXT_CALL(context, dmlite_setcredentials, &creds);
  
  /* Do tests */
  testGetPools(context);
  testGet(context);
  testPut(context);
  
  /* Clean-up */
  dmlite_context_free(context);
  dmlite_manager_free(manager);
  
  return TEST_FAILURES;
}
