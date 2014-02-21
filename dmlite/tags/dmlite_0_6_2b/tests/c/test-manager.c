#include <dmlite/c/catalog.h>
#include <dmlite/c/dmlite.h>
#include <dmlite/common/errno.h>
#include "utils.h"


int main(int argn, char** argv)
{
  dmlite_manager* manager;
  dmlite_context* context;
  char buffer[512];
  
  printf("DMLite API Version: %d\n", dmlite_api_version());
  
  /* Creation */
  SECTION("Creation");
  manager = dmlite_manager_new();
  TEST_ASSERT(manager != NULL);
  
  SECTION("Loading errors")
  TEST_ASSERT_EQUAL(DMLITE_CFGERR(ENOENT),
                    dmlite_manager_load_plugin(manager, "/does/not/exist", "plugin_something"));
  TEST_ASSERT_EQUAL(DMLITE_CFGERR(DMLITE_NO_SUCH_SYMBOL),
                    dmlite_manager_load_plugin(manager, "./plugin_mock.so", "plugin_fake"));
  
  /* Load the mock plug-in */  
  TEST_MANAGER_CALL(manager, dmlite_manager_load_plugin, "./plugin_mock.so", "plugin_mock");
  
  /* Set an unknown option */
  TEST_ASSERT_EQUAL(DMLITE_CFGERR(DMLITE_UNKNOWN_KEY),
                    dmlite_manager_set(manager, "Option", "Value"));
  
  /* Set a known option, and get it back */
  TEST_MANAGER_CALL(manager, dmlite_manager_set, "TestParam", "something");
  TEST_MANAGER_CALL(manager, dmlite_manager_get, "TestParam", buffer, sizeof(buffer));
  TEST_ASSERT_STR_EQUAL("something", buffer);

  /* Instantiate */
  context = dmlite_context_new(manager);
  TEST_ASSERT(context != NULL);
  if (context == NULL) {
    log_failure("%s", dmlite_manager_error(manager));
    exit(1);
  }
  
  /* Doing anything must fail because the credentials weren't set */
  struct stat s;
  TEST_ASSERT_EQUAL(DMLITE_SYSERR(DMLITE_NO_SECURITY_CONTEXT),
                    dmlite_stat(context, "/", &s));
  
  /* Set credentials */
  SECTION("Security context");
  dmlite_credentials creds;
  memset(&creds, 0, sizeof(creds));
  creds.client_name = "root";
  TEST_CONTEXT_CALL(context, dmlite_setcredentials, &creds);
  
  /* Get context back and check */  
  const dmlite_security_context* secCtx = dmlite_get_security_context(context);
  TEST_ASSERT(secCtx != NULL);
  
  TEST_ASSERT_EQUAL(1, secCtx->ngroups);
  TEST_ASSERT_STR_EQUAL("root", secCtx->user.name);
  
  dmlite_any* any = dmlite_any_dict_get(secCtx->user.extra, "uid");
  TEST_ASSERT_EQUAL(0, dmlite_any_to_long(any));
  dmlite_any_free(any);
  
  TEST_ASSERT_STR_EQUAL(creds.client_name, secCtx->credentials->client_name);
  TEST_ASSERT_EQUAL(creds.nfqans, secCtx->credentials->nfqans)

  /* Clean-up */
  dmlite_context_free(context);
  dmlite_manager_free(manager);
  
  return TEST_FAILURES;
}
