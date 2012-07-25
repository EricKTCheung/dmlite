#include <dmlite/c/dmlite.h>
#include <dmlite/c/catalog.h>
#include "utils.h"



void testChDir(dmlite_context* context)
{
  char buffer[64];
  memset(buffer, 0, sizeof(buffer));
  
  SECTION("Test chdir/getcwd");
  
  TEST_CONTEXT_CALL(context, dmlite_chdir, "/usr");
  TEST_ASSERT_EQUAL(buffer, dmlite_getcwd(context, buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("/usr", buffer);
  TEST_CONTEXT_CALL(context, dmlite_chdir, "/");
  TEST_ASSERT_EQUAL(buffer, dmlite_getcwd(context, buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("/", buffer);
}



void testStat(dmlite_context* context)
{
  struct stat  s;
  dmlite_xstat sx;
  char         buffer[64];
  
  memset(&s,  0, sizeof(s));
  memset(&sx, 0, sizeof(sx));
  
  SECTION("Test stat");
  
  TEST_CONTEXT_CALL(context, dmlite_stat, "/etc/hosts", &s);
  TEST_ASSERT_EQUAL(0, s.st_uid);
  TEST_ASSERT_EQUAL(0, s.st_gid);
  
  SECTION("Extended stat with no metadata");
  
  TEST_CONTEXT_CALL(context, dmlite_statx, "/etc/hosts", &sx);
  TEST_ASSERT_EQUAL(s.st_ino, sx.stat.st_ino);
  TEST_ASSERT_STR_EQUAL("hosts", sx.name);
  TEST_ASSERT_EQUAL(NULL, sx.extra);
  
  SECTION("Extended stat with metadata");
  
  sx.extra = dmlite_any_dict_new();
  TEST_CONTEXT_CALL(context, dmlite_statx, "/etc/hosts", &sx);
  dmlite_any* extra = dmlite_any_dict_get(sx.extra, "easter");
  if (extra) {
    dmlite_any_to_string(extra, buffer, sizeof(buffer));
    TEST_ASSERT_STR_EQUAL("egg", buffer);
    dmlite_any_free(extra);
  }
  dmlite_any_dict_free(sx.extra);
}



void testReadDir(dmlite_context* context)
{
  dmlite_dir*   d;
  dmlite_xstat* sx;
  char          buffer[64];
  
  SECTION("Test read dir")
  
  TEST_CONTEXT_CALL_PTR(d, context, dmlite_opendir, "/usr/");
  
  while ((sx = dmlite_readdirx(context, d)) != NULL) {
    log_dump("/usr/%s", sx->name);
    dmlite_any* extra = dmlite_any_dict_get(sx->extra, "easter");
    if (extra) {
      dmlite_any_to_string(extra, buffer, sizeof(buffer));
      TEST_ASSERT_STR_EQUAL("egg", buffer);
      dmlite_any_free(extra);
    }
  }
  TEST_ASSERT_EQUAL(0, dmlite_errno(context));
  
  TEST_CONTEXT_CALL(context, dmlite_closedir, d);
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
  testChDir(context);
  testStat(context);
  testReadDir(context);
  
  /* Clean-up */
  dmlite_context_free(context);
  dmlite_manager_free(manager);
  
  return TEST_FAILURES;
}
