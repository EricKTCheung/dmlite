#include <dmlite/c/dmlite.h>
#include <dmlite/c/io.h>
#include <fcntl.h>
#include "utils.h"



void testRead(dmlite_context* context)
{
  dmlite_fd*       file;
  dmlite_any_dict* dict = dmlite_any_dict_new();
  char             buffer[64];
  memset(buffer, 0, sizeof(buffer));
  
  SECTION("Read");
  
  /* Bad token */
  file = dmlite_fopen(context, "/file", O_RDONLY, dict);
  TEST_ASSERT_EQUAL(NULL, file);
  TEST_ASSERT_EQUAL(EACCES, dmlite_errno(context));
          
  /* Open non-existing */
  file = dmlite_fopen(context, "/does-not-exist", O_RDONLY, dict);
  TEST_ASSERT_EQUAL(NULL, file);
  TEST_ASSERT_EQUAL(ENOENT, dmlite_errno(context));
  
  /* Good to open */
  dmlite_any* token = dmlite_any_new_string("123456789");
  dmlite_any_dict_insert(dict, "token", token);
  dmlite_any_free(token);
  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDONLY, dict);
  
  /* Read */
  TEST_ASSERT_EQUAL(4, dmlite_fread(file, buffer, 4));
  buffer[5] = '\0';
  TEST_ASSERT_STR_EQUAL("abcd", buffer);
  TEST_ASSERT_EQUAL(4, dmlite_fread(file, buffer, 4));
  buffer[5] = '\0';
  TEST_ASSERT_STR_EQUAL("efgh", buffer);
  
  /* Seek and read */
  TEST_ASSERT_EQUAL(0, dmlite_fseek(file, 10, SEEK_SET));
  TEST_ASSERT_EQUAL(4, dmlite_fread(file, buffer, 4));
  buffer[5] = '\0';
  TEST_ASSERT_STR_EQUAL("klmn", buffer);
  
  TEST_ASSERT_EQUAL(14, dmlite_ftell(file));
  
  TEST_ASSERT_EQUAL(0, dmlite_feof(file));
  
  /* Free */
  TEST_ASSERT_EQUAL(0, dmlite_fclose(file));
  dmlite_any_dict_free(dict);
}



void testWrite(dmlite_context* context)
{
  dmlite_fd*       file;
  dmlite_any_dict* dict = dmlite_any_dict_new();
  char             buffer[64];
  
  SECTION("Read");
  
  /* Good to open */
  dmlite_any* token = dmlite_any_new_string("987654321");
  dmlite_any_dict_insert(dict, "token", token);
  dmlite_any_free(token);
  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDWR, dict);
  
  /* Write a chunk */
  TEST_ASSERT_EQUAL(10, dmlite_fwrite(file, "123456789", 10));
  
  /* Seek and read it back */
  TEST_ASSERT_EQUAL(0, dmlite_fseek(file, 0, SEEK_SET));
  TEST_ASSERT_EQUAL(10, dmlite_fread(file, buffer, 10));
  TEST_ASSERT_STR_EQUAL("123456789", buffer);
  
  
  /* Free */
  TEST_ASSERT_EQUAL(0, dmlite_fclose(file));
  dmlite_any_dict_free(dict);
}



void testInsecure(dmlite_context* context)
{
  dmlite_fd* file;

  SECTION("Insecure");

  /* Opening without token should be good */
  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDONLY | O_INSECURE, NULL);
  TEST_ASSERT_EQUAL(0, dmlite_fclose(file));
}



void testVector(dmlite_context* context)
{
  dmlite_fd* file;
  const char *strings[] = {"this-", "is-a-", "nice-string"};
  char buffer[128];
  struct iovec vector[3];
  size_t i, expected;

  SECTION("Vector write");

  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDONLY | O_INSECURE, NULL);

  for (i = 0, expected = 0; i < 3; ++i) {
    vector[i].iov_base = (void*)strings[i];
    vector[i].iov_len  = strlen(strings[i]);
    expected += vector[i].iov_len;
  }
  TEST_ASSERT_EQUAL(expected, dmlite_fwritev(file, vector, 3));

  SECTION("Vector read");

  TEST_ASSERT_EQUAL(0, dmlite_fseek(file, 0, SEEK_SET));

  memset(buffer, 0, sizeof(buffer));
  vector[0].iov_base = buffer;
  for (i = 1; i < 3; ++i) {
    vector[i].iov_base = vector[i - 1].iov_base + vector[i - 1].iov_len;
  }

  TEST_ASSERT_EQUAL(expected, dmlite_freadv(file, vector, 3));
  TEST_ASSERT_STR_EQUAL("this-is-a-nice-string", buffer);

  TEST_ASSERT_EQUAL(0, dmlite_fclose(file));
}



void testStat(dmlite_context* context)
{
  dmlite_fd* file;
  struct stat fstat;
  SECTION("Test fstat");

  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDONLY | O_INSECURE, NULL);

  TEST_ASSERT_EQUAL(0, dmlite_fstat(file, &fstat));
  TEST_ASSERT_EQUAL(1024, fstat.st_size);

  TEST_ASSERT_EQUAL(0, dmlite_fclose(file));
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
  testRead(context);
  testWrite(context);
  testInsecure(context);
  testVector(context);
  testStat(context);
  
  /* Clean-up */
  dmlite_context_free(context);
  dmlite_manager_free(manager);
  
  return TEST_FAILURES;
}

