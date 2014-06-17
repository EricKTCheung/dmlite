#include <dmlite/c/checksums.h>
#include <fcntl.h>
#include "utils.h"

void testShortToLong()
{
  char buffer[64];

  SECTION("Checksums Short => Long");

  TEST_ASSERT_STR_EQUAL("md5", dmlite_checksum_full_name("md", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("md5", dmlite_checksum_full_name("MD", buffer, sizeof(buffer)));

  TEST_ASSERT_STR_EQUAL("adler32", dmlite_checksum_full_name("ad", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("adler32", dmlite_checksum_full_name("AD", buffer, sizeof(buffer)));

  TEST_ASSERT_STR_EQUAL("crc32", dmlite_checksum_full_name("cs", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("crc32", dmlite_checksum_full_name("CS", buffer, sizeof(buffer)));
}



void testLongToShort()
{
  char buffer[64];

  SECTION("Checksums Long => Short");

  TEST_ASSERT_STR_EQUAL("MD", dmlite_checksum_short_name("MD5", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("MD", dmlite_checksum_short_name("md5", buffer, sizeof(buffer)));

  TEST_ASSERT_STR_EQUAL("AD", dmlite_checksum_short_name("adler32", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("AD", dmlite_checksum_short_name("ADLER32", buffer, sizeof(buffer)));

  TEST_ASSERT_STR_EQUAL("CS", dmlite_checksum_short_name("CRC32", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("CS", dmlite_checksum_short_name("crc32", buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("CS", dmlite_checksum_short_name("CrC32", buffer, sizeof(buffer)));
}


/*
 * Remember the content coming from the plugin_mock is
 * 'a' to 'z' and again
 */
void testDigest(dmlite_context* context)
{
  dmlite_fd* file;
  char       buffer[512];

  SECTION("Open file");
  TEST_CONTEXT_CALL_PTR(file, context, dmlite_fopen, "/file", O_RDONLY | O_INSECURE, NULL);

  SECTION("MD5 checksum");
  TEST_ASSERT_EQUAL(0, dmlite_checksum_md5(file, 0, 14, buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("0845a5972cd9ad4a46bad66f1253581f", buffer);

  SECTION("ADLER32 checksum");
  TEST_ASSERT_EQUAL(0, dmlite_checksum_adler32(file, 5, 16, buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("38e806d9", buffer);

  SECTION("CRC32 checksum");
  TEST_ASSERT_EQUAL(0, dmlite_checksum_crc32(file, 10, 9, buffer, sizeof(buffer)));
  TEST_ASSERT_STR_EQUAL("2525988025", buffer);

  dmlite_fclose(file);
}



int main(int argc, char** argv)
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


  testShortToLong();
  testLongToShort();
  testDigest(context);


  dmlite_context_free(context);
  dmlite_manager_free(manager);

  return TEST_FAILURES;
}
