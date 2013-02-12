#include <dmlite/c/utils.h>
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



int main(int argc, char** argv)
{
  testShortToLong();
  testLongToShort();

  return TEST_FAILURES;
}
