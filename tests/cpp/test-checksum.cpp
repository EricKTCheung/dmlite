#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <cstring>
#include <dmlite/cpp/utils/checksums.h>
#include "test-base.h"

class TestChecksum: public TestBase
{
protected:
  const static char *FILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->stackInstance->setSecurityCredentials(cred1);
    this->catalog->create(FILE, 0755);
  }

  void tearDown()
  {
    if (this->catalog) {
      this->stackInstance->setSecurityContext(root);
      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
    }

    TestBase::tearDown();
  }

  void testBasic()
  {
    const std::string csumtype("MD");
    const std::string csumvalue("1a31009319c99ebdefd23055b20ff034");
    
    // Change checksum to something else
    this->catalog->setChecksum(FILE, csumtype, csumvalue);
    
    // Compare
    dmlite::ExtendedStat meta = this->catalog->extendedStat(FILE);
    
    CPPUNIT_ASSERT_EQUAL(csumtype,  meta.csumtype);
    CPPUNIT_ASSERT_EQUAL(csumvalue, meta.csumvalue);
  }

  void testTranslationToLong()
  {
    CPPUNIT_ASSERT_EQUAL(std::string("MD5"), dmlite::checksums::fullChecksumName("md"));
    CPPUNIT_ASSERT_EQUAL(std::string("MD5"), dmlite::checksums::fullChecksumName("MD"));

    CPPUNIT_ASSERT_EQUAL(std::string("ADLER32"), dmlite::checksums::fullChecksumName("AD"));
    CPPUNIT_ASSERT_EQUAL(std::string("ADLER32"), dmlite::checksums::fullChecksumName("ad"));

    CPPUNIT_ASSERT_EQUAL(std::string("UNIXcksum"), dmlite::checksums::fullChecksumName("CS"));
    CPPUNIT_ASSERT_EQUAL(std::string("UNIXcksum"), dmlite::checksums::fullChecksumName("cs"));
  }

  void testTranslationToShort()
  {
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("MD5"));
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("md5"));
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("mD5"));

    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("ADLER32"));
    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("adler32"));
    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("aDlEr32"));

    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("UNIXcksum"));
    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("unixcksum"));
    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("unixCKSum"));
  }

  CPPUNIT_TEST_SUITE(TestChecksum);
  CPPUNIT_TEST(testBasic);
  CPPUNIT_TEST(testTranslationToLong);
  CPPUNIT_TEST(testTranslationToShort);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestChecksum);

const char* TestChecksum::FILE  = "test-checksum";

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

