#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <cstring>
#include <dmlite/cpp/utils/checksums.h>
#include "test-base.h"

// Mock IO handler, so the test has the control
// over the data being digested, and it knows
// about the expected output
class MockIOHandler: public dmlite::IOHandler
{
public:
  static const char *MOCK_IO_CONTENT;

  const char*  pos_;
  const size_t mockContentLen_;
public:

  MockIOHandler(): pos_(MOCK_IO_CONTENT), mockContentLen_(strlen(MOCK_IO_CONTENT)) {}

  size_t read(char* buffer, size_t count) throw (dmlite::DmException) {
    size_t remaining = mockContentLen_ - this->tell();
    size_t readable = (remaining<count)?remaining:count;

    ::strncpy(buffer, pos_, readable);
    pos_ += readable;

    return readable;
  }

  void seek(off_t offset, Whence whence) throw (dmlite::DmException) {
    switch (whence) {
      case kSet:
        pos_ = MOCK_IO_CONTENT + offset;
        break;
      case kCur:
        pos_ += offset;
        break;
      case kEnd:
        pos_ = MOCK_IO_CONTENT + strlen(MOCK_IO_CONTENT) + offset;
        break;
    }

    if (pos_ < MOCK_IO_CONTENT)
      throw dmlite::DmException(EINVAL, "MockIOHandler::seek results in a negative offset");
  }

  off_t tell(void) throw (dmlite::DmException) {
    return pos_ - MOCK_IO_CONTENT;
  }
};

// Checksum of the following excerpt
// md5:     410443f5ffe2a1555be3e999274a6dc0
// crc32:   2931039948
// adler32: 27b62f0e
const char* MockIOHandler::MOCK_IO_CONTENT =
"You're in a desert walking along in the sand when all \
of the sudden you look down, and you see a tortoise, \
it's crawling toward you.";

// Test
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
    CPPUNIT_ASSERT_EQUAL(std::string("md5"), dmlite::checksums::fullChecksumName("md"));
    CPPUNIT_ASSERT_EQUAL(std::string("md5"), dmlite::checksums::fullChecksumName("MD"));

    CPPUNIT_ASSERT_EQUAL(std::string("adler32"), dmlite::checksums::fullChecksumName("AD"));
    CPPUNIT_ASSERT_EQUAL(std::string("adler32"), dmlite::checksums::fullChecksumName("ad"));

    CPPUNIT_ASSERT_EQUAL(std::string("crc32"), dmlite::checksums::fullChecksumName("CS"));
    CPPUNIT_ASSERT_EQUAL(std::string("crc32"), dmlite::checksums::fullChecksumName("cs"));
  }

  void testTranslationToShort()
  {
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("MD5"));
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("md5"));
    CPPUNIT_ASSERT_EQUAL(std::string("MD"), dmlite::checksums::shortChecksumName("mD5"));

    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("ADLER32"));
    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("adler32"));
    CPPUNIT_ASSERT_EQUAL(std::string("AD"), dmlite::checksums::shortChecksumName("aDlEr32"));

    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("CRC32"));
    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("crc32"));
    CPPUNIT_ASSERT_EQUAL(std::string("CS"), dmlite::checksums::shortChecksumName("crC32"));
  }

  void testHexPrinter()
  {
    const unsigned char val[] = {0xBD, 0x77, 0xF5};
    std::string hexDump = dmlite::checksums::hexPrinter(val, 3);
    CPPUNIT_ASSERT_EQUAL(std::string("bd77f5"), hexDump);

    const unsigned char val2[] = {0xFF, 0x00, 0x10, 0x42, 0x23, 0x00};
    hexDump = dmlite::checksums::hexPrinter(val2, 6);
    CPPUNIT_ASSERT_EQUAL(std::string("ff0010422300"), hexDump);
  }

  void testDecPrinter()
  {
    unsigned long val = 12345600;
    std::string decDump = dmlite::checksums::decPrinter((unsigned char*)&val, sizeof(val));
    CPPUNIT_ASSERT_EQUAL(std::string("12345600"), decDump);

    unsigned long valArray[] = {1234, 5678, 42};
    decDump = dmlite::checksums::decPrinter((unsigned char*)valArray, sizeof(valArray));
    CPPUNIT_ASSERT_EQUAL(std::string("1234 5678 42"), decDump);
  }

  void testMD5()
  {
    MockIOHandler mockIO;
    std::string fullMD5 = dmlite::checksums::md5(&mockIO, 0, 0);
    CPPUNIT_ASSERT_EQUAL(std::string("410443f5ffe2a1555be3e999274a6dc0"),
                         fullMD5);

    std::string partialChecksum = dmlite::checksums::md5(&mockIO, 12, 6);
    CPPUNIT_ASSERT_EQUAL(std::string("3fd6b6210e33bb046e69f256a138e28d"),
                         partialChecksum);
  }

  void testCRC32()
  {
    MockIOHandler mockIO;
    std::string fullCKsum = dmlite::checksums::crc32(&mockIO, 0, 0);
    CPPUNIT_ASSERT_EQUAL(std::string("2931039948"),
                         fullCKsum);

    std::string partialChecksum = dmlite::checksums::crc32(&mockIO, 12, 6);
    CPPUNIT_ASSERT_EQUAL(std::string("243448890"),
                         partialChecksum);
  }

  void testAdler32()
  {
    MockIOHandler mockIO;
    std::string fullAdler32 = dmlite::checksums::adler32(&mockIO, 0, 0);
    CPPUNIT_ASSERT_EQUAL(std::string("27b62f0e"),
                         fullAdler32);

    std::string partialChecksum = dmlite::checksums::adler32(&mockIO, 12, 6);
    CPPUNIT_ASSERT_EQUAL(std::string("08aa0288"),
                         partialChecksum);
  }

  CPPUNIT_TEST_SUITE(TestChecksum);
  CPPUNIT_TEST(testBasic);
  CPPUNIT_TEST(testTranslationToLong);
  CPPUNIT_TEST(testTranslationToShort);
  CPPUNIT_TEST(testHexPrinter);
  CPPUNIT_TEST(testDecPrinter);
  CPPUNIT_TEST(testMD5);
  CPPUNIT_TEST(testCRC32);
  CPPUNIT_TEST(testAdler32);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestChecksum);

const char* TestChecksum::FILE  = "test-checksum";

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

