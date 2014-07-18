#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <netdb.h>
#include "test-base.h"

class TestGuid: public TestBase
{
protected:
  const static char* FILE;
  const static char* GUID;
  
public:

  void setUp()
  {
    TestBase::setUp();

    try {
      this->catalog->unlink(FILE);
    }
    catch (...) {
      // Nothing
    }
  }

  void tearDown()
  {
    try {
      this->catalog->unlink(FILE);
    }
    catch (...) {
      // Nothing
    }
    TestBase::tearDown();
  }

  void testPut()
  {
    struct xstat    xStat;
    struct uri      location;
    char            hostname[HOST_NAME_MAX];
    struct hostent* host;

    gethostname(hostname, sizeof(hostname));
    host = gethostbyname(hostname);

    std::string token = this->catalog->put(FILE, &location, GUID);

    if (strcmp(location.host, host->h_name) != 0)
      CPPUNIT_FAIL(std::string("This test only support local PUT's!! ") +
                   host->h_name + " != " + location.host);

    fclose(fopen(location.path, "w"));

    this->catalog->putDone(FILE, token);

    xStat = this->catalog->extendedStat(FILE);

    CPPUNIT_ASSERT_EQUAL(std::string(GUID), std::string(xStat.guid));
  }

  CPPUNIT_TEST_SUITE(TestGuid);
  CPPUNIT_TEST(testPut);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestGuid::FILE = "/dpm/cern.ch/home/dteam/test-guid";
const char* TestGuid::GUID = "1234-5678-FF-4321";

CPPUNIT_TEST_SUITE_REGISTRATION(TestGuid);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}