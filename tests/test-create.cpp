#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestCreate: public TestBase
{
protected:
  struct stat statBuf, iStat;
  const static int   MODE;
  const static char *FOLDER;
  const static char *NESTED;
  const static char *SYMLINK;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, MODE);
  }

  void tearDown()
  {
    this->catalog->removeDir(FOLDER);
    TestBase::tearDown();
  }

  void testRegular()
  {
    struct stat s;
    // Empty
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(0, (int)s.st_nlink);
    CPPUNIT_ASSERT_EQUAL(MODE | S_IFDIR, (int)s.st_mode);
    // Add a folder
    this->catalog->makeDir(NESTED, MODE);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(1, (int)s.st_nlink);
    // Add a symlink
    this->catalog->symlink(FOLDER, SYMLINK);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(2, (int)s.st_nlink);
    // Remove the folder
    this->catalog->removeDir(NESTED);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(1, (int)s.st_nlink);
    // And the symlink
    this->catalog->unlink(SYMLINK);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(0, (int)s.st_nlink);
  }



  CPPUNIT_TEST_SUITE(TestCreate);
  CPPUNIT_TEST(testRegular);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestCreate::MODE    = 0700;
const char* TestCreate::FOLDER  = "/dpm/cern.ch/home/dteam/test-create";
const char* TestCreate::NESTED  = "/dpm/cern.ch/home/dteam/test-create/nested";
const char* TestCreate::SYMLINK = "/dpm/cern.ch/home/dteam/test-create/test-link";

CPPUNIT_TEST_SUITE_REGISTRATION(TestCreate);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
