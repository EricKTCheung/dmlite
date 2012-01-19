#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestChDir: public TestBase
{
protected:
  struct stat statBuf;
  const static int   MODE;
  const static char *FOLDER;
  const static char *NESTED;

public:

  void setUp()
  {
    TestBase::setUp();

    try {
      this->catalog->makeDir(FOLDER, MODE);
    } catch (dmlite::DmException e) {
      if (e.code() != DM_EXISTS)
        throw;
    }
    try {
      this->catalog->makeDir(std::string(FOLDER) + "/" + std::string(NESTED), MODE);
    } catch (dmlite::DmException e) {
      if (e.code() != DM_EXISTS)
        throw;
    }
  }

  void tearDown()
  {
    if (this->catalog) {
      this->catalog->changeDir("/");
      this->catalog->removeDir(std::string(FOLDER) + "/" + std::string(NESTED));
      this->catalog->removeDir(FOLDER);
    }
    TestBase::tearDown();
  }

  void testRegular()
  {
    this->catalog->changeDir(FOLDER);

    statBuf = this->catalog->stat(NESTED);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);

    CPPUNIT_ASSERT_EQUAL(std::string(FOLDER), this->catalog->getWorkingDir());
  }

  CPPUNIT_TEST_SUITE(TestChDir);
  CPPUNIT_TEST(testRegular);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestChDir::MODE    = 0755;
const char* TestChDir::FOLDER  = "/dpm/cern.ch/home/dteam/test-chdir";
const char* TestChDir::NESTED  = "chdir";

CPPUNIT_TEST_SUITE_REGISTRATION(TestChDir);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

