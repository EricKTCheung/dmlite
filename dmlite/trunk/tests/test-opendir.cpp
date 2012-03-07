#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include <utime.h>
#include "test-base.h"

class TestOpendir: public TestBase
{
protected:
  const static char *FOLDER;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, 0700);
  }

  void tearDown()
  {
    if (this->catalog != 0x00)
      this->catalog->removeDir(FOLDER);
    TestBase::tearDown();
  }

  void testDefault()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime < after.st_atime);
  }


  CPPUNIT_TEST_SUITE(TestOpendir);
  CPPUNIT_TEST(testDefault);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestOpendir::FOLDER  = "test-opendir";

CPPUNIT_TEST_SUITE_REGISTRATION(TestOpendir);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

