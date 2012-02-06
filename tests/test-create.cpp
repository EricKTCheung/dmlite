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
  const static char *FILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00) {
      try {
        this->catalog->unlink(FILE);
      }
      catch (...) { }

      try {
        this->catalog->unlink(SYMLINK);
      }
      catch (...) { }

      try {
        this->catalog->removeDir(NESTED);
      }
      catch (...) { }

      this->catalog->umask(022);
      this->catalog->removeDir(FOLDER);
    }
    TestBase::tearDown();
  }

  void testRegular()
  {
    struct stat s;
    time_t ini_mtime;
    // Empty
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(0, (int)s.st_nlink);
    CPPUNIT_ASSERT_EQUAL(MODE | S_IFDIR, (int)s.st_mode);
    ini_mtime = s.st_mtime;
    sleep(1); // Or mtime may be the same, since the operation can be fast

    // Add a folder
    this->catalog->makeDir(NESTED, MODE);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(1, (int)s.st_nlink);
    CPPUNIT_ASSERT(s.st_mtime > ini_mtime);

    // Add a symlink
    this->catalog->symlink(FOLDER, SYMLINK);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(2, (int)s.st_nlink);
    
    // Add a file
    this->catalog->create(FILE, MODE);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(3, (int)s.st_nlink);
    
    // Remove the file
    this->catalog->unlink(FILE);
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

  void testSetGid()
  {
    struct stat s;

    this->catalog->changeMode(FOLDER, MODE | S_ISGID);
    s = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(MODE | S_ISGID | S_IFDIR, (int)s.st_mode);

    this->catalog->makeDir(NESTED, MODE);
    s = this->catalog->stat(NESTED);
    CPPUNIT_ASSERT_EQUAL(MODE | S_IFDIR | S_ISGID, (int)s.st_mode);
    this->catalog->removeDir(NESTED);
  }

  void testUmask()
  {
    struct stat s;

    this->catalog->umask(066);
    mode_t prev = this->catalog->umask(0077);
    CPPUNIT_ASSERT_EQUAL(066, (int)prev);
    
    this->catalog->makeDir(NESTED, 0777);
    s = this->catalog->stat(NESTED);
    CPPUNIT_ASSERT_EQUAL(0700 | S_IFDIR, (int)s.st_mode);

    this->catalog->removeDir(NESTED);
  }

  void testExistsNoReplicas()
  {
    struct stat s;

    this->catalog->create(FILE, MODE);
    // Again, should not fail
    this->catalog->create(FILE, MODE);

    s = this->catalog->stat(FILE);
    CPPUNIT_ASSERT_EQUAL(0l, (long)s.st_size);
  }

  void testExistsWithReplicas()
  {
    struct stat s;

    this->catalog->create(FILE, MODE);
    s = this->catalog->stat(FILE);

    this->catalog->addReplica("", s.st_ino,
                              "localhost", "sfn://something",
                              '-', 'P',
                              "", "");

    CPPUNIT_ASSERT_THROW(this->catalog->create(FILE, MODE), dmlite::DmException);

    this->catalog->deleteReplica("", s.st_ino, "sfn://something");
  }

  CPPUNIT_TEST_SUITE(TestCreate);
  CPPUNIT_TEST(testRegular);
  CPPUNIT_TEST(testSetGid);
  CPPUNIT_TEST(testUmask);
  CPPUNIT_TEST(testExistsNoReplicas);
  CPPUNIT_TEST(testExistsWithReplicas);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestCreate::MODE    = 0700;
const char* TestCreate::FOLDER  = "/dpm/cern.ch/home/dteam/test-create";
const char* TestCreate::NESTED  = "/dpm/cern.ch/home/dteam/test-create/nested/";
const char* TestCreate::SYMLINK = "/dpm/cern.ch/home/dteam/test-create/test-link";
const char* TestCreate::FILE    = "/dpm/cern.ch/home/dteam/test-create/test-file";

CPPUNIT_TEST_SUITE_REGISTRATION(TestCreate);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
