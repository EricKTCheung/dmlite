#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include <utime.h>
#include "test-base.h"

#define NUMFILES 100

class TestOpendir: public TestBase
{
protected:
  const static int   MODE;
  const static char *FOLDER;
  const static char *NESTED;
  const static char *NUMFILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00)
    {
      this->catalog->removeDir(FOLDER);
    }
    TestBase::tearDown();
  }

  void testOnlyOpen()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime == after.st_atime);
  }

  void testOnlyOpenTwice()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime == after.st_atime);

    sleep(1);
    d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);
  }

  void testOpenAndRead()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);
  }

  void testOpenAndReadTwice()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);
    sleep(1);

    d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);
  }

  void testOpenAndReadTwiceNotEmpty()
  {
    struct stat before, after, nested;

    this->catalog->makeDir(NESTED, MODE);
    nested = this->catalog->stat(NESTED);

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    dirent *entry = this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT_EQUAL(nested.st_ino, entry->d_ino);

    sleep(1);
    dmlite::Directory* d2 = this->catalog->openDir(FOLDER);
    dirent *entry2 = this->catalog->readDir(d);
    this->catalog->closeDir(d);

    CPPUNIT_ASSERT_EQUAL(entry->d_ino, entry2->d_ino);

    this->catalog->removeDir(NESTED);
  }

  void testOpenAndReadTwiceFull()
  {
    struct stat before, after, nested;

    this->catalog->makeDir(NESTED, MODE);
    nested = this->catalog->stat(NESTED);

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    dirent *entry;
    do {
      entry = this->catalog->readDir(d);
    } while (entry != 0x00);

    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

//    CPPUNIT_ASSERT_EQUAL(nested.st_ino, entry->d_ino);

    sleep(1);
    dmlite::Directory* d2 = this->catalog->openDir(FOLDER);
    dirent *entry2 = this->catalog->readDir(d);
    this->catalog->closeDir(d);

//    CPPUNIT_ASSERT_EQUAL(entry->d_ino, entry2->d_ino);

    this->catalog->removeDir(NESTED);
  }

  CPPUNIT_TEST_SUITE(TestOpendir);
  //*
  CPPUNIT_TEST(testOnlyOpen);
  CPPUNIT_TEST(testOnlyOpenTwice);
  CPPUNIT_TEST(testOpenAndRead);
  CPPUNIT_TEST(testOnlyOpen);
  CPPUNIT_TEST(testOpenAndReadTwice);
  CPPUNIT_TEST(testOpenAndReadTwiceNotEmpty);
  //*/
  CPPUNIT_TEST(testOpenAndReadTwiceFull);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestOpendir::MODE    = 0777;
const char* TestOpendir::FOLDER  = "test-opendir";
const char* TestOpendir::NESTED  = "test-opendir/nested";
const char* TestOpendir::NUMFILE = "test-opendir/file_";


CPPUNIT_TEST_SUITE_REGISTRATION(TestOpendir);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

