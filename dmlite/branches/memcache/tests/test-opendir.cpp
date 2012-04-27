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
    this->catalog->makeDir(NESTED, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00)
    {
      this->catalog->removeDir(NESTED);
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
    dirent *entry2 = this->catalog->readDir(d2);
    this->catalog->closeDir(d2);

    CPPUNIT_ASSERT_EQUAL(entry->d_ino, entry2->d_ino);
  }

  void testOpenAndReadTwiceFull()
  {
    struct stat before, after, nested;

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

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);

    sleep(1);
    dmlite::Directory* d2 = this->catalog->openDir(FOLDER);
    dirent *entry2;
    do {
      entry2 = this->catalog->readDir(d2);
    } while (entry != 0x00);

    this->catalog->closeDir(d2);
  }

  void testOpenAndReadDirtyCash()
  {
    struct stat before, after, nested;
    int count, count_cached, count_cached_twice;
    
    nested = this->catalog->stat(NESTED);

    before = this->catalog->stat(FOLDER);

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    dirent *entry;
    count = 0;
    do {
      entry = this->catalog->readDir(d);
      count++;
    } while (entry != 0x00);

    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    sleep(1);
    d = this->catalog->openDir(FOLDER);
    count_cached = 0;
    do {
      entry = this->catalog->readDir(d);
      count_cached++;
    } while (entry != 0x00);

    this->catalog->closeDir(d);
    CPPUNIT_ASSERT_EQUAL(count, 2);
    CPPUNIT_ASSERT_EQUAL(count, count_cached);

    this->catalog->create(NUMFILE, MODE);

    sleep(1);
    d = this->catalog->openDir(FOLDER);
    count_cached_twice = 0;
    do {
      entry = this->catalog->readDir(d);
      count_cached_twice++;
    } while (entry != 0x00);

    this->catalog->closeDir(d);

    this->catalog->unlink(NUMFILE);

    // there should be one file more now
    CPPUNIT_ASSERT_EQUAL(count_cached + 1, count_cached_twice);
  }

  CPPUNIT_TEST_SUITE(TestOpendir);
  //*
  CPPUNIT_TEST(testOnlyOpen);
  CPPUNIT_TEST(testOnlyOpenTwice);
  CPPUNIT_TEST(testOpenAndRead);
  CPPUNIT_TEST(testOnlyOpen);
  CPPUNIT_TEST(testOpenAndReadTwice);
  CPPUNIT_TEST(testOpenAndReadTwiceNotEmpty);
  CPPUNIT_TEST(testOpenAndReadTwiceFull);
  CPPUNIT_TEST(testOpenAndReadDirtyCash);
  //*/
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

