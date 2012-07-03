#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include <unistd.h>
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
//    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->makeDir(NESTED, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00)
    {
      this->catalog->removeDir(NESTED);
//      this->catalog->removeDir(FOLDER);
    }
    TestBase::tearDown();
  }

  void testOnlyOpen()
  {
    struct stat before, after;

    before = this->catalog->extendedStat(FOLDER).stat;

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;

    CPPUNIT_ASSERT(before.st_atime == after.st_atime);
  }

  void testOnlyOpenTwice()
  {
    struct stat before, after;

    before = this->catalog->extendedStat(FOLDER).stat;

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;

    CPPUNIT_ASSERT(before.st_atime == after.st_atime);

    sleep(1);
    d = this->catalog->openDir(FOLDER);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;
  }

  void testOpenAndRead()
  {
    struct stat before, after;

    before = this->catalog->extendedStat(FOLDER).stat;

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);
  }

  void testOpenAndReadTwice()
  {
    this->pluginManager->configure("UpdateAccessTime", "no");
    dmlite::StackInstance* si2 = new dmlite::StackInstance(this->pluginManager);
    
    si2->setSecurityCredentials(this->cred1);
    
    dmlite::Catalog* catalog2 = si2->getCatalog();
    catalog2->setStackInstance(this->stackInstance);
    catalog2->setSecurityContext(&this->root);
    catalog2->changeDir(BASE_DIR);
    
    struct stat before, after;

    before = catalog2->extendedStat(FOLDER).stat;

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = catalog2->extendedStat(FOLDER).stat;

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);
    sleep(1);

    d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;
  }

  void testOpenAndReadTwiceNotEmpty()
  {
    struct stat before, after, nested;

    nested = this->catalog->extendedStat(NESTED).stat;

    before = this->catalog->extendedStat(FOLDER).stat;

    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    dirent *entry = this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;

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

    nested = this->catalog->extendedStat(NESTED).stat;

    before = this->catalog->extendedStat(FOLDER).stat;


    sleep(1);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    dirent *entry;
    do {
      entry = this->catalog->readDir(d);
    } while (entry != 0x00);

    this->catalog->closeDir(d);

    after = this->catalog->extendedStat(FOLDER).stat;

    CPPUNIT_ASSERT(before.st_atime <= after.st_atime);

    sleep(1);
    dmlite::Directory* d2 = this->catalog->openDir(FOLDER);
    dirent *entry2;
    do {
      entry2 = this->catalog->readDir(d2);
    } while (entry2 != 0x00);

    this->catalog->closeDir(d2);

  }

  void testOpenAndReadDirtyCash()
  {
    struct stat before, after, nested;
    int count, count_cached, count_cached_twice;
    
  }

  void testCreateFiles()
  {
    std::stringstream file_prefix;
    std::string file_name;
    int repeats = this->repeats;


    if (this->catalog->openDir(FOLDER) == 0x00)
      this->catalog->makeDir(FOLDER, MODE);

    for (int i = 0; i < repeats; ++i) {
      file_prefix << NUMFILE;
      file_prefix << i;
      file_name = file_prefix.str();
      file_prefix.str("");
      std::cout << file_name << std::endl;
      this->catalog->create(file_name, MODE);
    }
  }

  void testDeleteFiles()
  {
    std::stringstream file_prefix;
    std::string file_name;
    int repeats = this->repeats;

    file_prefix << NUMFILE;

    this->catalog->removeDir(FOLDER);

    for (int i = 0; i < repeats; ++i) {
      file_prefix << i;
      file_name = file_prefix.str();
      file_prefix.str("");
      this->catalog->unlink(file_name);
    }
  }

  CPPUNIT_TEST_SUITE(TestOpendir);
  //CPPUNIT_TEST(testOnlyOpen);
  //CPPUNIT_TEST(testOnlyOpenTwice);
  CPPUNIT_TEST(testOpenAndReadTwiceFull);
  //CPPUNIT_TEST(testCreateFiles);
  //*/
  //CPPUNIT_TEST(testOpenAndReadNoUpdate);
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

