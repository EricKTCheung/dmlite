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

  void testOpenAndRead()
  {
    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    sleep(2);
    dmlite::Directory* d = this->catalog->openDir(FOLDER);
    this->catalog->readDir(d);
    this->catalog->closeDir(d);

    after = this->catalog->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime < after.st_atime);
  }
  

  void testOpenAndReadNoUpdate()
  {
    dmlite::Catalog *catalog2;
    
    this->pluginManager->configure("NsUpdateAccessTime", "no");
    catalog2 = this->pluginManager->getCatalogFactory()->createCatalog(0x00);
    catalog2->setSecurityContext(&this->root);
    catalog2->changeDir(BASE_DIR);
    
    struct stat before, after;

    before = catalog2->stat(FOLDER);

    sleep(2);
    dmlite::Directory* d = catalog2->openDir(FOLDER);
    catalog2->readDir(d);
    catalog2->closeDir(d);

    after = catalog2->stat(FOLDER);

    CPPUNIT_ASSERT(before.st_atime == after.st_atime);
    
    delete catalog2;
  }


  CPPUNIT_TEST_SUITE(TestOpendir);
  CPPUNIT_TEST(testOnlyOpen);
  CPPUNIT_TEST(testOpenAndRead);
  CPPUNIT_TEST(testOpenAndReadNoUpdate);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestOpendir::FOLDER  = "test-opendir";

CPPUNIT_TEST_SUITE_REGISTRATION(TestOpendir);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

