#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestReplicas: public TestBase
{
protected:
  const static int   MODE;
  const static char *FOLDER;
  const static char *FILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->create(FILE, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00) {
      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
      IGNORE_NOT_EXIST(this->catalog->removeDir(FOLDER));
    }
    TestBase::tearDown();
  }

  void testAddAndRemove()
  {
    struct stat s;

    s = this->catalog->stat(FILE);

    this->catalog->addReplica(std::string(), s.st_ino, "a.host.com",
                              "http://a.host.com/replica", '-', 'P',
                              "the-pool", "the-fs");

    FileReplica replica = this->catalog->get(FILE);

    CPPUNIT_ASSERT_EQUAL((unsigned)s.st_ino, (unsigned)replica.fileid);
    CPPUNIT_ASSERT_EQUAL(std::string("http://a.host.com/replica"),
                         std::string(replica.unparsed_location));
    CPPUNIT_ASSERT_EQUAL('-', replica.status);

    this->catalog->deleteReplica(std::string(), s.st_ino, "http://a.host.com/replica");

    CPPUNIT_ASSERT_THROW(this->catalog->get(FILE), dmlite::DmException);
  }

  void testAddNoHost()
  {
    struct stat s;

    s = this->catalog->stat(FILE);

    this->catalog->addReplica(std::string(), s.st_ino, std::string(),
                              "http://a.host.com/replica", '-', 'P',
                              "the-pool", "the-fs");

    FileReplica replica = this->catalog->get(FILE);

    CPPUNIT_ASSERT_EQUAL(std::string("http://a.host.com/replica"),
                         std::string(replica.unparsed_location));
    CPPUNIT_ASSERT_EQUAL(std::string("a.host.com"),
                         std::string(replica.location.host));

    this->catalog->deleteReplica(std::string(), s.st_ino, "http://a.host.com/replica");
  }


  CPPUNIT_TEST_SUITE(TestReplicas);
  CPPUNIT_TEST(testAddAndRemove);
  CPPUNIT_TEST(testAddNoHost);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestReplicas::MODE   = 0700;
const char* TestReplicas::FOLDER = "test-replica";
const char* TestReplicas::FILE   = "test-replica/file";

CPPUNIT_TEST_SUITE_REGISTRATION(TestReplicas);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

