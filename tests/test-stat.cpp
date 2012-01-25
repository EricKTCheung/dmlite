#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestStat: public TestBase
{
protected:
  struct stat statBuf, iStat;
  const static int   MODE;
  const static char *FOLDER;
  const static char *NESTED;
  const static char *SYMLINK;
  const static char *RELATIVE;
  const static char *SYMREL;
  const static char *TEST_USER;
  const static char *TEST_USER_2;

  uid_t uid, uid2;
  gid_t gid, gid2;
public:

  void setUp()
  {
    TestBase::setUp();
    std::vector<std::string> empty;
    std::vector<gid_t>       gids;


    this->catalog->getIdMap(TEST_USER, empty, &uid, &gids);
    gid = gids[0];
    this->catalog->setUserId(uid, gid, TEST_USER);
    
    this->catalog->getIdMap(TEST_USER_2, empty, &uid2, &gids);
    gid2 = gids[0];

    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->makeDir(NESTED, MODE);

    this->catalog->symlink(FOLDER, SYMLINK);
    this->catalog->symlink(RELATIVE, SYMREL);
  }

  void tearDown()
  {
    if (this->catalog) {
      this->catalog->setUserId(uid, gid, TEST_USER);

      try {
        this->catalog->unlink(SYMLINK);
      }
      catch (...) {}
      try {
        this->catalog->unlink(SYMREL);
      }
      catch (...) {}

      try {
        this->catalog->removeDir(NESTED);
      }
      catch (...) {}
      try {
        this->catalog->removeDir(FOLDER);
      }
      catch (...) {}
    }
    TestBase::tearDown();
  }

  void testRegular()
  {
    statBuf = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);

    statBuf = this->catalog->stat(NESTED);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
  }

  void testDifferentUser()
  {
    // Change user
    this->catalog->setUserId(uid2, gid2, TEST_USER_2);

    // First level should pass
    statBuf = this->catalog->stat(FOLDER);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);

    // Nested shouldn't
    CPPUNIT_ASSERT_THROW(statBuf = this->catalog->stat(NESTED), dmlite::DmException);
  }

  void testSymLink()
  {
    // Stat the link
    statBuf = this->catalog->linkStat(SYMLINK);
    CPPUNIT_ASSERT_EQUAL(S_IFLNK, (int)statBuf.st_mode & S_IFLNK);
    // Stat the folder
    statBuf = this->catalog->stat(SYMLINK);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
    // Stat relative link
    statBuf = this->catalog->stat(SYMREL);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
  }

  void testRelative()
  {
    statBuf = this->catalog->stat(RELATIVE);
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
  }

  void testIStat()
  {
    statBuf = this->catalog->stat(FOLDER);
    iStat   = this->catalog->stat(statBuf.st_ino);

    // Check some only, if they are fine, the rest should be fine.
    CPPUNIT_ASSERT_EQUAL(statBuf.st_ino,  iStat.st_ino);
    CPPUNIT_ASSERT_EQUAL(statBuf.st_mode, iStat.st_mode);
    CPPUNIT_ASSERT_EQUAL(statBuf.st_uid,  iStat.st_uid);
  }

  CPPUNIT_TEST_SUITE(TestStat);
  CPPUNIT_TEST(testRegular);
  CPPUNIT_TEST(testDifferentUser);
  CPPUNIT_TEST(testSymLink);
  CPPUNIT_TEST(testRelative);
  CPPUNIT_TEST(testIStat);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestStat::MODE    = 0700;
const char* TestStat::FOLDER  = "/dpm/cern.ch/home/dteam/test-stat";
const char* TestStat::NESTED  = "/dpm/cern.ch/home/dteam/test-stat/nested";
const char* TestStat::SYMLINK = "/dpm/cern.ch/home/dteam/test-link";
const char* TestStat::RELATIVE = "/dpm/cern.ch/home/dteam/../../home/dteam/test-stat";
const char* TestStat::SYMREL   = "/dpm/cern.ch/home/dteam/test-link-rel";
const char* TestStat::TEST_USER   = "/C=CH/O=CERN/OU=GD/CN=Test user 0";
const char* TestStat::TEST_USER_2 = "/C=CH/O=CERN/OU=GD/CN=Test user 1";

CPPUNIT_TEST_SUITE_REGISTRATION(TestStat);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
