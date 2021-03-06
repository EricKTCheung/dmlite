#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include "test-base.h"

class TestStat: public TestBase
{
protected:
  struct stat statBuf, statBufCached, iStat;
  const static int   MODE;
  const static char *FOLDER;
  const static char *FILE;
  const static char *NESTED;
  const static char *SYMLINK;
  const static char *RELATIVE;
  const static char *SYMREL;

  dmlite::Acl ACL;

public:

  void setUp()
  {
    TestBase::setUp();

    this->stackInstance->setSecurityCredentials(cred1);
    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->makeDir(NESTED, MODE);

    this->catalog->symlink(FOLDER, SYMLINK);
    this->catalog->symlink(RELATIVE, SYMREL);

    std::stringstream ss;
    const dmlite::SecurityContext* ctx = this->stackInstance->getSecurityContext();

    ss << "A7" << getUid(ctx) << ",C0" << getGid(ctx) << ",E70,F50";
    ACL = dmlite::Acl(ss.str());
  }

  void tearDown()
  {
    if (this->catalog) {
      this->stackInstance->setSecurityContext(root);

      IGNORE_NOT_EXIST(this->catalog->unlink(SYMLINK));
      IGNORE_NOT_EXIST(this->catalog->unlink(SYMREL));
      IGNORE_NOT_EXIST(this->catalog->removeDir(NESTED));
      IGNORE_NOT_EXIST(this->catalog->removeDir(FOLDER));
    }
    TestBase::tearDown();
  }

  void testRegular()
  {
    statBuf = this->catalog->extendedStat(FOLDER).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);

    statBuf = this->catalog->extendedStat(NESTED).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
  }

  void testDifferentUser()
  {
    // Change user
    this->stackInstance->setSecurityCredentials(cred2);

    // First level should pass
    statBuf = this->catalog->extendedStat(FOLDER).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);

    // Nested shouldn't
    CPPUNIT_ASSERT_THROW(this->catalog->extendedStat(NESTED), dmlite::DmException);
  }

  void testSymLink()
  {
    // Stat the link
    statBuf = this->catalog->extendedStat(SYMLINK, false).stat;
    CPPUNIT_ASSERT_EQUAL(S_IFLNK, (int)statBuf.st_mode & S_IFLNK);
    // Stat the folder
    statBuf = this->catalog->extendedStat(SYMLINK).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
    // Stat relative link
    statBuf = this->catalog->extendedStat(SYMREL).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
  }

  void testRelative()
  {
    struct stat test;
    
    statBuf = this->catalog->extendedStat(RELATIVE).stat;
    CPPUNIT_ASSERT_EQUAL(MODE, (int)statBuf.st_mode & MODE);
    
    // Ending with .
    statBuf = this->catalog->extendedStat(NESTED).stat;
    test    = this->catalog->extendedStat(std::string(NESTED) + "/.").stat;
    CPPUNIT_ASSERT_EQUAL(statBuf.st_ino, test.st_ino);
    
    // Ending with ..
    statBuf = this->catalog->extendedStat(FOLDER).stat;
    test    = this->catalog->extendedStat(std::string(NESTED) + "/..").stat;
    CPPUNIT_ASSERT_EQUAL(statBuf.st_ino, test.st_ino);
  }

  /// @note Adapter will not pass this since it does not implement inode access
  void testIStat()
  {
    dmlite::INode* inode;
    try {
      inode = this->stackInstance->getINode();
    }
    catch (dmlite::DmException& e) {
      // If not implemented, it is not implemented (some plugins can not)
      if (e.code() == DMLITE_SYSERR(DMLITE_NO_INODE)) return;
      throw;
    }

    statBuf = this->catalog->extendedStat(FOLDER).stat;
    iStat   = inode->extendedStat(statBuf.st_ino).stat;;

    // Check some only, if they are fine, the rest should be fine.
    CPPUNIT_ASSERT_EQUAL(statBuf.st_ino,  iStat.st_ino);
    CPPUNIT_ASSERT_EQUAL(statBuf.st_mode, iStat.st_mode);
    CPPUNIT_ASSERT_EQUAL(statBuf.st_uid,  iStat.st_uid);
  }

  void testCacheRegular()
  {
    statBuf = this->catalog->extendedStat(NESTED).stat;
    statBufCached = this->catalog->extendedStat(NESTED).stat;

    CPPUNIT_ASSERT_EQUAL((int)statBuf.st_mode, (int)statBufCached.st_mode);
    CPPUNIT_ASSERT_EQUAL((int)statBuf.st_ino, (int)statBufCached.st_ino);
  }

  void testCacheDifferentUser()
  {
    this->stackInstance->setSecurityCredentials(cred1);

    // stat to cache result
    statBuf = this->catalog->extendedStat(FOLDER).stat;

    // Change user
    this->stackInstance->setSecurityCredentials(cred2);

    // First level should pass
    statBufCached = this->catalog->extendedStat(FOLDER).stat;
    CPPUNIT_ASSERT_EQUAL((int)statBuf.st_mode, (int)statBufCached.st_mode);
    CPPUNIT_ASSERT_EQUAL((int)statBuf.st_ino, (int)statBufCached.st_ino);

    // switch user back to cache
    this->stackInstance->setSecurityCredentials(cred1);
    statBuf = this->catalog->extendedStat(NESTED).stat;
    this->stackInstance->setSecurityCredentials(cred2);

    // Nested shouldn't
    CPPUNIT_ASSERT_THROW(this->catalog->extendedStat(NESTED), dmlite::DmException);
  }

  /// See LCGDM-818
  void testAclNoFollow()
  {
    this->catalog->create(FILE, 0750);
    this->catalog->setAcl(FILE, ACL);

    dmlite::ExtendedStat xstat = this->catalog->extendedStat(FILE, false);

    CPPUNIT_ASSERT_EQUAL(ACL, xstat.acl);
  }

  
  CPPUNIT_TEST_SUITE(TestStat);
  CPPUNIT_TEST(testRegular);
  CPPUNIT_TEST(testDifferentUser);
  CPPUNIT_TEST(testSymLink);
  CPPUNIT_TEST(testRelative);
  CPPUNIT_TEST(testIStat);
  CPPUNIT_TEST(testCacheRegular);
  CPPUNIT_TEST(testCacheDifferentUser);
  CPPUNIT_TEST(testAclNoFollow);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestStat::MODE        = 0700;
const char* TestStat::FOLDER      = "test-stat";
const char* TestStat::FILE        = "test-stat.file";
const char* TestStat::NESTED      = "test-stat/nested";
const char* TestStat::SYMLINK     = "test-link";
const char* TestStat::RELATIVE    = "test-stat/../test-stat/./nested";
const char* TestStat::SYMREL      = "test-link-rel";

CPPUNIT_TEST_SUITE_REGISTRATION(TestStat);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
