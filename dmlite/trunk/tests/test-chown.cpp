#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestChown: public TestBase
{
protected:
  const static char *FILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->create(FILE, 0755);
  }

  void tearDown()
  {
    if (this->catalog) {
      this->catalog->setUserId(uid1, gid1_1, TEST_USER);
      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
    }

    TestBase::tearDown();
  }

  void testRoot()
  {
    // Root should be able to change the owner and the group
    this->catalog->changeOwner(FILE, uid1, gid1_1);

    struct stat s = this->catalog->stat(FILE);

    CPPUNIT_ASSERT_EQUAL(uid1, s.st_uid);
    CPPUNIT_ASSERT_EQUAL(gid1_1, s.st_gid);
  }

  void testOwner()
  {
    struct stat s;

    // Change the owner
    this->catalog->changeOwner(FILE, uid1, gid1_1);
    // Change the user
    this->catalog->setUserId(uid1, gid1_1, TEST_USER);
    this->catalog->setVomsData("dteam", user_groups);

    // It should be OK a -1, -1
    this->catalog->changeOwner(FILE, -1, -1);
    s = this->catalog->stat(FILE);
    CPPUNIT_ASSERT_EQUAL(uid1, s.st_uid);
    CPPUNIT_ASSERT_EQUAL(gid1_1, s.st_gid);

    // Also, it should be ok to change to current value
    this->catalog->changeOwner(FILE, uid1, gid1_1);
    s = this->catalog->stat(FILE);
    CPPUNIT_ASSERT_EQUAL(uid1, s.st_uid);
    CPPUNIT_ASSERT_EQUAL(gid1_1, s.st_gid);

    // It should NOT be able to change the user
    try {
      this->catalog->changeOwner(FILE, uid2, -1);
    }
    catch (dmlite::DmException e) {
      CPPUNIT_ASSERT_EQUAL(DM_BAD_OPERATION, e.code());
    }

    // It should BE able to change the group to one it belongs to
    this->catalog->changeOwner(FILE, -1, gid1_2);
    s = this->catalog->stat(FILE);
    CPPUNIT_ASSERT_EQUAL(uid1, s.st_uid);
    CPPUNIT_ASSERT_EQUAL(gid1_2, s.st_gid);

    // It should NOT be able to change the group to one it does not belong to
    try {
      this->catalog->changeOwner(FILE, -1, gid2);
    }
    catch (dmlite::DmException e) {
      CPPUNIT_ASSERT_EQUAL(DM_BAD_OPERATION, e.code());
    }
  }

  void testOther()
  {
    // Change the owner
    this->catalog->changeOwner(FILE, uid1, gid1_1);
    // Change the user
    this->catalog->setUserId(uid2, gid2, TEST_USER_2);

    // It should NOT be able to change the group or the owner
    try {
      this->catalog->changeOwner(FILE, uid2, -1);
    }
    catch (dmlite::DmException e) {
      CPPUNIT_ASSERT_EQUAL(DM_BAD_OPERATION, e.code());
    }

    try {
      this->catalog->changeOwner(FILE, -1, -gid2);
    }
    catch (dmlite::DmException e) {
      CPPUNIT_ASSERT_EQUAL(DM_BAD_OPERATION, e.code());
    }
  }

  CPPUNIT_TEST_SUITE(TestChown);
  CPPUNIT_TEST(testRoot);
  CPPUNIT_TEST(testOwner);
  CPPUNIT_TEST(testOther);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestChown);

const char* TestChown::FILE        = "/dpm/cern.ch/home/dteam/test-chown";

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

