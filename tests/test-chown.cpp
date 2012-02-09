#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestChown: public TestBase
{
protected:
  const static char *FILE;
  const static char *TEST_USER;
  const static char *TEST_USER_2;

  uid_t uid1, uid2;
  gid_t gid1_1, gid1_2, gid2;

  std::vector<std::string> user_groups;

public:

  void setUp()
  {
    TestBase::setUp();

    std::vector<std::string> groups;
    std::vector<gid_t>       gids;

    groups.push_back("dteam");
    groups.push_back("org.glite.voms-test");
    this->catalog->getIdMap(TEST_USER, groups, &uid1, &gids);
    if (gids.size() < 2)
      throw dmlite::DmException(DM_NO_SUCH_GROUP, std::string("No GID's given for ") + TEST_USER);
    gid1_1 = gids[0];
    gid1_2 = gids[1];

    groups.clear();
    groups.push_back("atlas");
    this->catalog->getIdMap(TEST_USER_2, groups, &uid2, &gids);
    if (gids.size() == 0)
      throw dmlite::DmException(DM_NO_SUCH_GROUP, std::string("No GID's given for ") + TEST_USER_2);
    gid2 = gids[0];

    this->catalog->create(FILE, 0755);

    user_groups.clear();
    user_groups.push_back("org.glite.voms-test");
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
const char* TestChown::TEST_USER   = "/C=CH/O=CERN/OU=GD/CN=Test user 0";
const char* TestChown::TEST_USER_2 = "/C=CH/O=CERN/OU=GD/CN=Test user 1";

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

