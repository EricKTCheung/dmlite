#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include "../plugins/common/Security.h"

class Security: public CppUnit::TestFixture {
protected:
  struct stat stat_;
  UserInfo  user;
  GroupInfo group;
public:

  void setUp()
  {
    // Set the security fields
    stat_.st_uid  = 200;
    stat_.st_gid  = 200;
    stat_.st_mode = 0750;
    user.banned = group.banned = 0;
  }

  void testOwner()
  {
    std::vector<GroupInfo> gids;
    // Owner and group
    group.gid = user.uid = 200;
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
    // Owner but no group
    group.gid = 500;
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
  }

  void testGroup()
  {
    std::vector<GroupInfo> gids;
    // Main group matches
    group.gid = 200;
    user.uid  = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));
    // Main doesn't match, but a secondary does
    GroupInfo secondary;
    secondary.banned = 0;
    secondary.gid = 200;
    gids.push_back(secondary);
    group.gid = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));
  }

  void testOther()
  {
    std::vector<GroupInfo> gids;
    // No match
    group.gid = user.uid = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD));
  }

  void testAcl()
  {
    std::vector<GroupInfo> gids;
    const char *acl = "A70,B7101,C7102,D5103,E70,F00,a70,c70,f50";
    // No match
    group.gid = user.uid = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD));
    // Match user
    user.uid = 101;
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));

    // Match main group
    user.uid  = 500;
    group.gid = 103;
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IEXEC));

    // Match secondary
    GroupInfo secondary;
    group.gid        = 500;
    secondary.gid    = 103;
    secondary.banned = 0;
    gids.push_back(secondary);
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IEXEC));
  }

  void testUserBanned()
  {
    std::vector<GroupInfo> gids;
    // Owner and group
    group.gid = user.uid = 200;
    user.banned = 1;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
  }

  void testMainGroupBanned()
  {
    std::vector<GroupInfo> gids;
    // Owner matches, group banned
    group.gid    = user.uid = 200;
    group.banned = 1;
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));
    // Owner doesn't match. Group does, but banned.
    user.uid = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));
  }

  void testSecondaryBanned()
  {
    std::vector<GroupInfo> gids;
    GroupInfo secondary;
    // Owner doesn't match, main group does, but secondary banned.
    user.uid         = 500;
    group.gid        = 200;
    secondary.gid    = 500;
    secondary.banned = 1;
    gids.push_back(secondary);
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));
    // Owner doesn't match, main group neither, secondary does but banned.
    gids.clear();
    group.gid     = 500;
    secondary.gid = 200;
    gids.push_back(secondary);
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));

    // Owner doesn't match, main group neither and banned. Secondary OK.
    gids.clear();
    group.banned     = 1;
    secondary.banned = 0;
    gids.push_back(secondary);
    CPPUNIT_ASSERT_EQUAL(0, dmlite::checkPermissions(user, group, gids,
                                                     0x00, stat_,
                                                     S_IREAD | S_IEXEC));
  }

  void testAclBanned()
  {
    std::vector<GroupInfo> gids;
    const char *acl = "A70,B7101,C7102,D5103,E70,F00,a70,c70,f50";
    // Match user
    user.uid    = 101;
    user.banned = 1;
    group.gid   = 500;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IWRITE | S_IEXEC));

    // Match main group
    user.uid     = 500;
    user.banned  = 0;
    group.gid    = 103;
    group.banned = 1;
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IEXEC));

    // Main group banned, but secondary matches (and banned)
    GroupInfo secondary;
    group.gid        = 500;
    secondary.gid    = 103;
    secondary.banned = 1;
    gids.push_back(secondary);
    CPPUNIT_ASSERT_EQUAL(1, dmlite::checkPermissions(user, group, gids,
                                                     acl, stat_,
                                                     S_IREAD | S_IEXEC));
  }



  void testGetVoFromRole()
  {
    std::string vo;
    vo = dmlite::voFromRole(std::string("/dteam/Role=NULL/Capability=NULL"));
    CPPUNIT_ASSERT_EQUAL(std::string("dteam"), vo);
  }



  CPPUNIT_TEST_SUITE(Security);
  CPPUNIT_TEST(testOwner);
  CPPUNIT_TEST(testGroup);
  CPPUNIT_TEST(testOther);
  CPPUNIT_TEST(testAcl);
  CPPUNIT_TEST(testUserBanned);
  CPPUNIT_TEST(testMainGroupBanned);
  CPPUNIT_TEST(testSecondaryBanned);
  CPPUNIT_TEST(testAclBanned);
  CPPUNIT_TEST(testGetVoFromRole);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(Security);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run();
}

