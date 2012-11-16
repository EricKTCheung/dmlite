#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sstream>
#include <vector>
#include "test-base.h"


class TestAuthn: public TestBase {
protected:
  dmlite::Authn* authn;
  
  const static char* USER;
  const static char* GROUP;
  
public:
  
  void setUp()
  {
    TestBase::setUp();
    this->authn = this->stackInstance->getAuthn();
  }
  
  void tearDown()
  {
    if (this->authn) {
      try {
        this->authn->deleteUser(USER);
      }
      catch (const dmlite::DmException& e) {
        if (e.code() != DMLITE_NO_SUCH_USER) throw;
      }
      try {
        this->authn->deleteGroup(GROUP);
      }
      catch (const dmlite::DmException& e) {
        if (e.code() != DMLITE_NO_SUCH_GROUP) throw;
      }
    }
    TestBase::tearDown();
  }
  
  void testUserAddAndRemove()
  {
    dmlite::UserInfo u = this->authn->newUser(USER);
    CPPUNIT_ASSERT_EQUAL(std::string(USER), u.name);
    
    this->authn->getUser(USER);
    
    this->authn->deleteUser(USER);
    CPPUNIT_ASSERT_THROW(this->authn->getUser(USER), dmlite::DmException);
  }
  
  void testUserMetadata()
  {
    // Skip test on adapter (does not support this, and it will not)
    if (this->authn->getImplId() == "NsAdapterCatalog" ||
        this->authn->getImplId() =="DpmAdapterCatalog") return;

    dmlite::UserInfo u = this->authn->newUser(USER);
    CPPUNIT_ASSERT_EQUAL(std::string(USER), u.name);
    
    u["additional"] = 42u;
    this->authn->updateUser(u);
    
    u.clear();
    
    u = this->authn->getUser(USER);
    CPPUNIT_ASSERT_EQUAL(42lu, u.getUnsigned("additional"));
  }
  
  void testGroupAddAndRemove()
  {
    dmlite::GroupInfo g = this->authn->newGroup(GROUP);
    CPPUNIT_ASSERT_EQUAL(std::string(GROUP), g.name);
    
    this->authn->getGroup(GROUP);
    
    this->authn->deleteGroup(GROUP);
    CPPUNIT_ASSERT_THROW(this->authn->getGroup(GROUP), dmlite::DmException);
  }
  
  void testGroupMetadata()
  {
    // Skip test on adapter (does not support this, and it will not)
    if (this->authn->getImplId() == "NsAdapterCatalog" ||
        this->authn->getImplId() =="DpmAdapterCatalog") return;

    dmlite::GroupInfo g = this->authn->newGroup(GROUP);
    CPPUNIT_ASSERT_EQUAL(std::string(GROUP), g.name);
    
    g["additional"] = 42u;
    this->authn->updateGroup(g);
    
    g.clear();
    
    g = this->authn->getGroup(GROUP);
    CPPUNIT_ASSERT_EQUAL(42lu, g.getUnsigned("additional"));
  }
  
  void testList()
  {
    std::vector<dmlite::GroupInfo> groups = this->authn->getGroups();
    std::vector<dmlite::UserInfo>  users  = this->authn->getUsers();
    
    CPPUNIT_ASSERT(groups.size() > 1);
    CPPUNIT_ASSERT(users.size() > 1);
  }
  
  void testById()
  {
    uid_t uid;
    gid_t gid;
    
    dmlite::UserInfo u     = this->authn->newUser(USER);
    uid = u.getUnsigned("uid");
    dmlite::UserInfo utest = this->authn->getUser("uid", uid);
    CPPUNIT_ASSERT_EQUAL(u.name, utest.name);
    
    dmlite::GroupInfo g     = this->authn->newGroup(GROUP);
    gid = g.getUnsigned("gid");
    dmlite::GroupInfo gtest = this->authn->getGroup("gid", gid);
    CPPUNIT_ASSERT_EQUAL(g.name, gtest.name);
  }
  
  void testBuiltIn()
  {
    dmlite::PluginManager pm;

    pm.configure("AnonymousUser", "nobody");
    pm.configure("AnonymousGroup", "users");

    dmlite::StackInstance stack(&pm);

    // Generate a security context with a user that does not exist
    dmlite::SecurityCredentials creds;
    creds.clientName = "/DC=org/DC=example/OU=hsimpson";
    creds.fqans.push_back("does/not-exist");

    stack.setSecurityCredentials(creds);
    const dmlite::SecurityContext* secctx = stack.getSecurityContext();

    CPPUNIT_ASSERT_EQUAL(std::string("nobody"), secctx->user.name);
    CPPUNIT_ASSERT_EQUAL((size_t)1, secctx->groups.size());
    CPPUNIT_ASSERT_EQUAL(std::string("users"), secctx->groups[0].name);

    // Unknown user without group must be nobody with default group (nobody)
    creds.fqans.clear();

    stack.setSecurityCredentials(creds);
    secctx = stack.getSecurityContext();

    CPPUNIT_ASSERT_EQUAL(std::string("nobody"), secctx->user.name);
    CPPUNIT_ASSERT_EQUAL((size_t)1, secctx->groups.size());
    CPPUNIT_ASSERT_EQUAL(std::string("nobody"), secctx->groups[0].name);

    // Knwon user, unknown group
    creds.clientName = "root";
    creds.fqans.push_back("does/not-exist");

    stack.setSecurityCredentials(creds);
    secctx = stack.getSecurityContext();

    CPPUNIT_ASSERT_EQUAL(std::string("root"), secctx->user.name);
    CPPUNIT_ASSERT_EQUAL((size_t)1, secctx->groups.size());
    CPPUNIT_ASSERT_EQUAL(std::string("users"), secctx->groups[0].name);
  }

  CPPUNIT_TEST_SUITE(TestAuthn);
  CPPUNIT_TEST(testUserAddAndRemove);
  CPPUNIT_TEST(testUserMetadata);
  CPPUNIT_TEST(testGroupAddAndRemove);
  CPPUNIT_TEST(testGroupMetadata);
  CPPUNIT_TEST(testList);
  CPPUNIT_TEST(testById);
  CPPUNIT_TEST(testBuiltIn);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestAuthn::USER  = "/C=COM/O=EXAMPLE/OU=WWW/CN=User";
const char* TestAuthn::GROUP = "/C=COM/O=EXAMPLE/OU=WWW/CN=Group";

CPPUNIT_TEST_SUITE_REGISTRATION(TestAuthn);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
