#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"

class TestSecurity: public TestBase
{
public:

  void setUp()
  {
    TestBase::setUp();
  }

  void tearDown()
  {
    TestBase::tearDown();
  }

  void testCred()
  {
    const dmlite::SecurityContext *ctx;

    this->catalog->setSecurityCredentials(cred1);
    ctx = &this->catalog->getSecurityContext();

    CPPUNIT_ASSERT_EQUAL(std::string(TEST_USER),
                         std::string(ctx->getUser().name));
    CPPUNIT_ASSERT(0 != ctx->getUser().uid);
  }

  void testDiff()
  {
    dmlite::SecurityContext ctx1, ctx2;

    this->catalog->setSecurityCredentials(cred1);
    ctx1 = this->catalog->getSecurityContext();

    this->catalog->setSecurityCredentials(cred2);
    ctx2 = this->catalog->getSecurityContext();

    CPPUNIT_ASSERT(ctx1.getUser().uid != ctx2.getUser().uid);
  }

  CPPUNIT_TEST_SUITE(TestSecurity);
  CPPUNIT_TEST(testCred);
  CPPUNIT_TEST(testDiff);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestSecurity);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
