#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/utils/security.h>
#include "test-base.h"




void* testStackInstance(void* udata)
{
  dmlite::StackInstance* si = static_cast<dmlite::StackInstance*>(udata);
  dmlite::Catalog* catalog = si->getCatalog();

  dmlite::ExtendedStat* xstat = new dmlite::ExtendedStat();
  *xstat = catalog->extendedStat("/");

  delete si;

  return xstat;
}


class TestThreaded: public TestBase
{
public:
  void setUp()
  {
    TestBase::setUp();
    delete stackInstance;
    stackInstance = NULL;
  }



  void tearDown()
  {
    TestBase::tearDown();
  }


  // Spawn a new thread, and make sure a StackInstance
  // created in one thread can be used in a different one.
  void testBetweenThreads(void)
  {
    pthread_t thread;
    dmlite::ExtendedStat* xstat;
    dmlite::StackInstance* si;

    si = new dmlite::StackInstance(this->pluginManager);
    si->setSecurityCredentials(this->cred1);
    pthread_create(&thread, NULL, testStackInstance, si);
    pthread_join(thread, (void**)(&xstat));
    CPPUNIT_ASSERT_EQUAL(std::string("/"), xstat->name);

    si = new dmlite::StackInstance(this->pluginManager);
    si->setSecurityCredentials(this->cred1);
    pthread_create(&thread, NULL, testStackInstance, si);
    pthread_join(thread, (void**)(&xstat));
    CPPUNIT_ASSERT_EQUAL(std::string("/"), xstat->name);

    delete xstat;
  }


  CPPUNIT_TEST_SUITE(TestThreaded);
  CPPUNIT_TEST(testBetweenThreads);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestThreaded);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
