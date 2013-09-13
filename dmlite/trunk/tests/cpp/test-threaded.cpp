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



void* appendMapfiles(void*)
{
    for (size_t i = 0; i < 100; ++i) {
        std::ostringstream path;
        path << "/tmp/mapfile-" << i;

        try {
            dmlite::voFromDn(path.str(), "/DC=does/DN=not/OU=really/CN=matters");
        }
        catch (...) {
            // Ignore
        }
    }

    return NULL;
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

  // This is a regression test for LCGDM-1167
  // Basically, spawn a bunch of threads and start appending
  // fake files, to see if eventually it gets corrupted
  // Run with valgrind --tool=helgrind to see if it is clean!
  void testMessWithVoFromDn(void)
  {
      static const size_t NBULLIES = 20;
      pthread_t bullies[NBULLIES];

      // Spawn the bullies
      for (size_t i = 0; i < NBULLIES; ++i)
          pthread_create(&bullies[i], NULL, appendMapfiles, (void*)i);

      // Wait for them
      for (size_t i = 0; i < NBULLIES; ++i)
          pthread_join(bullies[i], NULL);
  }


  CPPUNIT_TEST_SUITE(TestThreaded);
  CPPUNIT_TEST(testBetweenThreads);
  CPPUNIT_TEST(testMessWithVoFromDn);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestThreaded);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
