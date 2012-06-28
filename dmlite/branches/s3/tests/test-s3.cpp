#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <dmlite/cpp/dmlite.h>
#include <iostream>
#include <ios>
#include <iosfwd>
#include <string.h>
#include "test-base.h"

class TestS3: public TestBase
{
protected:
  dmlite::PoolManager   *poolManager;
  std::vector<Pool>     pools;

  static const char* KEY;

public:
  static const char* config;

  void setUp()
  {
    TestBase::setUp();

    this->poolManager = this->stackInstance->getPoolManager();
    // Ask for the pools
    pools = poolManager->getPools();
  }

  void tearDown()
  {
    TestBase::tearDown();
  }

  void testGetLocation(void)
  {
    Location loc;
    FileReplica repl;
    std::memcpy(repl.rfn, KEY, strlen(KEY));

    for (int i = 0; i < pools.size(); ++i) {
      dmlite::PoolHandler *handler = stackInstance->getPoolDriver(pools[i].pool_type)->createPoolHandler(pools[i].pool_name);
      printf("%s\n", KEY);
      printf("pool type = %s, pool name = %s.\n",
              handler->getPoolType().c_str(),
              handler->getPoolName().c_str());
      if (handler->getPoolType() == std::string("s3")) {
        loc = handler->getLocation(repl);
        printf("Host, Path: %s + %s\n", loc.host, loc.path);
        handler->putLocation(std::string(KEY));
      }
    }
  }

  CPPUNIT_TEST_SUITE(TestS3);
  CPPUNIT_TEST(testGetLocation);
  CPPUNIT_TEST_SUITE_END();
};

//const char* TestS3::KEY  = "/dpm/cern.ch/home/dteam/atlas-higgs-boson.found";
const char* TestS3::KEY  = "atlas-higgs-boson.found";
//const char* TestS3::KEY  = "testfile.txt";

CPPUNIT_TEST_SUITE_REGISTRATION(TestS3);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}


