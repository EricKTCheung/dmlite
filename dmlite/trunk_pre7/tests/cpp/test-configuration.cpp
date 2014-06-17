#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/utils/security.h>
#include "test-base.h"

class TestConfiguration: public TestBase
{
protected:

public:

  void testSetAndGet()
  {
    try {
      std::cout << pluginManager->getConfiguration("HostDNIsRoot")
                << std::endl;
    }
    catch (dmlite::DmException& e) {
      // Doesn't really matter this
      std::cout << e.what() << std::endl;
    }
  }

  CPPUNIT_TEST_SUITE(TestConfiguration);
  CPPUNIT_TEST(testSetAndGet);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestConfiguration);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
