#include <cppunit/extensions/HelperMacros.h>
#include "test-base.h"

class TestComment: public TestBase
{
public:

  void testComment()
  {
    const std::string comment("This is a comment!@1234XX");

    this->catalog->setComment("/dpm", comment);
    CPPUNIT_ASSERT_EQUAL(comment, this->catalog->getComment("/dpm"));
  }


  CPPUNIT_TEST_SUITE(TestComment);
  CPPUNIT_TEST(testComment);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestComment);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
