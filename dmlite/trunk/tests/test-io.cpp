#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <dmlite/dmlite++.h>
#include <iostream>
#include <ios>
#include <iosfwd>

class TestIO: public CppUnit::TestFixture
{
private:
  dmlite::PluginManager* manager;
  dmlite::IOFactory*     io;

public:
  static const char* config;

  void setUp()
  {
    manager = new dmlite::PluginManager();
    manager->loadConfiguration(config);
    io = manager->getIOFactory();
  }

  void tearDown()
  {
    if (manager)
      delete manager;
  }

  void testOpen(void)
  {
    char b;

    std::istream* s = io->createIO("/dev/zero", std::ios_base::in);

    *s >> b;
    CPPUNIT_ASSERT_EQUAL((char)0, b);
    
    delete s;
  }

  void testNotExist(void)
  {
    CPPUNIT_ASSERT_THROW(io->createIO("/this/should/not/exist", std::ios_base::in),
                         dmlite::DmException);
  }

  void testWriteAndRead(void)
  {
    std::string istring;
    std::string ostring;

    // Open to write
    ostring = "This-is-the-string-to-be-checked!";
    std::ostream* os = io->createIO("/tmp/test-io-wr", std::ios_base::out | std::ios_base::trunc);
    *os << ostring;
    delete os;

    // Open to read
    std::istream* is = io->createIO("/tmp/test-io-wr", std::ios_base::in);
    *is >> istring;
    CPPUNIT_ASSERT_EQUAL(ostring, istring);
    delete is;
  }

  
  CPPUNIT_TEST_SUITE(TestIO);
  CPPUNIT_TEST(testOpen);
  CPPUNIT_TEST(testNotExist);
  CPPUNIT_TEST(testWriteAndRead);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestIO);

const char* TestIO::config = 0x00;

int main(int argn, char **argv)
{
  if (argn < 2) {
    std::cerr << "Usage: " << argv[0] << " <config file>" << std::endl;
    return -1;
  }

  TestIO::config = argv[1];

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run();
}
