#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <dmlite/dmlite++.h>
#include <iostream>
#include <ios>
#include <iosfwd>
#include <string.h>

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

    dmlite::IOHandler* s = io->createIO("/dev/zero", std::ios_base::in);

    s->read(&b, sizeof(char));
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
    const char ostring[] = "This-is-the-string-to-be-checked!";

    // Open to write
    dmlite::IOHandler* os = io->createIO("/tmp/test-io-wr", std::ios_base::out | std::ios_base::trunc);
    os->write(ostring, strlen(ostring));
    delete os;

    // Open to read
    char buffer[512] = "";
    dmlite::IOHandler* is = io->createIO("/tmp/test-io-wr", std::ios_base::in);
    size_t nb = is->read(buffer, sizeof(buffer));

    CPPUNIT_ASSERT_EQUAL(strlen(ostring), nb);
    CPPUNIT_ASSERT_EQUAL(std::string(ostring), std::string(buffer));

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
