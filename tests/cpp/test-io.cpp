#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>


#include "dmlite/cpp/utils/security.h"
#include <cppunit/extensions/HelperMacros.h>
#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/io.h>
#include <iostream>
#include <ios>
#include <iosfwd>



class TestIO: public CppUnit::TestFixture
{
private:
  dmlite::PluginManager*  manager;
  dmlite::IODriver*       io;
  dmlite::StackInstance*  si;
  dmlite::Extensible      extras;
  dmlite::SecurityContext root;

public:
  static const char* config;

  void setUp()
  {
    manager = new dmlite::PluginManager();
    manager->loadConfiguration(config);
    manager->configure("TokenPassword", "test");
    manager->configure("TokenId", "DN");
    si = new dmlite::StackInstance(manager);
    
    dmlite::GroupInfo group;
    group.name   = "root";
    group["gid"] = 0u;
  
    root.user["uid"] = 0u;
    root.groups.push_back(group);
    
    si->setSecurityContext(root);
    
    io = si->getIODriver();
  }

  void tearDown()
  {
    if (si)
      delete si;
    if (manager)
      delete manager;
    extras.clear();
  }

  void testOpen(void)
  {
    char b;
    
    extras["token"] = dmlite::generateToken("", "/dev/zero", "test",
                                            1000, false);
    dmlite::IOHandler* s = io->createIOHandler("/dev/zero",
                                               O_RDONLY,
                                               extras);

    s->read(&b, sizeof(char));
    CPPUNIT_ASSERT_EQUAL((char)0, b);
    
    delete s;
  }

  void testNotExist(void)
  {
    extras["token"] = dmlite::generateToken("", "/this/should/not/exist", "test",
                                            1000, false);
    CPPUNIT_ASSERT_THROW(io->createIOHandler("/this/should/not/exist",
                                             O_RDONLY,
                                             extras),
                         dmlite::DmException);
  }

  void testWriteAndRead(void)
  {
    const char ostring[] = "This-is-the-string-to-be-checked!";
    
    extras["token"] = dmlite::generateToken("", "/tmp/test-io-wr", "test",
                                            1000, true);

    // Open to write
    dmlite::IOHandler* os = io->createIOHandler("/tmp/test-io-wr",
                                                O_WRONLY | O_CREAT,
                                                extras);
    os->write(ostring, strlen(ostring));
    delete os;

    // Open to read
    char buffer[512] = "";
    dmlite::IOHandler* is = io->createIOHandler("/tmp/test-io-wr",
                                                O_RDONLY,
                                                extras);
    size_t nb = is->read(buffer, sizeof(buffer));

    CPPUNIT_ASSERT_EQUAL(strlen(ostring), nb);
    CPPUNIT_ASSERT_EQUAL(std::string(ostring), std::string(buffer));

    delete is;
  }

  void testInsecure(void)
  {
      const char ostring[] = "test-insecure";

      dmlite::IOHandler* os = io->createIOHandler("/tmp/test-insecure",
                                                  O_WRONLY | O_CREAT | dmlite::IODriver::kInsecure,
                                                  dmlite::Extensible());

      os->write(ostring, strlen(ostring));
      delete os;

      char buffer[512];
      dmlite::IOHandler* is = io->createIOHandler("/tmp/test-insecure",
                                                  O_RDONLY | dmlite::IODriver::kInsecure,
                                                  dmlite::Extensible());

      size_t nb = is->read(buffer, sizeof(buffer));

      CPPUNIT_ASSERT_EQUAL(strlen(ostring), nb);
      CPPUNIT_ASSERT_EQUAL(std::string(ostring), std::string(buffer));

      delete is;
  }

  void testWritev(void)
  {
    // Write
    char *ostrings[] = {"string-01", "string-02", "string-03"};

    dmlite::IOHandler* os = io->createIOHandler("/tmp/writev",
                                                O_WRONLY | O_CREAT | dmlite::IODriver::kInsecure,
                                                dmlite::Extensible());

    struct iovec iovector[3];
    size_t expected = 0;
    for (size_t i = 0; i < 3; ++i) {
      iovector[i].iov_base = static_cast<void*>(ostrings[i]);
      iovector[i].iov_len = strlen(ostrings[i]);
      expected += strlen(ostrings[i]);
    }

    size_t nb = os->writev(iovector, 3);
    CPPUNIT_ASSERT_EQUAL(expected, nb);

    delete os;

    // Read it back
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    dmlite::IOHandler* is = io->createIOHandler("/tmp/writev",
                                                O_RDONLY | dmlite::IODriver::kInsecure,
                                                dmlite::Extensible());
    nb = is->read(buffer, sizeof(buffer));
    delete is;

    CPPUNIT_ASSERT_EQUAL(expected, nb);
    CPPUNIT_ASSERT_EQUAL(std::string("string-01string-02string-03"), std::string(buffer));
  }

  void testReadv(void)
  {
    // Write
    char ostring[] = "string-01string-02string-03";

    dmlite::IOHandler* os = io->createIOHandler("/tmp/readv",
                                                O_WRONLY | O_CREAT | dmlite::IODriver::kInsecure,
                                                dmlite::Extensible());
    CPPUNIT_ASSERT_EQUAL(sizeof(ostring), os->write(ostring, sizeof(ostring)));
    delete os;

    // Read it back using readv
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    struct iovec iovector[3];
    iovector[0].iov_base = buffer;
    iovector[0].iov_len  = 9;
    iovector[1].iov_base = buffer + 9;
    iovector[1].iov_len  = 9;
    iovector[2].iov_base = buffer + 18;
    iovector[3].iov_len  = 9;

    dmlite::IOHandler* is = io->createIOHandler("/tmp/writev",
                                                O_RDONLY | dmlite::IODriver::kInsecure,
                                                dmlite::Extensible());
    size_t nb = is->readv(iovector, 3);
    delete is;

    CPPUNIT_ASSERT_EQUAL(strlen(ostring), nb);
    CPPUNIT_ASSERT_EQUAL(std::string(ostring), std::string(buffer));
  }

  
  CPPUNIT_TEST_SUITE(TestIO);
  CPPUNIT_TEST(testOpen);
  CPPUNIT_TEST(testNotExist);
  CPPUNIT_TEST(testWriteAndRead);
  CPPUNIT_TEST(testInsecure);
  CPPUNIT_TEST(testWritev);
  CPPUNIT_TEST(testReadv);
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
  return runner.run()?0:1;
}
