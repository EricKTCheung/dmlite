#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/io.h>
#include <dmlite/cpp/poolmanager.h>
#include "test-base.h"



static bool operator == (const dmlite::Acl& a, const dmlite::Acl& b)
{
  return a.serialize() == b.serialize();
}



static std::ostringstream& operator << (std::ostringstream& of, const dmlite::Acl& acl)
{
  of << acl.serialize();
  return of;
}



class TestOverwrite: public TestBase
{
protected:
  static const char* FILE;
  std::string PATH;
  dmlite::Acl ACL;

public:

  void setUp()
  {
    PATH = BASE_DIR + "/" + FILE;
    TestBase::setUp();

    const dmlite::SecurityContext* ctx = this->stackInstance->getSecurityContext();

    std::stringstream ss;
    ss << "A7" << ctx->user.getUnsigned("uid") << ",C0"
       << ctx->groups[0].getUnsigned("gid") << ",E70,F50";
    ACL = dmlite::Acl(ss.str());
  }

  void tearDown()
  {
    if (stackInstance)
      IGNORE_NOT_EXIST(stackInstance->getCatalog()->unlink(PATH));
    TestBase::tearDown();
  }

  void testOverwrite()
  {
    dmlite::PoolManager* poolManager = stackInstance->getPoolManager();
    dmlite::Catalog*     catalog     = stackInstance->getCatalog();

    // Put the file first
    dmlite::Location loc = poolManager->whereToWrite(PATH);

    dmlite::IOHandler* fd = stackInstance->getIODriver()->createIOHandler(loc[0].path, O_WRONLY | O_CREAT, loc[0]);
    fd->write("abc", 3);
    fd->close();
    delete fd;

    stackInstance->getIODriver()->doneWriting(loc[0].path, loc[0]);

    // Change the mode
    catalog->setAcl(PATH, ACL);
    mode_t expectedMode = (catalog->extendedStat(PATH).stat.st_mode) & 0777;

    // Overwrite
    stackInstance->set("overwrite", true);
    loc = poolManager->whereToWrite(PATH);

    fd = stackInstance->getIODriver()->createIOHandler(loc[0].path, O_WRONLY | O_CREAT, loc[0]);
    fd->write("abc", 3);
    fd->close();
    delete fd;

    stackInstance->getIODriver()->doneWriting(loc[0].path, loc[0]);

    // Mode must be the same
    dmlite::ExtendedStat xstat = catalog->extendedStat(PATH);
    CPPUNIT_ASSERT_EQUAL(expectedMode, xstat.stat.st_mode & 0777);
    CPPUNIT_ASSERT_EQUAL(ACL, xstat.acl);
  }

  CPPUNIT_TEST_SUITE(TestOverwrite);
  CPPUNIT_TEST(testOverwrite);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestOverwrite::FILE = "test-overwrite";

CPPUNIT_TEST_SUITE_REGISTRATION(TestOverwrite);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
