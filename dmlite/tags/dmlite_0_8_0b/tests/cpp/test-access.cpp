#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/utils/security.h>
#include "test-base.h"

class TestAccess: public TestBase
{
protected:
  static const char* FILE;
  static const char* REPLICA_HOST;
  static const char* REPLICA_RFN;

public:
  void setUp()
  {
    TestBase::setUp();

    this->stackInstance->setSecurityCredentials(this->cred1);
  }

  void tearDown()
  {
    if (this->catalog) {
      try {
        std::vector<dmlite::Replica> replicas = this->catalog->getReplicas(FILE);
        for (unsigned i = 0; i < replicas.size(); ++i) {
          this->catalog->deleteReplica(replicas[i]);
        }
      }
      catch (dmlite::DmException& e) {
        if (e.code() != ENOENT) throw;
      }
      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
    }
    TestBase::tearDown();
  }

  void testHome()
  {
    CPPUNIT_ASSERT_EQUAL(true, this->catalog->access(BASE_DIR, R_OK));
  }

  void testRoot()
  {
    CPPUNIT_ASSERT_EQUAL(false, this->catalog->access("/", W_OK));
  }

  void testNonExistant()
  {
    CPPUNIT_ASSERT_THROW(this->catalog->access("/hopefully/fake/path", R_OK), dmlite::DmException);
  }

  void testExistingReplica()
  {
    this->catalog->create(FILE, 0700);
    dmlite::ExtendedStat xstat = this->catalog->extendedStat(FILE);

    dmlite::Replica replica;
    replica.fileid = xstat.stat.st_ino;
    replica.server = REPLICA_HOST;
    replica.rfn    = REPLICA_RFN;
    replica.status = dmlite::Replica::kAvailable;

    this->catalog->addReplica(replica);

    CPPUNIT_ASSERT_EQUAL(true, this->catalog->accessReplica(REPLICA_RFN, R_OK));
    CPPUNIT_ASSERT_EQUAL(true, this->catalog->accessReplica(REPLICA_RFN, X_OK));
    CPPUNIT_ASSERT_EQUAL(false, this->catalog->accessReplica(REPLICA_RFN, W_OK));
  }

  void testNoReplica()
  {
    // Let's give some margin, and allow a forbidden instead of a ENOENT
    try {
      CPPUNIT_ASSERT_EQUAL(false, this->catalog->accessReplica("/etc/passwd", R_OK));
    }
    catch (dmlite::DmException& e) {
      CPPUNIT_ASSERT_EQUAL(DMLITE_NO_SUCH_REPLICA, e.code());
    }
  }

  CPPUNIT_TEST_SUITE(TestAccess);
  CPPUNIT_TEST(testHome);
  CPPUNIT_TEST(testRoot);
  CPPUNIT_TEST(testNonExistant);
  CPPUNIT_TEST(testExistingReplica);
  CPPUNIT_TEST(testNoReplica);
  CPPUNIT_TEST_SUITE_END();
};

const char* TestAccess::FILE         = "test-access-replica";
const char* TestAccess::REPLICA_RFN  = "dummy.example.com:/legacy/path";
const char* TestAccess::REPLICA_HOST = "dummy.example.com";

CPPUNIT_TEST_SUITE_REGISTRATION(TestAccess);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
