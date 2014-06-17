#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <cstring>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/security.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <sys/stat.h>
#include "test-base.h"

class TestSecurity: public TestBase
{
private:
  static const std::string HOST_CERTIFICATE;

public:

  void setUp()
  {
    TestBase::setUp();
  }

  void tearDown()
  {
    TestBase::tearDown();
  }

  void testCred()
  {
    const dmlite::SecurityContext *ctx;

    this->stackInstance->setSecurityCredentials(cred1);
    ctx = this->stackInstance->getSecurityContext();

    CPPUNIT_ASSERT_EQUAL(std::string(TEST_USER),
                         std::string(ctx->user.name));
  }

  void testDiff()
  {
    dmlite::SecurityContext ctx1, ctx2;

    this->stackInstance->setSecurityCredentials(cred1);
    ctx1 = *this->stackInstance->getSecurityContext();

    this->stackInstance->setSecurityCredentials(cred2);
    ctx2 = *this->stackInstance->getSecurityContext();

    CPPUNIT_ASSERT(boost::any_cast<uid_t>(ctx1.user["uid"]) !=
                   boost::any_cast<uid_t>(ctx2.user["uid"]));
  }
  
  void testHostCert()
  {
    // Initialize
    dmlite::SecurityCredentials hostCreds;
    hostCreds.clientName = dmlite::getCertificateSubject(HOST_CERTIFICATE);
    
    // Check
    const dmlite::SecurityContext* ctx;
    this->stackInstance->setSecurityCredentials(hostCreds);
    ctx = this->stackInstance->getSecurityContext();
    
    CPPUNIT_ASSERT_EQUAL(0u, boost::any_cast<uid_t>(ctx->user["uid"]));
  }

  CPPUNIT_TEST_SUITE(TestSecurity);
  CPPUNIT_TEST(testCred);
  CPPUNIT_TEST(testDiff);
  CPPUNIT_TEST(testHostCert);
  CPPUNIT_TEST_SUITE_END();
};

const std::string TestSecurity::HOST_CERTIFICATE = "/etc/grid-security/hostcert.pem";


CPPUNIT_TEST_SUITE_REGISTRATION(TestSecurity);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
