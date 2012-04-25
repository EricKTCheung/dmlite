#include <cppunit/TestAssert.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include "test-base.h"
#include <stdlib.h>

const char* TestBase::config             = 0x00;
const char* TestBase::TEST_USER          = "/C=CH/O=CERN/OU=GD/CN=Test user 0";
unsigned    TestBase::TEST_USER_NGROUPS  = 2;
const char* TestBase::TEST_USER_GROUPS[] = {"dteam", "org.glite.voms-test"};

const char* TestBase::TEST_USER_2          = "/C=CH/O=CERN/OU=GD/CN=Test user 1";
unsigned    TestBase::TEST_USER_2_NGROUPS  = 1;
const char* TestBase::TEST_USER_2_GROUPS[] = {"atlas"};


TestBase::TestBase(): pluginManager(0x00), catalog(0x00)
{
  const char *testHome;

  // Get the test root directory
  if ((testHome = getenv("DPNS_HOME")) != NULL)
    BASE_DIR = std::string(testHome);
  else if ((testHome = getenv("LFC_HOME")) != NULL)
    BASE_DIR = std::string(testHome);
  else
    throw dmlite::DmException(DM_INVALID_VALUE,
                              "Could not guess the test directory. Try setting DPNS_HOME or LFC_HOME");

  memset(&cred1, 0x00, sizeof(cred1));
  memset(&cred2, 0x00, sizeof(cred2));

  root.resizeGroup(1);
  root.getUser().uid   = 0;
  root.getGroup(0).gid = 0;
}


void TestBase::setUp()
{
  pluginManager = new dmlite::PluginManager();
  pluginManager->loadConfiguration(TestBase::config);
  
  stackInstance = new dmlite::StackInstance(pluginManager);
  
  // Initialize security context
  stackInstance->setSecurityContext(root);

  // Catalog
  catalog = stackInstance->getCatalog();

  // Credentials 1
  this->cred1.client_name = TEST_USER;
  this->cred1.fqans       = TEST_USER_GROUPS;
  this->cred1.nfqans      = TEST_USER_NGROUPS;
  this->cred1.mech        = CRED_MECH_NONE;

  // Credentials 2
  this->cred2.client_name = TEST_USER_2;
  this->cred2.fqans       = TEST_USER_2_GROUPS;
  this->cred2.nfqans      = TEST_USER_2_NGROUPS;
  this->cred2.mech        = CRED_MECH_NONE;

  // Change dir
  this->catalog->changeDir(BASE_DIR);
}



void TestBase::tearDown()
{
  if (stackInstance)
    delete stackInstance;
  if (pluginManager)
    delete pluginManager;
}



int testBaseMain(int argn, char **argv)
{
  if (argn < 2) {
    std::cerr << "Usage: " << argv[0] << " <config file>" << std::endl;
    return -1;
  }

  TestBase::config = argv[1];

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run()?0:-1;
}
