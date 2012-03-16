#include <cppunit/TestAssert.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <iostream>
#include <stdlib.h>
#include "test-base.h"

const char* TestBase::config = 0x00;
const char* TestBase::TEST_USER   = "/C=CH/O=CERN/OU=GD/CN=Test user 0";
const char* TestBase::TEST_USER_2 = "/C=CH/O=CERN/OU=GD/CN=Test user 1";


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
}


void TestBase::setUp()
{
  pluginManager = new dmlite::PluginManager();
  pluginManager->loadConfiguration(TestBase::config);

  // Catalog
  catalog = pluginManager->getCatalogFactory()->createCatalog();

  // Users
  std::vector<std::string> groups;
  std::vector<gid_t>       gids;

  groups.push_back("dteam");
  groups.push_back("org.glite.voms-test");
  this->catalog->getIdMap(TEST_USER, groups, &uid1, &gids);
  if (gids.size() < 2)
    throw dmlite::DmException(DM_NO_SUCH_GROUP, std::string("No GID's given for ") + TEST_USER);
  gid1_1 = gids[0];
  gid1_2 = gids[1];

  groups.clear();
  groups.push_back("atlas");
  this->catalog->getIdMap(TEST_USER_2, groups, &uid2, &gids);
  if (gids.size() == 0)
    throw dmlite::DmException(DM_NO_SUCH_GROUP, std::string("No GID's given for ") + TEST_USER_2);
  gid2 = gids[0];

  user_groups.clear();
  user_groups.push_back("dteam");
  user_groups.push_back("org.glite.voms-test");

  // Change dir
  this->catalog->changeDir(BASE_DIR);
}



void TestBase::tearDown()
{
  if (catalog)
    delete catalog;
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
  return runner.run();
}
