#include <cppunit/TestAssert.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <iostream>
#include "test-base.h"

const char *TestBase::config = 0x00;


TestBase::TestBase(): pluginManager(0x00), catalog(0x00)
{
  // Nothing
}


void TestBase::setUp()
{
  pluginManager = new dmlite::PluginManager();
  pluginManager->loadConfiguration(TestBase::config);
  // Catalog
  catalog = pluginManager->getCatalogFactory()->createCatalog();
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
