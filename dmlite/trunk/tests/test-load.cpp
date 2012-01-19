#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <dmlite/dmlite++.h>
#include <iostream>
#include <sys/stat.h>

class PluginLoaded: public CppUnit::TestFixture {
protected:
  dmlite::PluginManager *pluginManager;

public:
  void setUp()
  {
    pluginManager = new dmlite::PluginManager();
  }

  void tearDown()
  {
    if (pluginManager)
      delete pluginManager;
  }

  void testDummy()
  {
    // Loading the dummy plugin alone should fail
    CPPUNIT_ASSERT_THROW(pluginManager->loadPlugin("../core/libdm.so", "plugin_dummy"), dmlite::DmException);
  }

  void testNested()
  {
    dmlite::Catalog *catalog;
    struct stat  buf;

    // Load first
    try {
      pluginManager->loadPlugin("../plugins/adapter/plugin_adapter.so", "plugin_adapter_ns");
      // Load second
      pluginManager->loadPlugin("../plugins/profiler/plugin_profiler.so", "plugin_profiler");
      // Configure (adapter should recognise)
      pluginManager->set("Host", "arioch.cern.ch");
      // Get interface
      catalog = pluginManager->getCatalogFactory()->create();
      CPPUNIT_ASSERT(catalog != 0x00);
    }
    catch (dmlite::DmException exc) {
      CPPUNIT_FAIL(exc.what());
    }
    // Call stat (it is irrelevant, but errno can not be DM_NOT_IMPLEMENTED)
    try {
      buf = catalog->stat("/");
      CPPUNIT_ASSERT_EQUAL((unsigned)0, buf.st_uid);
    }
    catch (dmlite::DmException exc)
    {
      CPPUNIT_ASSERT(exc.code() != DM_NOT_IMPLEMENTED);
    }
    // Free
    if (catalog) delete catalog;
  }

  CPPUNIT_TEST_SUITE(PluginLoaded);
  CPPUNIT_TEST(testDummy);
  CPPUNIT_TEST(testNested);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(PluginLoaded);

int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run();
}

