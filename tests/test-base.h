#ifndef TEST_BASE_H
#define	TEST_BASE_H

#include <cppunit/TestFixture.h>
#include <dmlite/dmlite++.h>
#include <dmlite/dm_exceptions.h>

#define IGNORE_NOT_EXIST(f) try { f; } catch (dmlite::DmException e) { if (e.code() != DM_NO_SUCH_FILE) throw; }


class TestBase: public CppUnit::TestFixture {
public:
  TestBase();
  void setUp();
  void tearDown();

protected:
  dmlite::PluginManager *pluginManager;
  dmlite::Catalog       *catalog;
private:
  friend int testBaseMain(int, char**);
  static const char *config;
};


int testBaseMain(int argn, char **argv);

#endif
