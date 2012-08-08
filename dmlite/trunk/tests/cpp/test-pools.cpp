#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/poolmanager.h>
#include <dpm/dpm_api.h>
#include <iostream>
#include <vector>
#include "test-base.h"

const float GB = 1073741824;

static std::ostream& operator << (std::ostream& o, const dmlite::Pool& p)
{
  o << p.name << '@' << p.type << '[' << p.serialize() << ']';
  return o;
}


static void printPools(dmlite::StackInstance* stack, const std::vector<dmlite::Pool>& pools)
{
  for (unsigned i = 0; i < pools.size(); ++i) {
    try {
      dmlite::PoolHandler *handler = stack->getPoolDriver(pools[i].type)->createPoolHandler(pools[i].name);

      std::cout << "Pool type:   " << handler->getPoolType()   << std::endl
                << "Pool name:   " << handler->getPoolName()   << std::endl
                << "Capacity:    " << handler->getTotalSpace() / GB << " GB" << std::endl
                << "Free:        " << handler->getFreeSpace() / GB  << " GB" << std::endl
                << "Metadata:" << std::endl
                << pools[i].serialize() << std::endl;

      delete handler;
    }
    catch (dmlite::DmException& e) {
      if (e.code() != DM_UNKNOWN_POOL_TYPE)
        throw;
      std::cout << "Pool type:   " << pools[i].type << std::endl
                << "Pool name:   " << pools[i].name << std::endl
                << "UNKNOWN POOL TYPE!!" << std::endl;
    }
    std::cout << std::endl;
  }
}


class TestPools: public TestBase {
protected:
  dmlite::PoolManager* poolManager;
  
public:
  
  void setUp()
  {
    TestBase::setUp();
    poolManager = this->stackInstance->getPoolManager();
  }
  
  void tearDown()
  {
    dmlite::Pool pool;
    
    if (poolManager) {
      try {
        pool.name = "test_hadoop";
        pool.type = "hadoop";
        poolManager->deletePool(pool);
      }
      catch (dmlite::DmException& e) {
        if (e.code() != DM_NO_SUCH_POOL) throw;
      }

      try {
        pool.name = "test_fs";
        pool.type = "filesystem";
        poolManager->deletePool(pool);
      }
      catch (dmlite::DmException& e) {
        if (e.code() != DM_NO_SUCH_POOL) throw;
      }
    }
    
    TestBase::tearDown();
  }
  
  void testBase()
  {
    std::cout << "============= ALL POOLS ============="
            << std::endl << std::endl;
    printPools(stackInstance, poolManager->getPools(dmlite::PoolManager::kAny));

    // Available for read
    std::cout << "======== AVAILABLE FOR READ ========="
              << std::endl << std::endl;
    printPools(stackInstance, poolManager->getPools(dmlite::PoolManager::kForRead));

    // Available for write
    std::cout << "======== AVAILABLE FOR WRITE ========"
              << std::endl << std::endl;
    printPools(stackInstance, poolManager->getPools(dmlite::PoolManager::kForWrite));

    // Show not available
    std::cout << "=========== NOT AVAILABLE ==========="
              << std::endl << std::endl;
    printPools(stackInstance, poolManager->getPools(dmlite::PoolManager::kNone));
  }
  
  void testUnknown()
  {
    dmlite::Pool pool;
    
    pool.name = "does_not_matter";
    pool.type = "unknown_pool_type";
    
    CPPUNIT_ASSERT_THROW(poolManager->newPool(pool), dmlite::DmException);
  }
  
  void testAddHadoop()
  {
    dmlite::Pool pool;
    
    // Add it
    pool.name = "test_hadoop";
    pool.type = "hadoop";
    pool["hostname"] = std::string("namenode.cern.ch");
    pool["port"]     = 8020;
    pool["username"] = std::string("test");
    pool["mode"]     = std::string("r");
    
    poolManager->newPool(pool);
    
    // Get it back and check it was OK
    pool.name = pool.type = "XYZ";
    pool.clear();
    
    pool = poolManager->getPool("test_hadoop");
    
    CPPUNIT_ASSERT_EQUAL(std::string("test_hadoop"),      pool.name);
    CPPUNIT_ASSERT_EQUAL(std::string("hadoop"),           pool.type);
    CPPUNIT_ASSERT_EQUAL(std::string("namenode.cern.ch"), pool.getString("hostname"));
    CPPUNIT_ASSERT_EQUAL(8020ul,                          pool.getUnsigned("port"));
    CPPUNIT_ASSERT_EQUAL(std::string("test"),             pool.getString("username"));
    CPPUNIT_ASSERT_EQUAL(std::string("r"),                pool.getString("mode"));
    
    poolManager->deletePool(pool);
    CPPUNIT_ASSERT_THROW(poolManager->getPool("test_hadoop"), dmlite::DmException);
  }
  
  // This is a special case, so we have to make sure it is actually going
  // to the DPM!!
  void testAddFilesystem()
  {
    dmlite::Pool pool;
    std::vector<boost::any> gids;
    boost::any gid;
    
    gid = 101u;
    gids.push_back(gid);
    gid = 105u;
    gids.push_back(gid);
    
    // Initialize and add
    pool.name = "test_fs";
    pool.type = "filesystem";
    
    pool["defsize"]         = 102400;
    pool["gc_start_thresh"] = 0;
    pool["gc_stop_thresh"]  = 0;
    pool["def_lifetime"]    = 1234;
    pool["defpintime"]      = 4321;
    pool["max_lifetime"]    = 9999;
    pool["maxpintime"]      = 9999;
    pool["fss_policy"]      = std::string("maxfreespace");
    pool["gc_policy"]       = std::string("lru");
    pool["mig_policy"]      = std::string("none");
    pool["rs_policy"]       = std::string("fifo");
    pool["ret_policy"]      = std::string("R");
    pool["s_type"]          = std::string("-");
    pool["groups"]          = gids;
    
    poolManager->newPool(pool);
    
    // Get it back, and make sure it is the same
    dmlite::Pool poolGet = poolManager->getPool("test_fs");
    
    CPPUNIT_ASSERT_EQUAL(pool, poolGet);
    
    // That is not good enough. We also want to check if DPM saw it
    struct dpm_pool* dpmPools;
    int    nPools;
    bool   found;
    CPPUNIT_ASSERT_EQUAL(0, dpm_getpools(&nPools, &dpmPools));

    found = false;
    for (int i = 0; i < nPools; ++i) {
      if (pool.name == dpmPools[i].poolname) {
        found = true;
        
        CPPUNIT_ASSERT_EQUAL((u_signed64)102400, dpmPools[i].defsize);
        CPPUNIT_ASSERT_EQUAL(0,      dpmPools[i].gc_start_thresh);
        CPPUNIT_ASSERT_EQUAL(0,      dpmPools[i].gc_stop_thresh);
        CPPUNIT_ASSERT_EQUAL(1234,   dpmPools[i].def_lifetime);
        CPPUNIT_ASSERT_EQUAL(4321,   dpmPools[i].defpintime);
        CPPUNIT_ASSERT_EQUAL(9999,   dpmPools[i].max_lifetime);
        CPPUNIT_ASSERT_EQUAL(9999,   dpmPools[i].maxpintime);
        CPPUNIT_ASSERT_EQUAL('R',    dpmPools[i].ret_policy);
        CPPUNIT_ASSERT_EQUAL('-',    dpmPools[i].s_type);
        
        CPPUNIT_ASSERT_EQUAL(std::string("maxfreespace"),
                             std::string(dpmPools[i].fss_policy));
        CPPUNIT_ASSERT_EQUAL(std::string("lru"),
                             std::string(dpmPools[i].gc_policy));
        CPPUNIT_ASSERT_EQUAL(std::string("none"),
                             std::string(dpmPools[i].mig_policy));
        CPPUNIT_ASSERT_EQUAL(std::string("fifo"),
                             std::string(dpmPools[i].rs_policy));
        
        CPPUNIT_ASSERT_EQUAL(2, dpmPools[i].nbgids);
        CPPUNIT_ASSERT_EQUAL(101u, dpmPools[i].gids[0]);
        CPPUNIT_ASSERT_EQUAL(105u, dpmPools[i].gids[1]);
      }
      
      free(dpmPools[i].gids);
    }
    free(dpmPools);
    
    CPPUNIT_ASSERT_EQUAL(true, found);
    
    poolManager->deletePool(pool);    
    CPPUNIT_ASSERT_THROW(poolManager->getPool("test_fs"), dmlite::DmException);
  }
  
  void testAddFilesystemWithFs()
  {
    dmlite::Pool pool;
    std::vector<boost::any> gids;
    boost::any gid;
    
    gid = 101u;
    gids.push_back(gid);
    gid = 105u;
    gids.push_back(gid);
    
    // Initialize
    pool.name = "test_fs";
    pool.type = "filesystem";
    
    pool["defsize"]         = 102400;
    pool["gc_start_thresh"] = 0;
    pool["gc_stop_thresh"]  = 0;
    pool["def_lifetime"]    = 1234;
    pool["defpintime"]      = 4321;
    pool["max_lifetime"]    = 9999;
    pool["maxpintime"]      = 9999;
    pool["fss_policy"]      = std::string("maxfreespace");
    pool["gc_policy"]       = std::string("lru");
    pool["mig_policy"]      = std::string("none");
    pool["rs_policy"]       = std::string("fifo");
    pool["ret_policy"]      = std::string("R");
    pool["s_type"]          = std::string("-");
    pool["groups"]          = gids;
    
    // Filesystem
    dmlite::Extensible fs;
    
    fs["server"] = std::string("localhost.cern.ch");
    fs["fs"]     = std::string("/tmp");
    fs["status"] = FS_DISABLED;
    fs["weight"] = 10;
    
    std::vector<boost::any> fss;
    fss.push_back(fs);
    pool["filesystems"] = fss;
    
    // Add
    poolManager->newPool(pool);
    
    // Make sure the fs was added
    struct dpm_fs* dpmFs;
    int nFs;
    
    char poolname[16];
    strcpy(poolname, "test_fs");
    CPPUNIT_ASSERT_EQUAL(0, dpm_getpoolfs("test_fs", &nFs, &dpmFs));
    CPPUNIT_ASSERT_EQUAL(1, nFs);
    
    CPPUNIT_ASSERT_EQUAL(std::string("localhost.cern.ch"),
                         std::string(dpmFs[0].server));
    CPPUNIT_ASSERT_EQUAL(std::string("/tmp"),
                         std::string(dpmFs[0].fs));
    CPPUNIT_ASSERT_EQUAL(FS_DISABLED, dpmFs[0].status);
    CPPUNIT_ASSERT_EQUAL(10, dpmFs[0].weight);
    
    free(dpmFs);
  }
  
  CPPUNIT_TEST_SUITE(TestPools);
  CPPUNIT_TEST(testBase);
  CPPUNIT_TEST(testUnknown);
  CPPUNIT_TEST(testAddHadoop);
  CPPUNIT_TEST(testAddFilesystem);
  CPPUNIT_TEST(testAddFilesystemWithFs);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestPools);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
