#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <cstring>
#include <sys/stat.h>
#include "test-base.h"

class TestReplicas: public TestBase
{
protected:
  const static int   MODE;
  const static char *FOLDER;
  const static char *FILE;

public:

  void setUp()
  {
    TestBase::setUp();
    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->create(FILE, MODE);
  }

  void tearDown()
  {
    if (this->catalog != 0x00) {
      try {
        struct stat s = this->catalog->extendedStat(FILE).stat;
        std::vector<FileReplica> replicas = this->catalog->getReplicas(FILE);
        for (unsigned i = 0; i < replicas.size(); ++i) {
          this->catalog->deleteReplica(replicas[i]);
        }
      }
      catch (dmlite::DmException& e) {
        switch (e.code()) {
          case DM_NO_SUCH_FILE: case DM_NO_REPLICAS:
            break;
          default:
            throw;
        }
      }

      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
      IGNORE_NOT_EXIST(this->catalog->removeDir(FOLDER));
    }
    TestBase::tearDown();
  }

  void testAddAndRemove()
  {
    struct stat s;

    s = this->catalog->extendedStat(FILE).stat;
    
    FileReplica replica;
    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.server,     "b.host.com");
    strcpy(replica.rfn,        "http://a.host.com/replica");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';

    this->catalog->addReplica(replica);

    replica = this->catalog->getReplicas(FILE)[0];

    CPPUNIT_ASSERT_EQUAL((unsigned)s.st_ino, (unsigned)replica.fileid);
    CPPUNIT_ASSERT_EQUAL(std::string("http://a.host.com/replica"),
                         std::string(replica.rfn));
    CPPUNIT_ASSERT_EQUAL('-', replica.status);
    CPPUNIT_ASSERT_EQUAL(std::string("the-fs"), std::string(replica.filesystem));
    CPPUNIT_ASSERT_EQUAL(std::string("the-pool"), std::string(replica.pool));
    CPPUNIT_ASSERT_EQUAL(std::string("b.host.com"), std::string(replica.server));

    replica = this->catalog->getReplica("http://a.host.com/replica");
    
    this->catalog->deleteReplica(replica);

    CPPUNIT_ASSERT_THROW(this->catalog->getReplicas(FILE), dmlite::DmException);
  }

  void testAddNoHost()
  {
    struct stat s;

    s = this->catalog->extendedStat(FILE).stat;
    
    FileReplica replica;
    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.rfn,        "http://a.host.com/replica");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';

    this->catalog->addReplica(replica);

    replica = this->catalog->getReplicas(FILE)[0];

    CPPUNIT_ASSERT_EQUAL(std::string("http://a.host.com/replica"),
                         std::string(replica.rfn));
    CPPUNIT_ASSERT_EQUAL(std::string("a.host.com"),
                         std::string(replica.server));

    this->catalog->deleteReplica(replica);
  }

  void testModify()
  {
    struct stat s;

    s = this->catalog->extendedStat(FILE).stat;
    
    FileReplica replica;
    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.server,     "a.host.com");
    strcpy(replica.rfn,        "http://a.host.com/replica");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';

    this->catalog->addReplica(replica);

    replica = this->catalog->getReplica("https://a.host.com/replica");
    
    replica.ltime  = 12348;
    replica.status = 'D';
    replica.type   = 'V';
    
    this->catalog->updateReplica(replica);
    
    replica = this->catalog->getReplicas(FILE)[0];

    CPPUNIT_ASSERT_EQUAL(12348, (int)replica.ltime);
    CPPUNIT_ASSERT_EQUAL('D', replica.status);
    CPPUNIT_ASSERT_EQUAL('V', replica.type);
  }

  void testCachedEntries()
  {
    struct stat s;

    s = this->catalog->extendedStat(FILE).stat;
    
    FileReplica replica;
    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.server,     "b.host.com");
    strcpy(replica.rfn,        "http://a.host.com/replica");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';

    this->catalog->addReplica(replica);

    replica = this->catalog->getReplicas(FILE)[0];
    FileReplica replicaCached = this->catalog->getReplicas(FILE)[0];

    CPPUNIT_ASSERT_EQUAL((unsigned)replicaCached.fileid, (unsigned)replica.fileid);
    CPPUNIT_ASSERT_EQUAL(std::string(replicaCached.rfn),
                         std::string(replica.rfn));
    CPPUNIT_ASSERT_EQUAL(replicaCached.status, replica.status);
    CPPUNIT_ASSERT_EQUAL(std::string(replicaCached.filesystem), std::string(replica.filesystem));
    CPPUNIT_ASSERT_EQUAL(std::string(replicaCached.pool), std::string(replica.pool));
    CPPUNIT_ASSERT_EQUAL(std::string(replicaCached.server), std::string(replica.server));

    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.server,     "b.host.com");
    strcpy(replica.rfn,        "http://a.host.com/replica2");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';
    
    this->catalog->addReplica(replica);

    std::vector<FileReplica> replicaVector = this->catalog->getReplicas(FILE);

    CPPUNIT_ASSERT_EQUAL(2, (int) replicaVector.size());
  }

  void testCachedModify()
  {
    struct stat s;

    s = this->catalog->extendedStat(FILE).stat;
    
    FileReplica replica;
    memset(&replica, 0, sizeof(FileReplica));
    replica.fileid = s.st_ino;
    strcpy(replica.server,     "a.host.com");
    strcpy(replica.rfn,        "http://a.host.com/replica");
    strcpy(replica.pool,       "the-pool");
    strcpy(replica.filesystem, "the-fs");
    replica.status = '-';
    replica.type   = 'P';

    this->catalog->addReplica(replica);

    // value might be cached
    replica = this->catalog->getReplicas(FILE)[0];

    replica.ltime  = 12348;
    replica.status = 'D';
    replica.type   = 'V';
    
    this->catalog->updateReplica(replica);

    replica = this->catalog->getReplicas(FILE)[0];

    CPPUNIT_ASSERT_EQUAL(12348, (int)replica.ltime);
    CPPUNIT_ASSERT_EQUAL('D', replica.status);
    CPPUNIT_ASSERT_EQUAL('V', replica.type);
  }


  CPPUNIT_TEST_SUITE(TestReplicas);
  CPPUNIT_TEST(testAddAndRemove);
  CPPUNIT_TEST(testAddNoHost);
  CPPUNIT_TEST(testModify);
  CPPUNIT_TEST(testCachedEntries);
  CPPUNIT_TEST(testCachedModify);
  CPPUNIT_TEST_SUITE_END();
};

const int   TestReplicas::MODE   = 0700;
const char* TestReplicas::FOLDER = "test-replica";
const char* TestReplicas::FILE   = "test-replica/file";

CPPUNIT_TEST_SUITE_REGISTRATION(TestReplicas);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}

