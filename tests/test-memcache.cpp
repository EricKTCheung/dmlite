#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestAssert.h>
#include <sys/stat.h>
#include "test-base.h"
#include <sys/time.h>

#define REPEATS 1000
#define NUM_REPLICAS 200
#define NESTS 5 

class TestMemcache: public TestBase
{
protected:
  struct stat statBuf, statBufCached, iStat;
  const static int   MODE;
  const static char *FOLDER;
  const static char *NESTED;
  const static char *SYMLINK;
  const static char *RELATIVE;
  const static char *SYMREL;
  const static char *FILE;
  const static char *NUMFILE;

public:

  void setUp()
  {
    TestBase::setUp();

    this->catalog->makeDir(FOLDER, MODE);
    this->catalog->makeDir(NESTED, MODE);

    this->catalog->symlink(FOLDER, SYMLINK);
    this->catalog->symlink(RELATIVE, SYMREL);
    this->catalog->create(FILE, MODE);

    char currentFile[50];
    int repeats = TestBase::repeats;

    int i;
    for (i = 0; i < repeats; i++)
    {
      sprintf(currentFile, "%s%d", NUMFILE, i);
      this->catalog->create(std::string(currentFile), MODE);
    }
    
  }

  void tearDown()
  {
    this->catalog->changeDir(BASE_DIR);
    if (this->catalog != 0x00) {
//      setSecurityContext does not work anymore
//      this->catalog->setSecurityContext(root);
      
      try {
        struct stat s = this->catalog->stat(FILE);
        std::vector<FileReplica> replicas = this->catalog->getReplicas(FILE);
        for (unsigned i = 0; i < replicas.size(); ++i) {
          this->catalog->deleteReplica("", s.st_ino, replicas[i].url);
        }
      }
      catch (dmlite::DmException e) {
        switch (e.code()) {
          case DM_NO_SUCH_FILE: case DM_NO_REPLICAS:
            break;
          default:
            throw;
        }
      }
      
      char currentFile[50];
      int repeats = TestBase::repeats;
      int i;
      for (i = 0; i < repeats; i++)
      {
        sprintf(currentFile, "%s%d", NUMFILE, i);
        IGNORE_NOT_EXIST(this->catalog->unlink(std::string(currentFile)));
      }
      IGNORE_NOT_EXIST(this->catalog->unlink(SYMLINK));
      IGNORE_NOT_EXIST(this->catalog->unlink(SYMREL));
      IGNORE_NOT_EXIST(this->catalog->unlink(FILE));
      IGNORE_NOT_EXIST(this->catalog->removeDir(NESTED));
      IGNORE_NOT_EXIST(this->catalog->removeDir(FOLDER));
      
    }
    TestBase::tearDown();
  }

  void testStat()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    
	  gettimeofday(&start, NULL);
    statBuf = this->catalog->stat(FOLDER);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("stat:%f\n", stime);

	  gettimeofday(&start, NULL);
    statBuf = this->catalog->stat(FOLDER);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("cached stat:%f\n", stime);
    statBuf = this->catalog->stat(FOLDER);

  }

  void testRepeatedStat()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc = 0.0;
    int repeats = REPEATS; 
    
    // cache the entry
    statBuf = this->catalog->stat(FOLDER);

    for (int i = 0; i < repeats; i++)
    {
	    gettimeofday(&start, NULL);
      statBuf = this->catalog->stat(FOLDER);
      gettimeofday(&end, NULL);

      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;

  	  microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;

      time_acc += stime;
    }

	  printf("cached stat avg:%f\n", time_acc / repeats);
  }

  void testDeepStat()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    int nested = NESTS;
    std::string nested_cwd;
    std::string nested_stat;

    nested_cwd += std::string(NESTED);
    for (int i = 0; i < nested; i++)
    {
      nested_cwd += "/";
      nested_cwd += "nested";
      this->catalog->makeDir(nested_cwd, MODE);
    }

    nested_stat = nested_cwd;
    nested_stat += "/";
    nested_stat += std::string(FOLDER);

    this->catalog->makeDir(nested_stat, MODE);

	  gettimeofday(&start, NULL);
    statBuf = this->catalog->stat(nested_stat);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("deep stat:%f\n", stime);

	  gettimeofday(&start, NULL);
    statBuf = this->catalog->stat(nested_stat);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("cached deep stat:%f\n", stime);

    this->catalog->removeDir(nested_stat);
    for (int i = 0; i < nested; i++)
    {
      nested_cwd = nested_cwd.substr(0, nested_cwd.length()-7);
      this->catalog->changeDir(BASE_DIR);
      this->catalog->changeDir(nested_cwd);
      IGNORE_NOT_EXIST(this->catalog->removeDir("nested"));
    }
  }

  void testRepeatedDeepStat()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc = 0.0;
    int repeats = REPEATS; 
    int nested = NESTS;
    std::string nested_cwd;
    std::string nested_stat;

    nested_cwd += std::string(NESTED);
    for (int i = 0; i < nested; i++)
    {
      nested_cwd += "/";
      nested_cwd += "nested";
      this->catalog->makeDir(nested_cwd, MODE);
    }

    nested_stat = nested_cwd;
    nested_stat += "/";
    nested_stat += std::string(FOLDER);

    this->catalog->makeDir(nested_stat, MODE);
    statBuf = this->catalog->stat(nested_stat);

    for (int i = 0; i < repeats; i++)
    {
	    gettimeofday(&start, NULL);
      statBuf = this->catalog->stat(nested_stat);
      gettimeofday(&end, NULL);

      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;

  	  microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;

      time_acc += stime;
    }
	  printf("cached deep stat avg:%f\n", time_acc / repeats);

    this->catalog->removeDir(nested_stat);
    for (int i = 0; i < nested; i++)
    {
      nested_cwd = nested_cwd.substr(0, nested_cwd.length()-7);
      this->catalog->changeDir(BASE_DIR);
      this->catalog->changeDir(nested_cwd);
      IGNORE_NOT_EXIST(this->catalog->removeDir("nested"));
    }
  }

  void testGetReplica()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    FileReplica replica; 
    int num_replicas = NUM_REPLICAS;
    struct stat s;
    char replica_name[50]; 
    std::string this_replica_name;

    s = this->catalog->stat(FILE);
    for (int i = 0; i < num_replicas; i++)
    {
      sprintf(replica_name, "%s%d", "http://a.host.com/replica", i);
      this_replica_name = std::string(replica_name);
//      printf("%s\n", this_replica_name.c_str());
      this->catalog->addReplica(std::string(), s.st_ino, "b.host.com",
                                this_replica_name, '-', 'P',
                                "the-pool", "the-fs");
    }
    
	  gettimeofday(&start, NULL);
    replica = this->catalog->getReplicas(FILE)[0];
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("replica:%f\n", stime);

	  gettimeofday(&start, NULL);
    replica = this->catalog->getReplicas(FILE)[0];
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("cached replica:%f\n", stime);
  }

  void testRepeatedGetReplica()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc;
    FileReplica replica; 
    int repeats = REPEATS; 
    int num_replicas = NUM_REPLICAS;
    struct stat s;
    char replica_name[50]; 
    std::string this_replica_name;

    s = this->catalog->stat(FILE);
    for (int i = 0; i < num_replicas; i++)
    {
      sprintf(replica_name, "%s%d", "http://a.host.com/replica", i);
      this_replica_name = std::string(replica_name);
      this->catalog->addReplica(std::string(), s.st_ino, "b.host.com",
                                this_replica_name, '-', 'P',
                                "the-pool", "the-fs");
    }
    
    // cache the entry
   
    replica = this->catalog->getReplicas(FILE)[0];
 
    for (int i = 0; i < repeats; i++)
    {
	    gettimeofday(&start, NULL);
      replica = this->catalog->getReplicas(FILE)[0];
      gettimeofday(&end, NULL);

      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;

  	  microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;

      time_acc += stime;
    }

	  printf("cached replica avg:%f\n", time_acc / repeats);
  }

  void testGetReplicas()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    std::vector<FileReplica> replica;
    struct stat s;
    int num_replicas = NUM_REPLICAS;
    char replica_name[50]; 
    std::string this_replica_name;

    s = this->catalog->stat(FILE);
    for (int i = 0; i < num_replicas; i++)
    {
      sprintf(replica_name, "%s%d", "http://a.host.com/replica", i);
      this_replica_name = std::string(replica_name);
      this->catalog->addReplica(std::string(), s.st_ino, "b.host.com",
                                this_replica_name, '-', 'P',
                                "the-pool", "the-fs");
    }
    
	  gettimeofday(&start, NULL);
    replica = this->catalog->getReplicas(FILE);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("replicas:%f\n", stime);

	  gettimeofday(&start, NULL);
    replica = this->catalog->getReplicas(FILE);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("cached replicas:%f\n", stime);
  }

  void testRepeatedGetReplicas()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc;
    int repeats = REPEATS; 
    std::vector<FileReplica> replica;
    struct stat s;
    int num_replicas = NUM_REPLICAS;
    char replica_name[50]; 
    std::string this_replica_name;

    s = this->catalog->stat(FILE);
    for (int i = 0; i < num_replicas; i++)
    {
      sprintf(replica_name, "%s%d", "http://a.host.com/replica", i);
      this_replica_name = std::string(replica_name);
      this->catalog->addReplica(std::string(), s.st_ino, "b.host.com",
                                this_replica_name, '-', 'P',
                                "the-pool", "the-fs");
    }
    
    // cache the entry
   
    replica = this->catalog->getReplicas(FILE);
 
    for (int i = 0; i < repeats; i++)
    {
	    gettimeofday(&start, NULL);
      replica = this->catalog->getReplicas(FILE);
      gettimeofday(&end, NULL);

      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;

  	  microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;

      time_acc += stime;
    }

	  printf("cached replicas avg:%f\n", time_acc / repeats);
  }

  void testCreateRemoveDir()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc;
    char nested_folder[50]; 
    std::string nsf;

    this->catalog->changeDir(FOLDER);
    sprintf(nested_folder, "%s%d", "test", 1); 
    nsf = std::string(nested_folder);

	  gettimeofday(&start, NULL);
    this->catalog->makeDir(nsf, MODE);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("create dir:%f\n", stime);
    // cache the entry
    this->catalog->stat(nsf);

	  gettimeofday(&start, NULL);
    this->catalog->removeDir(nsf);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("remove dir:%f\n", stime);
  }

  void testRepeatedCreateRemoveDir()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float ctime_acc;
    float rtime_acc;
    char nested_folder[50]; 
    std::string nsf;
    int repeats = REPEATS; 

    this->catalog->changeDir(FOLDER);
    for (int i = 0; i < repeats; i++)
    {
      sprintf(nested_folder, "%s%d", "test", i); 
      nsf = std::string(nested_folder);
  
  	  gettimeofday(&start, NULL);
      this->catalog->makeDir(nsf, MODE);
      gettimeofday(&end, NULL);
  
      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;
  
    	microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;
      ctime_acc += stime;
  
      // cache the entry
      this->catalog->stat(nsf);
  
  	  gettimeofday(&start, NULL);
      this->catalog->removeDir(nsf);
      gettimeofday(&end, NULL);
  
      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;
  
    	microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;
      rtime_acc += stime;
    }
	  printf("create dir avg:%f\n", ctime_acc / repeats);
	  printf("remove dir avg:%f\n", rtime_acc / repeats);
  }


  void testCreateRemoveFile()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float time_acc;
    char nested_folder[50]; 
    std::string nsf;

    this->catalog->changeDir(FOLDER);
    sprintf(nested_folder, "%s%d", "test", 1); 
    nsf = std::string(nested_folder);

	  gettimeofday(&start, NULL);
    this->catalog->create(nsf, MODE);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("create file:%f\n", stime);
    // cache the entry
    this->catalog->stat(nsf);

	  gettimeofday(&start, NULL);
    this->catalog->unlink(nsf);
    gettimeofday(&end, NULL);

    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;

  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("unlink file:%f\n", stime);
  }

  void testRepeatedCreateRemoveFile()
  {
	  struct timeval start, end;
    long seconds, useconds;
  	double microtime;
    float stime;
    float ctime_acc;
    float rtime_acc;
    char nested_folder[50]; 
    std::string nsf;
    int repeats = REPEATS; 

    this->catalog->changeDir(FOLDER);
    for (int i = 0; i < repeats; i++)
    {
      sprintf(nested_folder, "%s%d", "test", i); 
      nsf = std::string(nested_folder);
  
  	  gettimeofday(&start, NULL);
      this->catalog->create(nsf, MODE);
      gettimeofday(&end, NULL);
  
      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;
  
    	microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;
      ctime_acc += stime;
  
      // cache the entry
      this->catalog->stat(nsf);
  
  	  gettimeofday(&start, NULL);
      this->catalog->unlink(nsf);
      gettimeofday(&end, NULL);
  
      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;
  
    	microtime = seconds * 1000000 + useconds;
      stime = microtime / 1000000;
      rtime_acc += stime;
    }
	  printf("create file avg:%f\n", ctime_acc / repeats);
	  printf("remove file avg:%f\n", rtime_acc / repeats);
  }

  void testOpenAndReadLargeDirFromDisk()
  {
	  struct timeval start, end, singleStart, singleEnd;
    long seconds, useconds;
  	double microtime;
    float stime, stime2;
    int repeats = TestBase::repeats;

    struct stat before, after;

    before = this->catalog->stat(FOLDER);

    int num_files = 0;
    int num_cached_files = 0;
    
    sleep(1);
    dmlite::Directory* d;
    dirent *entry;
  	gettimeofday(&start, NULL);
    d = this->catalog->openDir(FOLDER);
    entry  = this->catalog->readDir(d);
    this->catalog->closeDir(d);
    gettimeofday(&end, NULL);
  
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
  
  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("read dir with %d:%f\n", repeats, stime);

    sleep(1);
  	gettimeofday(&start, NULL);
    d = this->catalog->openDir(FOLDER);
    entry = this->catalog->readDir(d); 
    while (entry)
    {
      entry = this->catalog->readDir(d); 
      num_cached_files++;
//  	  gettimeofday(&singleEnd, NULL);
    
//      seconds  = singleEnd.tv_sec  - singleStart.tv_sec;
//      useconds = singleEnd.tv_usec - singleStart.tv_usec;
  
//  	  microtime = seconds * 1000000 + useconds;
//      stime = microtime / 1000000;

//	    printf("read one cached file:%f\n", stime);
    }
    this->catalog->closeDir(d);
    gettimeofday(&end, NULL);
  
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
  
  	microtime = seconds * 1000000 + useconds;
    stime2 = microtime / 1000000;

	  printf("cached read dir with %d:%f\n", repeats, stime2);
    printf("%f,%f\n", stime, stime2);
  }

  void testOpenAndReadLargeDir()
  {
	  struct timeval start, end, singleStart, singleEnd;
    long seconds, useconds;
  	double microtime;
    float stime, stime2, stime3;
    int repeats = TestBase::repeats;

    struct stat before, after;
    /*
    char currentFile[50];

    int i;
    for (i = 0; i < REPEATS; i++)
    {
      sprintf(currentFile, "%s%d", NUMFILE, i);
      this->catalog->create(std::string(currentFile), MODE);
    }
    */
    before = this->catalog->stat(FOLDER);

    int num_files = 0;
    int num_cached_files = 0;
    
    sleep(1);
    dmlite::Directory* d;
    dirent *entry;
  	gettimeofday(&start, NULL);
    d = this->catalog->openDir(FOLDER);
    entry  = this->catalog->readDir(d);
    while (entry)
    {
//  	  gettimeofday(&singleStart, NULL);
      entry  = this->catalog->readDir(d);
//  	  gettimeofday(&singleEnd, NULL);
    
//      seconds  = singleEnd.tv_sec  - singleStart.tv_sec;
//      useconds = singleEnd.tv_usec - singleStart.tv_usec;
  
//  	  microtime = seconds * 1000000 + useconds;
//      stime = microtime / 1000000;

//	    printf("read one file:%f\n", stime);

      num_files++;
    }
    this->catalog->closeDir(d);
    gettimeofday(&end, NULL);
  
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
  
  	microtime = seconds * 1000000 + useconds;
    stime = microtime / 1000000;

	  printf("read dir with %d:%f\n", repeats, stime);

    sleep(1);
  	gettimeofday(&start, NULL);
    d = this->catalog->openDir(FOLDER);
    entry = this->catalog->readDir(d); 
    while (entry)
    {
//  	  gettimeofday(&singleStart, NULL);
      entry = this->catalog->readDir(d); 
      num_cached_files++;
//  	  gettimeofday(&singleEnd, NULL);
    
//      seconds  = singleEnd.tv_sec  - singleStart.tv_sec;
//      useconds = singleEnd.tv_usec - singleStart.tv_usec;
  
//  	  microtime = seconds * 1000000 + useconds;
//      stime = microtime / 1000000;

//	    printf("read one cached file:%f\n", stime);
    }
    this->catalog->closeDir(d);
    gettimeofday(&end, NULL);
  
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
  
  	microtime = seconds * 1000000 + useconds;
    stime2 = microtime / 1000000;

    CPPUNIT_ASSERT_EQUAL(num_files, num_cached_files);

    sleep(1);
  	gettimeofday(&start, NULL);
    d = this->catalog->openDir(FOLDER);
    entry = this->catalog->readDir(d); 
    this->catalog->closeDir(d);
    gettimeofday(&end, NULL);
  
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
  
  	microtime = seconds * 1000000 + useconds;
    stime3 = microtime / 1000000;

	  printf("cached read dir with %d:%f\n", repeats, stime2);
    printf("cached read dir with %d first file:%f\n", repeats, stime3);
    printf("%f,%f,%f\n", stime, stime2, stime3);
    /*
    for (i = 0; i < REPEATS; i++)
    {
      sprintf(currentFile, "%s%d", NUMFILE, i);
      this->catalog->unlink(std::string(currentFile));
    }
    */
  }


  CPPUNIT_TEST_SUITE(TestMemcache);
//*
  CPPUNIT_TEST(testStat);
  CPPUNIT_TEST(testRepeatedStat);
  CPPUNIT_TEST(testDeepStat);
  CPPUNIT_TEST(testRepeatedDeepStat);
  CPPUNIT_TEST(testGetReplica);
  CPPUNIT_TEST(testRepeatedGetReplica);
  CPPUNIT_TEST(testGetReplicas);
  CPPUNIT_TEST(testRepeatedGetReplicas);
  CPPUNIT_TEST(testCreateRemoveDir);
  CPPUNIT_TEST(testRepeatedCreateRemoveDir);
  CPPUNIT_TEST(testCreateRemoveFile);
  CPPUNIT_TEST(testRepeatedCreateRemoveFile);
//*/ 
  CPPUNIT_TEST(testOpenAndReadLargeDirFromDisk);
  CPPUNIT_TEST(testOpenAndReadLargeDir);
  
  CPPUNIT_TEST_SUITE_END();
};

const int   TestMemcache::MODE        = 0777;
const char* TestMemcache::FOLDER      = "test-stat";
const char* TestMemcache::NESTED      = "test-stat/nested";
const char* TestMemcache::SYMLINK     = "test-link";
const char* TestMemcache::RELATIVE    = "test-stat/../test-stat/nested";
const char* TestMemcache::SYMREL      = "test-link-rel";
const char* TestMemcache::FILE        = "test-stat/file";
const char* TestMemcache::NUMFILE     = "test-stat/file_";

CPPUNIT_TEST_SUITE_REGISTRATION(TestMemcache);

int main(int argn, char **argv)
{
  return testBaseMain(argn, argv);
}
