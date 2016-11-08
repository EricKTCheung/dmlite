#include "TestDomeTaskExec.h"
#include <iostream>
#include <cppunit/TestFixture.h>
#include <cppunit/TestAssert.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>


using namespace std;

class DomeTaskTest: public CppUnit::TestFixture {
private: 
    TestDomeTaskExec core ;
public:
  void setUp()
  {
 	 core.instance = "test";
  }
  
  void testSubmitCmd() {
    std::cout << "Test testSubmitCmd" << std::endl;
    int key = core.submitCmd("/bin/echo Andrea");
    core.waitResult(key);
    CPPUNIT_ASSERT(core.getTask(key)->finished == 1);
  }


  void testKill() {
    std::cout << "Test testKill" << std::endl;
    int key = core.submitCmd("/bin/sleep 100");

    while (true) { 
	   core.tick();
           sleep(5);
           core.killTask(key);
	   break;
    }
    core.waitResult(key);
    CPPUNIT_ASSERT(core.getTask(key)->finished == 1);   
  }

  void testParallel() {
    std::cout << "Test Parallel" << std::endl;
    int i = 0;
    int key[100];
    for (i=0; i< 100;i++) {
    	key[i] = core.submitCmd("/bin/sleep 100");
     }
    sleep(5);
    for (i=0; i< 100;i++) {
        core.killTask(key[i]);
    	core.waitResult(key[i]);
    }   
    for (i=0; i< 100;i++) {
    	CPPUNIT_ASSERT(core.getTask(key[i])->finished == 1);
    } 
   
  }

  void testParallelVectorArgs() {
    std::cout << "Test Parallel Vector Args" << std::endl;
    std::vector<std::string> args;
    args.push_back("/bin/sleep");
    args.push_back("100");
    int i = 0;
    int key[100];
    for (i=0; i< 100;i++) {
        key[i] = core.submitCmd(args);
     }
    sleep(5);
    for (i=0; i< 100;i++) {
        core.killTask(key[i]);
        core.waitResult(key[i]);
    }
    for (i=0; i< 100;i++) {
        CPPUNIT_ASSERT(core.getTask(key[i])->finished == 1);
    }

  }

  CPPUNIT_TEST_SUITE(DomeTaskTest);
  CPPUNIT_TEST(testSubmitCmd);
  CPPUNIT_TEST(testKill);
  CPPUNIT_TEST(testParallel);
  CPPUNIT_TEST(testParallelVectorArgs);
  CPPUNIT_TEST_SUITE_END();

};

CPPUNIT_TEST_SUITE_REGISTRATION(DomeTaskTest);

int main (int argc, char **argv){
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  return runner.run()?0:1;

} 
