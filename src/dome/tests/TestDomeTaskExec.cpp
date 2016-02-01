#include "TestDomeTaskExec.h"
#include <iostream>

using namespace std;

#define ASSERT(assertion)  if((assertion) == false) std::cout << ("\nAssertion Failed ")

void testSubmitCmd(TestDomeTaskExec& core) {
    std::cout << "Test testSubmitCmd" << std::endl;
    int key = core.submitCmd("/bin/sleep 5");
    core.waitResult(key);
    ASSERT(core.getTask(key)->finished == 1);
 }


void testKill(TestDomeTaskExec& core) {
    std::cout << "Test testKill" << std::endl;
    int key = core.submitCmd("/bin/sleep 100");

    while (true) { 
	   core.tick();
           sleep(5);
           core.killTask(key);
	   break;
    }
    core.waitResult(key);
    ASSERT(core.getTask(key)->finished == 1);   
}

void testParallel(TestDomeTaskExec& core) {
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
    	ASSERT(core.getTask(key[i])->finished == 1);
    } 
   
}


int main () {
  TestDomeTaskExec core ;
  core.instance = "test";
  testSubmitCmd(core);
  testKill(core);
  testParallel(core);
} 
