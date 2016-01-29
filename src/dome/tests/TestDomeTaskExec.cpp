#include <iostream>
#include "TestDomeTaskExec.h"

void testSubmitCmd() {
    std::cout << "Test testSubmitCmd" << std::endl;
    TestDomeTaskExec *core = new TestDomeTaskExec();
    core->instance = "test";
    int key = core->submitCmd("/bin/sleep 5");
    core->waitResult(key);
}


void testKill() {
    std::cout << "Test testKill" << std::endl;
    TestDomeTaskExec *core = new TestDomeTaskExec();
    core->instance = "test";
    int key = core->submitCmd("/bin/sleep 100");

    while (true) { 
	   core->tick();
           sleep(5);
           core->killTask(key);
	   break;
    }
}

int main () {
  testSubmitCmd();
  //testKill();
}
