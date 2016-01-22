
#include "TestDomeTaskExec.h"


int main () {
	TestDomeTaskExec *core = new TestDomeTaskExec();
        core->instance = "test";
	int key = core->submitCmd("/bin/ls");
        
	while (true) 
	{
		core->tick();
		sleep(5);
		core->waitResult(key);
	}
}
