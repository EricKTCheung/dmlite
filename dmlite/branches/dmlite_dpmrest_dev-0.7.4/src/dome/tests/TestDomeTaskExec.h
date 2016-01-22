#include "DomeTaskExec.h"
#include "DomeLog.h"

class TestDomeTaskExec: public DomeTaskExec {

public:

  TestDomeTaskExec() {
      const char *fname = "TestDomeTaskExec::ctor";
      domelogmask = Logger::get()->getMask(domelogname);

  }
  

};
