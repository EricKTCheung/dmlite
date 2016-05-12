#include "DomeTaskExec.h"
#include "DomeLog.h"

class TestDomeTaskExec: public DomeTaskExec {

public:

  TestDomeTaskExec() {

      domelogmask = Logger::get()->getMask(domelogname);

  }
  

};
