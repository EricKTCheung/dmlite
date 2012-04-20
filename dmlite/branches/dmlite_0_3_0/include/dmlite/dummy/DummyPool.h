/// @file    include/dmlite/dummy/DummyPool.h
/// @brief   A dummy plugin that just delegates calls to a decorated one.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_DUMMYPOOL_H
#define	DMLITE_DUMMYPOOL_H

#include <dmlite/dmlite++.h>

namespace dmlite {

class DummyPoolManager: public PoolManager {
public:
  virtual ~DummyPoolManager();

  virtual void setSecurityContext(const SecurityContext*) throw (DmException);
  
  virtual PoolMetadata* getPoolMetadata(const Pool&) throw (DmException);

  virtual std::vector<Pool> getPools(void) throw (DmException);
  
  virtual Pool getPool(const std::string&) throw (DmException);

protected:
  PoolManager* decorated_;
private:
};

};

#endif	// DMLITE_DUMMYPOOL_H
