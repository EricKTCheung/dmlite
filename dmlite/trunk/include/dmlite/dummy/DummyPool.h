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

  virtual std::string getImplId(void) throw();

  virtual void setSecurityCredentials(const SecurityCredentials&) throw (DmException);
  virtual const SecurityContext& getSecurityContext(void) throw (DmException);
  virtual void setSecurityContext(const SecurityContext&);

  virtual std::vector<Pool> getPools(void) throw (DmException);

protected:
  PoolManager* decorated_;
private:
};

};

#endif	// DMLITE_DUMMYPOOL_H
