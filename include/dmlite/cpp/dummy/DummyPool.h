/// @file    include/dmlite/dummy/DummyPool.h
/// @brief   A dummy plugin that just delegates calls to a decorated one.
/// @details It makes sense as a base for other decorator plug-ins.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_DUMMYPOOL_H
#define	DMLITE_DUMMYPOOL_H

#include "../dmlite.h"

namespace dmlite {

class DummyPoolManager: public PoolManager {
public:
  virtual ~DummyPoolManager();

  virtual void setStackInstance(StackInstance*) throw (DmException);
  virtual void setSecurityContext(const SecurityContext*) throw (DmException);
  
  virtual PoolMetadata* getPoolMetadata(const std::string&) throw (DmException);

  virtual std::vector<Pool> getPools(PoolAvailability availability) throw (DmException);
  
  virtual Pool getPool(const std::string&) throw (DmException);
  
  
  virtual Location whereToRead (const std::string& path) throw (DmException);
  virtual Location whereToWrite(const std::string& path) throw (DmException);
  virtual void     doneWriting (const std::string& host, const std::string& rfn,
                                const std::map<std::string, std::string>& params) throw (DmException);

protected:
  PoolManager* decorated_;
private:
};

};

#endif	// DMLITE_DUMMYPOOL_H
