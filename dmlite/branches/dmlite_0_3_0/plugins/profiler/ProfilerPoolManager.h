/// @file    plugins/profiler/ProfilerPoolManager.h
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILERPOOLMANAGER_H
#define	PROFILERPOOLMANAGER_H

#include <dmlite/dm_pool.h>

namespace dmlite {

class ProfilerPoolManager: public PoolManager {
public:
  ProfilerPoolManager(PoolManager* decorated) throw (DmException);
  ~ProfilerPoolManager();

  std::string getImplId(void) throw();

  void setSecurityContext(const SecurityContext*) throw (DmException);

  PoolMetadata* getPoolMetadata(const Pool&) throw (DmException);
  
  std::vector<Pool> getPools(void) throw (DmException);
  Pool getPool(const std::string& poolname) throw (DmException);

protected:
  PoolManager* decorated_;
  char*        decoratedId_;
private:
};

};

#endif	// PROFILERPOOLMANAGER_H

