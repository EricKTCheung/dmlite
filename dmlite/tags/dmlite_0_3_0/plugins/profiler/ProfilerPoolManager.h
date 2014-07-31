/// @file    plugins/profiler/ProfilerPoolManager.h
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILERPOOLMANAGER_H
#define	PROFILERPOOLMANAGER_H

#include <dmlite/cpp/dm_pool.h>

namespace dmlite {

class ProfilerPoolManager: public PoolManager {
public:
  ProfilerPoolManager(PoolManager* decorated) throw (DmException);
  ~ProfilerPoolManager();

  std::string getImplId(void) throw();

  void setStackInstance(StackInstance* si) throw (DmException);
  void setSecurityContext(const SecurityContext*) throw (DmException);

  PoolMetadata* getPoolMetadata(const std::string&) throw (DmException);
  
  std::vector<Pool> getPools(void) throw (DmException);
  Pool getPool(const std::string& poolname) throw (DmException);
  std::vector<Pool> getAvailablePools(bool) throw (DmException);

protected:
  PoolManager* decorated_;
  char*        decoratedId_;
private:
};

};

#endif	// PROFILERPOOLMANAGER_H
