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

  void setSecurityCredentials(const SecurityCredentials&) throw (DmException);
  const SecurityContext& getSecurityContext() throw (DmException);
  void setSecurityContext(const SecurityContext&);

  std::vector<Pool>       getPools          (void)               throw (DmException);
  std::vector<FileSystem> getPoolFilesystems(const std::string&) throw (DmException);
  FileSystem              getFilesystem      (const std::string&, const std::string&, const std::string&) throw (DmException);

  void setUserId  (uid_t, gid_t, const std::string&) throw (DmException);
  void setVomsData(const std::string&, const std::vector<std::string>&) throw (DmException);

protected:
  PoolManager* decorated_;
  char*        decoratedId_;
private:
};

};

#endif	// PROFILERPOOLMANAGER_H

