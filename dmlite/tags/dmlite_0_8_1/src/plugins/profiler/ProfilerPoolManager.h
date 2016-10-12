/// @file   ProfilerPoolManager.h
/// @brief  Profiler plugin.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILERPOOLMANAGER_H
#define	PROFILERPOOLMANAGER_H

#include "ProfilerXrdMon.h"

#include <dmlite/cpp/poolmanager.h>

namespace dmlite {

  class ProfilerPoolManager: public PoolManager, private ProfilerXrdMon {
   public:
    ProfilerPoolManager(PoolManager* decorated) throw (DmException);
    ~ProfilerPoolManager();

    std::string getImplId(void) const throw();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    std::vector<Pool> getPools(PoolAvailability) throw (DmException);
    Pool getPool(const std::string& poolname) throw (DmException);
    
    void newPool(const Pool& pool) throw (DmException);
    void updatePool(const Pool& pool) throw (DmException);
    void deletePool(const Pool& pool) throw (DmException);

    Location whereToRead (const std::string& path) throw (DmException);
    Location whereToRead (ino_t inode)             throw (DmException);
    Location whereToWrite(const std::string& path) throw (DmException);
    

   protected:
    PoolManager* decorated_;
    char*        decoratedId_;
  };

};

#endif	// PROFILERPOOLMANAGER_H
