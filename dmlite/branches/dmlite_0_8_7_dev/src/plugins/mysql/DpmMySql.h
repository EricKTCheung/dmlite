/// @file   DpmMySql.h
/// @brief  MySQL DPM Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DPMMYSQL_H
#define	DPMMYSQL_H

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "NsMySql.h"
#include <dmlite/cpp/poolmanager.h>

namespace dmlite {
  
  const int kPoolNameMax = 15;
  const int kDefSizeMax = 15;
  const int kGc_Start_ThreshMax = 11;
  const int kGc_Stop_ThreshMax = 11;
  const int kDef_LifetimeMax = 11;
  const int kDefPintimeMax = 11;
  const int kMax_LifetimeMax = 11;
  const int kMaxPintimeMax = 11;
  const int kFss_PolicyMax = 15;
  const int kGc_PolicyMax = 15;
  const int kMig_PolicyMax = 15;
  const int kRs_PolicyMax = 15;
  const int kGroupsMax = 255;
  const int kRet_PolicyMax = 1;
  const int kS_TypeMax = 1;
  const int kPoolTypeMax = 32;
  const int kPoolMetaMax = 1024;

  class poolinfo {
  public:
    std::vector<Pool> pools;
    time_t pool_lastupd;
    poolinfo() {
        pool_lastupd = 0;
    }
  };

  class DpmMySqlFactory;

  /// Pool manager implementation.
  class MySqlPoolManager: public PoolManager {
   public:
    MySqlPoolManager(DpmMySqlFactory* factory, const std::string& dpmDb,
                     const std::string& adminUsername) throw (DmException);
    ~MySqlPoolManager();

    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext*) throw (DmException);

    std::vector<Pool> getPools(PoolAvailability availability) throw (DmException);
    Pool getPool(const std::string& poolname) throw (DmException);
    
    void newPool(const Pool& pool) throw (DmException);
    void updatePool(const Pool& pool) throw (DmException);
    void deletePool(const Pool& pool) throw (DmException);

    Location whereToRead (const std::string& path) throw (DmException);
    Location whereToRead (ino_t inode)             throw (DmException);
    Location whereToWrite(const std::string& path) throw (DmException);

    void cancelWrite(const Location& loc) throw (DmException);

   protected:
     Location whereToRead(const std::vector<Replica>& replicas) throw (DmException);

     std::vector<Pool> filterPools(std::vector<Pool>& pools, PoolAvailability availability) throw (DmException);
     std::vector<Pool> getPoolsFromMySql() throw (DmException);

   private:
    /// Plugin stack.
    StackInstance* stack_;

    /// DPM DB.
    std::string dpmDb_;

    /// The corresponding factory.
    DpmMySqlFactory* factory_;

    /// Security context
    const SecurityContext* secCtx_;

    /// Admin username for replication.
    const std::string adminUsername_;

    /// Cache of the pools.
    static poolinfo pools_;
    static boost::shared_mutex poolmtx_;
  };

};

#endif	// DPMMYSQL_H

