/// @file   DpmMySql.cpp
/// @brief  MySQL DPM Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/urls.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include "DpmMySql.h"
#include "Queries.h"
#include "MySqlFactories.h"

#include "utils/logger.h"

using namespace dmlite;

poolinfo              MySqlPoolManager::pools_;
boost::shared_mutex   MySqlPoolManager::poolmtx_;

MySqlPoolManager::MySqlPoolManager(DpmMySqlFactory* factory,
                                   const std::string& dpmDb,
                                   const std::string& adminUsername) throw (DmException):
      stack_(0x00), dpmDb_(dpmDb), factory_(factory), adminUsername_(adminUsername)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Ctor");
  // Nothing
}



MySqlPoolManager::~MySqlPoolManager()
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " Dtor");
  // Nothing
}



std::string MySqlPoolManager::getImplId() const throw ()
{
  return "mysql_pool_manager";
}



void MySqlPoolManager::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack_ = si;
}



void MySqlPoolManager::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  this->secCtx_ = ctx;
}



std::vector<Pool> MySqlPoolManager::getPools(PoolAvailability availability) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "Poolavailability: " << availability);

  {
    boost::shared_lock<boost::shared_mutex> l(poolmtx_);
    time_t timenow = time(0);
    if (pools_.pool_lastupd <= timenow + 60 && pools_.pool_lastupd >= timenow - 60) {
      Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. npools:" << pools_.pools.size());
      return filterPools(pools_.pools, availability);
    }
  }

  std::vector<Pool> pools;
  {
    boost::unique_lock<boost::shared_mutex> l(poolmtx_);
    time_t timenow = time(0);
    if (pools_.pool_lastupd <= timenow + 60 && pools_.pool_lastupd >= timenow - 60) {
      // updated in the meantime and valid again
      pools = pools_.pools;
    } else {
      pools = getPoolsFromMySql();
      pools_.pools.assign(pools.begin(), pools.end());
      pools_.pool_lastupd = timenow;
    }
  }
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. npools:" << pools.size());
  return filterPools(pools, availability);
}



std::vector<Pool> MySqlPoolManager::filterPools(std::vector<Pool>& pools, PoolAvailability availability) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");
  if (availability == kAny) {
    Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. npools:" << pools.size());
    return pools;
  }
  else {
    std::vector<Pool> filtered;

    for (unsigned i = 0; i < pools.size(); ++i) {
      PoolHandler* handler = this->stack_->getPoolDriver(pools[i].type)->
                               createPoolHandler(pools[i].name);

      bool isAvailable = handler->poolIsAvailable(availability == kForWrite ||
                                                  availability == kForBoth);

      if ((availability == kNone && !isAvailable) ||
          (availability != kNone && isAvailable))
        filtered.push_back(pools[i]);

      delete handler;
    }

    Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. npools:" << pools.size());
    return filtered;
  }
}



std::vector<Pool> MySqlPoolManager::getPoolsFromMySql() throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "");

  Pool pool;
  std::vector<Pool> pools;
  char poolName[kPoolNameMax], defSize[kDefSizeMax], gc_Start_Thresh[kGc_Start_ThreshMax], gc_Stop_Thresh[kGc_Stop_ThreshMax], def_Lifetime[kDef_LifetimeMax], defPintime[kDefPintimeMax], max_Lifetime[kMax_LifetimeMax], maxPintime[kMaxPintimeMax], fss_Policy[kFss_PolicyMax], gc_Policy[kGc_PolicyMax], mig_Policy[kMig_PolicyMax], rs_Policy[kRs_PolicyMax], Groups[kGroupsMax], ret_Policy[kRet_PolicyMax], s_Type[kS_TypeMax], poolType[kPoolTypeMax], poolMeta[kPoolMetaMax];


  // Get all
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->dpmDb_, STMT_GET_POOLS);

  stmt.execute();

  stmt.bindResult(0, poolName, sizeof(poolName));
  stmt.bindResult(1, defSize, sizeof(defSize));
  stmt.bindResult(2, gc_Start_Thresh, sizeof(gc_Start_Thresh));
  stmt.bindResult(3, gc_Stop_Thresh, sizeof(gc_Stop_Thresh));
  stmt.bindResult(4, def_Lifetime, sizeof(def_Lifetime));
  stmt.bindResult(5, defPintime, sizeof(defPintime));
  stmt.bindResult(6, max_Lifetime, sizeof(max_Lifetime));
  stmt.bindResult(7, maxPintime, sizeof(maxPintime));
  stmt.bindResult(8, fss_Policy, sizeof(fss_Policy));
  stmt.bindResult(9, gc_Policy, sizeof(gc_Policy));
  stmt.bindResult(10, mig_Policy, sizeof(mig_Policy));
  stmt.bindResult(11, rs_Policy, sizeof(rs_Policy));
  stmt.bindResult(12, Groups, sizeof(Groups));
  stmt.bindResult(13, ret_Policy, sizeof(ret_Policy));
  stmt.bindResult(14, s_Type, sizeof(s_Type));
  stmt.bindResult(15, poolType, sizeof(poolType));
  stmt.bindResult(16, poolMeta, sizeof(poolMeta));

  while (stmt.fetch()) {
    pool.clear();
    pool.name = poolName;
    pool["defsize"] = std::strtoul(defSize, NULL, 0);
    pool["gc_start_thresh"] = std::strtoul(gc_Start_Thresh, NULL, 0);
    pool["gc_stop_thresh"] = std::strtoul(gc_Stop_Thresh, NULL, 0);
    pool["def_lifetime"] = std::strtoul(def_Lifetime, NULL, 0);
    pool["defpintime"] = std::strtoul(defPintime, NULL, 0);
    pool["max_lifetime"] = std::strtoul(max_Lifetime, NULL, 0);
    pool["maxpintime"] = std::strtoul(maxPintime, NULL, 0);
    pool["fss_policy"] = std::string(fss_Policy);
    pool["gc_policy"] = std::string(gc_Policy);
    pool["mig_policy"] = std::string(mig_Policy);
    pool["rs_policy"] = std::string(rs_Policy);
    std::vector<boost::any> group_list;
    std::string gr = std::string(Groups);
    std::stringstream ss(gr);
    int i;
    while (ss >> i)
    {
      i = Extensible::anyToUnsigned(i);
      group_list.push_back(i);
      if (ss.peek() == ',')
        ss.ignore();
    }
    pool["groups"] = group_list;
    pool["ret_policy"] = std::string(ret_Policy);
    pool["s_type"] = std::string(s_Type);
    pool.type = poolType;
    pool.deserialize(poolMeta);
    pools.push_back(pool);
  }

  return pools;
}



Pool MySqlPoolManager::getPool(const std::string& poolname) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " poolname:" << poolname);

  std::vector<Pool> pools = getPools(kAny);
  std::vector<Pool>::iterator it;
  for (it = pools.begin(); it != pools.end(); ++it) {
    if (it->name == poolname) {
      Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. poolname:" << it->name);
      return *it;
    }
  }

  throw DmException(DMLITE_NO_SUCH_POOL, "Pool '%s' not found", poolname.c_str());
}



void MySqlPoolManager::newPool(const Pool& pool) throw (DmException)
{
  Log(Logger::Lvl2, mysqllogmask, mysqllogname, " pool:" << pool.name);

  // Must be root, or root group
  if (this->secCtx_->user.getUnsigned("uid") != 0 &&
      !dmlite::hasGroup(this->secCtx_->groups, 0)) {
    throw DmException(EACCES, "Only root user or root group can add pools");
  }

  // Get the driver
  PoolDriver* driver = this->stack_->getPoolDriver(pool.type);

  // Call the driver before
  driver->toBeCreated(pool);

  // Filesystem is the legacy pooltype, so the driver creates itself
  // the entry. Not nice, but we need to keep compatibility, and
  // since the running DPM daemon wouldn't see this until it is
  // restarted, let it do it itself.
  if (pool.type != "filesystem") {
    PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
    Statement stmt(conn, this->dpmDb_, STMT_INSERT_POOL);

    std::vector<boost::any> groups = pool.getVector("groups");
    std::ostringstream gids;

    if (groups.size() > 0) {
      unsigned i;
      for (i = 0; i < groups.size() - 1; ++i)
        gids << Extensible::anyToUnsigned(groups[i]) << ",";
      gids << Extensible::anyToUnsigned(groups[i]);
    }
    else {
      gids << "0";
    }

    stmt.bindParam( 0, pool.name);
    stmt.bindParam( 1, pool.getLong("defsize"));
    stmt.bindParam( 2, pool.getLong("gc_start_thresh"));
    stmt.bindParam( 3, pool.getLong("gc_stop_thresh"));
    stmt.bindParam( 4, pool.getLong("def_lifetime"));
    stmt.bindParam( 5, pool.getLong("defpintime"));
    stmt.bindParam( 6, pool.getLong("max_lifetime"));
    stmt.bindParam( 7, pool.getLong("maxpintime"));
    stmt.bindParam( 8, pool.getString("fss_policy"));
    stmt.bindParam( 9, pool.getString("gc_policy"));
    stmt.bindParam(10, pool.getString("mig_policy"));
    stmt.bindParam(11, pool.getString("rs_policy"));
    stmt.bindParam(12, gids.str());
    stmt.bindParam(13, pool.getString("ret_policy"));
    stmt.bindParam(14, pool.getString("s_type"));
    stmt.bindParam(15, pool.type);
    stmt.bindParam(16, pool.serialize());

    stmt.execute();
  }

  // Let the driver know
  driver->justCreated(pool);

  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting. poolname:" << pool.name);
}



void MySqlPoolManager::updatePool(const Pool& pool) throw (DmException)
{
  Log(Logger::Lvl2, mysqllogmask, mysqllogname, " pool:" << pool.name);

  // Must be root, or root group
  if (this->secCtx_->user.getUnsigned("uid") != 0 &&
      !dmlite::hasGroup(this->secCtx_->groups, 0)) {
    throw DmException(EACCES, "Only root user or root group can modify pools");
  }

  // Get the driver
  PoolDriver* driver = this->stack_->getPoolDriver(pool.type);

  // Update the db
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->dpmDb_, STMT_UPDATE_POOL);

  std::vector<boost::any> groups = pool.getVector("groups");
  std::ostringstream gids;

  if (groups.size() > 0) {
    unsigned i;
    for (i = 0; i < groups.size() - 1; ++i)
      gids << Extensible::anyToUnsigned(groups[i]) << ",";
    gids << Extensible::anyToUnsigned(groups[i]);
  }
  else {
    gids << "0";
  }

  stmt.bindParam( 0, pool.getLong("defsize"));
  stmt.bindParam( 1, pool.getLong("gc_start_thresh"));
  stmt.bindParam( 2, pool.getLong("gc_stop_thresh"));
  stmt.bindParam( 3, pool.getLong("def_lifetime"));
  stmt.bindParam( 4, pool.getLong("defpintime"));
  stmt.bindParam( 5, pool.getLong("max_lifetime"));
  stmt.bindParam( 6, pool.getLong("maxpintime"));
  stmt.bindParam( 7, pool.getString("fss_policy"));
  stmt.bindParam( 8, pool.getString("gc_policy"));
  stmt.bindParam( 9, pool.getString("mig_policy"));
  stmt.bindParam(10, pool.getString("rs_policy"));
  stmt.bindParam(11, gids.str());
  stmt.bindParam(12, pool.getString("ret_policy"));
  stmt.bindParam(13, pool.getString("s_type"));
  stmt.bindParam(14, pool.type);
  Pool p = pool;
  p.erase("defsize");
  p.erase("gc_start_thresh");
  p.erase("gc_stop_thresh");
  p.erase("def_lifetime");
  p.erase("defpintime");
  p.erase("max_lifetime");
  p.erase("maxpintime");
  p.erase("fss_policy");
  p.erase("gc_policy");
  p.erase("mig_policy");
  p.erase("rs_policy");
  p.erase("groups");
  p.erase("ret_policy");
  p.erase("s_type");
  stmt.bindParam(15, p.serialize());
  stmt.bindParam(16, pool.name);

  if (stmt.execute() == 0)
    throw DmException(DMLITE_NO_SUCH_POOL, "Pool '%s' not found", pool.name.c_str());

  // Tell the driver, since it may need to know
  driver->update(pool);

  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting. poolname:" << pool.name);
}



void MySqlPoolManager::deletePool(const Pool& pool) throw (DmException)
{
  Log(Logger::Lvl2, mysqllogmask, mysqllogname, " pool:" << pool.name);

    // Must be root, or root group
  if (this->secCtx_->user.getUnsigned("uid") != 0 &&
      !dmlite::hasGroup(this->secCtx_->groups, 0)) {
    throw DmException(EACCES, "Only root user or root group can delete pools");
  }

  // Notify the driver
  this->stack_->getPoolDriver(pool.type)->toBeDeleted(pool);

  // Remove from the database, ignoring if it does not exist, since
  // the handler may have done it (fs)
  PoolGrabber<MYSQL*> conn(MySqlHolder::getMySqlPool());
  Statement stmt(conn, this->dpmDb_, STMT_DELETE_POOL);
  stmt.bindParam(0, pool.name);
  stmt.execute();

  Log(Logger::Lvl1, mysqllogmask, mysqllogname, "Exiting. poolname:" << pool.name);
}



Location MySqlPoolManager::whereToRead(const std::string& path) throw (DmException)
{
  return this->whereToRead(this->stack_->getCatalog()->getReplicas(path));
}



Location MySqlPoolManager::whereToRead(ino_t inode) throw (DmException)
{
  return this->whereToRead(this->stack_->getINode()->getReplicas(inode));
}



Location MySqlPoolManager::whereToRead(const std::vector<Replica>& replicas) throw (DmException)
{
  unsigned i;
  std::vector<Location> available;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " nr:" << replicas.size());

  if (replicas.size() == 0)
    throw DmException(DMLITE_NO_REPLICAS, "No replicas");


  // See if available, and translate
  for (i = 0; i < replicas.size(); ++i) {
    if (replicas[i].hasField("pool")) {
      Pool pool = this->getPool(replicas[i].getString("pool"));

      try {
        PoolHandler* handler = this->stack_->getPoolDriver(pool.type)->
                                  createPoolHandler(pool.name);
        if (handler->replicaIsAvailable(replicas[i]))
          available.push_back(handler->whereToRead(replicas[i]));

        delete handler;
      }
      catch (DmException& e) {
        if (e.code() != DMLITE_NO_SUCH_POOL) throw;
      }
    }
  }

  // Pick a random one from the available
  if (available.size() > 0) {
    i = rand() % available.size();
    Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. rep:" << available[i].toString());
    return available[i];
  }
  else {
    throw DmException(DMLITE_NO_REPLICAS,
                      "None of the replicas is available for reading");
  }
}



Location MySqlPoolManager::whereToWrite(const std::string& path) throw (DmException)
{
  bool overwrite = 0;
  mode_t createMode = 0664;
  Acl acl;

  Log(Logger::Lvl4, mysqllogmask, mysqllogname, " path:" << path);

  if (this->stack_->contains("overwrite"))
    overwrite = Extensible::anyToBoolean(this->stack_->get("overwrite"));

  std::vector<Pool> pools = this->getPools(PoolManager::kForWrite);
  if (pools.size() == 0)
    throw DmException(ENOSPC, "There are no pools available for writing");

  // Pick a pool as requested
  Pool chosenPool;
  bool poolFound = false;
  if (this->stack_->contains("pool")) {
    boost::any any = this->stack_->get("pool");
    std::string requestedPool = Extensible::anyToString(any);

    std::vector<Pool>::const_iterator i;
    for (i = pools.begin(); i != pools.end(); ++i) {
      if ((*i).name == requestedPool) {
        chosenPool = *i;
        poolFound = true;
        break;
      }
    }
    if (!poolFound)
      throw DmException(ENOSPC, "The specified pool could not be selected");
  }
  // Or pick a random one
  else {
    unsigned i = rand()  % pools.size();
    chosenPool = pools[i];
  }

  // Get the handler
  PoolHandler* handler = this->stack_->getPoolDriver(chosenPool.type)->createPoolHandler(chosenPool.name);

  // Remove replicas if overwrite is true and the file exists
  if (overwrite) {
      try {
          ExtendedStat xstat = this->stack_->getCatalog()->extendedStat(path);
          std::vector<Replica> replicas = this->stack_->getCatalog()->getReplicas(path);

          createMode = xstat.stat.st_mode;
          acl = xstat.acl;

          for (std::vector<Replica>::const_iterator i = replicas.begin(); i != replicas.end(); ++i) {
            std::string poolName = i->getString("pool");
            if (!poolName.empty()) {
              Pool pool = this->stack_->getPoolManager()->getPool(poolName);
              PoolDriver* driver = this->stack_->getPoolDriver(pool.type);
              PoolHandler* handler = driver->createPoolHandler(pool.name);

              // beware: this also removes the stat entry
              try {
                handler->removeReplica(*i);
              } catch (dmlite::DmException& e) {
                if (e.code() != ENOENT) throw;
              }

              delete handler;
            }

            // Remove replica (if the driver didn't)
            try {
              this->stack_->getCatalog()->deleteReplica(*i);
            }
            catch (DmException& e) {
              if (e.code() != DMLITE_NO_SUCH_REPLICA) throw;
            }
          }
      }
      catch (dmlite::DmException& e) {
          if (e.code() != ENOENT) throw;
      }
      /* This statement is because of memcache, please don't remove it.
       * Further up, handler->removeReplica also removes the file's stat entry,
       * directly on the dpns level with the adapter. From this, it's not
       * possible to inform higher levels in the dmlite stack to clear any
       * information, e.g. clearing the memcache.
       * Calling unlink on the plugin stack solves this.
       * This call will, if everything above went right, because the entry
       * has already been deleted, so we ignore all exceptions. If one of the
       * plugins in the stack raises an exception before it gets to the memcache
       * plugin, this approach fails.
       */
      try {
        this->stack_->getCatalog()->unlink(path);
      } catch (...) { /* pass */ }
  }

  // Create the entry if it does not exist already
  // Note: the expected behavior is to truncate and keep mode if it does
  // Don't fail if it is a legitimate replication request
  if (this->secCtx_->user.name == this->adminUsername_ &&
      this->stack_->contains("replicate")) {
    try {
      this->stack_->getCatalog()->extendedStat(path);
    } catch (DmException& e) {
      this->stack_->getCatalog()->create(path, createMode);
    }
  } else {
    this->stack_->getCatalog()->create(path, createMode);
  }
  if (!acl.empty())
    this->stack_->getCatalog()->setAcl(path, acl);

  // Delegate to it
  Location loc = handler->whereToWrite(path);
  delete handler;

  // Done!
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. loc:" << loc.toString());
  return loc;
}



void MySqlPoolManager::cancelWrite(const Location& loc) throw (DmException)
{
  Log(Logger::Lvl4, mysqllogmask, mysqllogname, "loc:" << loc.toString());

  if (loc.empty())
    throw DmException(EINVAL, "Location is empty");

  Replica replica;
  try {
    replica = this->stack_->getCatalog()->getReplicaByRFN(loc[0].url.path);
  }
  catch (DmException& e) {
    if (e.code() != DMLITE_NO_SUCH_REPLICA) throw;
    // Try again with old-style replicas (host:path)
    replica = this->stack_->getCatalog()->getReplicaByRFN(loc[0].url.domain + ":" + loc[0].url.path);
  }

  Pool pool = this->getPool(replica.getString("pool"));
  PoolHandler* handler = this->stack_->getPoolDriver(pool.type)->createPoolHandler(pool.name);

  handler->cancelWrite(loc);
  this->stack_->getINode()->unlink(replica.fileid);


  delete handler;
  Log(Logger::Lvl3, mysqllogmask, mysqllogname, "Exiting. loc:" << loc.toString());

}
