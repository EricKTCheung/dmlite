/// @file   core/Pool-C.cpp
/// @brief  C wrapper for dmlite::Pool.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/dmlite.h>
#include <dmlite/c/dm_pool.h>
#include "Private.h"



int dm_getpools(dm_context* context, int* nbpools, struct pool** pools)
{
  TRY(context, getpools)
  NOT_NULL(nbpools);
  NOT_NULL(pools);
  
  std::vector<pool> poolSet = context->stack->getPoolManager()->getPools();

  *pools   = new pool[poolSet.size()];
  *nbpools = poolSet.size();

  std::copy(poolSet.begin(), poolSet.end(), *pools);

  CATCH(context, getpools)
}



int dm_freepools(dm_context* context, int npools, struct pool* pools)
{
  delete [] pools;
  return 0;
}



int dm_freelocation(dm_context* context, struct location* loc)
{
  Location *locp = (Location*)loc;
  delete locp;
  return 0;
}



int dm_get(dm_context* context, const char* path, struct location** loc)
{
  TRY(context, get)
  NOT_NULL(path);
  NOT_NULL(loc);
  Location *locp = new Location(context->stack->getPoolManager()->whereToRead(path));
  *loc = locp;
  CATCH(context, get)
}



int dm_getlocation(dm_context* context, const FileReplica* replica, struct location** loc)
{
  TRY(context, getlocation)
  NOT_NULL(replica);
  
  Pool pool = context->stack->getPoolManager()->getPool(replica->pool);
  dmlite::PoolDriver*  driver = context->stack->getPoolDriver(pool.pool_type);
  dmlite::PoolHandler* handler = driver->createPoolHandler(pool.pool_name);
  
  try {
    Location *locp = new Location(handler->getLocation(*replica));
    *loc = locp;
    delete handler;
  }
  catch (...) {
    delete handler;
    throw;
  }
  
  CATCH(context, getlocation)
}



int dm_put(dm_context* context, const char* path, struct location** loc)
{
  TRY(context, put)
  NOT_NULL(path);
  NOT_NULL(loc);
  Location *locp = new Location(context->stack->getPoolManager()->whereToWrite(path));
  *loc = locp;
  CATCH(context, put)
}



int dm_putdone(dm_context* context, const char* host, const char* rfn, unsigned nextras, struct keyvalue* extrasp)
{
  TRY(context, putdone)
  NOT_NULL(host);
  NOT_NULL(rfn);
  
  std::map<std::string, std::string> extras;
  
  for (unsigned i = 0; i < nextras; ++i)
    extras.insert(std::pair<std::string, std::string>(extrasp[i].key, extrasp[i].value));
  
  context->stack->getPoolManager()->doneWriting(host, rfn, extras);
  CATCH(context, putdone)
}
