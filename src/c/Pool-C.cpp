/// @file   core/Pool-C.cpp
/// @brief  C wrapper for dmlite::Pool.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/c/dmlite.h>
#include <dmlite/c/pool.h>
#include <dmlite/cpp/poolmanager.h>
#include "Private.h"



static dmlite_location* dmlite_cpplocation_to_clocation(const dmlite::Location& loc,
                                                        dmlite_location* locp)
{
  locp->nchunks = loc.size();
  if (locp->nchunks > 0) {
    locp->chunks = new dmlite_chunk[locp->nchunks];
    for (unsigned i = 0; i < locp->nchunks; ++i) {
      locp->chunks[i].offset = loc[i].offset;
      locp->chunks[i].size   = loc[i].size;
      strncpy(locp->chunks[i].host, loc[i].host.c_str(),
              sizeof(locp->chunks[i].host));
      strncpy(locp->chunks[i].path, loc[i].path.c_str(),
              sizeof(locp->chunks[i].path));
      
      locp->chunks[i].extra = new dmlite_any_dict();
      locp->chunks[i].extra->extensible.copy(loc[i]);
    }
  }
  else {
    locp->chunks = 0;
  }
  
  return locp;
}


int dmlite_getpools(dmlite_context* context, unsigned* nbpools, dmlite_pool** pools)
{
  TRY(context, getpools)
  NOT_NULL(nbpools);
  NOT_NULL(pools);
  
  std::vector<dmlite::Pool> poolSet = context->stack->getPoolManager()->getPools();

  *pools   = new dmlite_pool[poolSet.size()];
  *nbpools = poolSet.size();

  for (unsigned i = 0; i < poolSet.size(); ++i) {
    strncpy((*pools)[i].pool_type, poolSet[i].type.c_str(),
             sizeof((*pools)[i].pool_type));
    strncpy((*pools)[i].pool_name, poolSet[i].name.c_str(),
            sizeof((*pools)[i].pool_name));
    
    (*pools)[i].extra = new dmlite_any_dict;
    (*pools)[i].extra->extensible.copy(poolSet[i]);
  }

  CATCH(context, getpools)
}



int dmlite_freepools(dmlite_context* context, unsigned npools, dmlite_pool* pools)
{
  for (unsigned i = 0; i < npools; ++i)
    delete pools[i].extra;
  delete [] pools;
  return 0;
}



int dmlite_freelocation(dmlite_context* context, dmlite_location* loc)
{
  for (unsigned i = 0; i < loc->nchunks; ++i)
    delete loc->chunks[i].extra;
  delete loc->chunks;
  delete loc;
  return 0;
}



dmlite_location* dmlite_get(dmlite_context* context, const char* path)
{
  TRY(context, get)
  NOT_NULL(path);
  dmlite::Location loc = context->stack->getPoolManager()->whereToRead(path);
  
  dmlite_location *locp = new dmlite_location();  
  return dmlite_cpplocation_to_clocation(loc, locp);
  CATCH_POINTER(context, get)
}



dmlite_location* dmlite_getlocation(dmlite_context* context, const dmlite_replica* replica)
{
  TRY(context, getlocation)
  NOT_NULL(replica);
  
  dmlite::Replica replicapp;
  
  replicapp.atime      = replica->atime;
  replicapp.fileid     = replica->fileid;
  replicapp.ltime      = replica->ltime;
  replicapp.nbaccesses = replica->nbaccesses;
  replicapp.ptime      = replica->ptime;
  replicapp.replicaid  = replica->replicaid;
  replicapp.rfn        = replica->rfn;
  replicapp.server     = replica->server;
  replicapp.status     = static_cast<dmlite::Replica::ReplicaStatus>(replica->status);
  replicapp.type       = static_cast<dmlite::Replica::ReplicaType>(replica->type);
  
  if (replica->extra != NULL)
    replicapp.copy(replica->extra->extensible);
  
  dmlite::Pool pool = context->stack->getPoolManager()->getPool(replicapp.getString("pool"));
  dmlite::PoolDriver*  driver = context->stack->getPoolDriver(pool.type);
  dmlite::PoolHandler* handler = driver->createPoolHandler(pool.name);
  
  try {
    dmlite::Location loc = handler->whereToRead(replicapp);
    delete handler;
    
    dmlite_location* locp = new dmlite_location();    
    return dmlite_cpplocation_to_clocation(loc, locp);
  }
  catch (...) {
    delete handler;
    throw;
  }
  
  CATCH_POINTER(context, getlocation)
}



dmlite_location* dmlite_put(dmlite_context* context, const char* path)
{
  TRY(context, put)
  NOT_NULL(path);
  dmlite::Location loc = context->stack->getPoolManager()->whereToWrite(path);
  
  dmlite_location* locp = new dmlite_location();  
  return dmlite_cpplocation_to_clocation(loc, locp);
  CATCH_POINTER(context, put)
}



int dmlite_putdone(dmlite_context* context, const char* host, const char* rfn,
                   const dmlite_any_dict* extra)
{
  TRY(context, putdone)
  NOT_NULL(host);
  NOT_NULL(rfn);  
  if (extra != NULL)
    context->stack->getPoolManager()->doneWriting(host, rfn, extra->extensible);
  else
    context->stack->getPoolManager()->doneWriting(host, rfn, dmlite::Extensible());
  CATCH(context, putdone)
}
