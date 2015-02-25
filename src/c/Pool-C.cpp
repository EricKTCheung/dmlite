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
    ::memset(locp->chunks, 0, sizeof(dmlite_chunk) * locp->nchunks);

    for (unsigned i = 0; i < locp->nchunks; ++i) {
      locp->chunks[i].offset = loc[i].offset;
      locp->chunks[i].size   = loc[i].size;

      strncpy(locp->chunks[i].url.scheme, loc[i].url.scheme.c_str(),
              sizeof(locp->chunks[i].url.scheme));

      strncpy(locp->chunks[i].url.domain, loc[i].url.domain.c_str(),
              sizeof(locp->chunks[i].url.domain));

      locp->chunks[i].url.port = loc[i].url.port;

      strncpy(locp->chunks[i].url.path, loc[i].url.path.c_str(),
              sizeof(locp->chunks[i].url.path));
      
      if (locp->chunks[i].url.query == NULL)
        locp->chunks[i].url.query = dmlite_any_dict_new();
      locp->chunks[i].url.query->extensible = loc[i].url.query;
    }
  }
  else {
    locp->chunks = NULL;
  }

  return locp;
}



void dmlite_clocation_to_cpplocation(const dmlite_location* locp,
                                     dmlite::Location& loc)
{
  for (unsigned i = 0; i < locp->nchunks; ++i) {
    dmlite::Chunk chunk;
    chunk.url.scheme = locp->chunks[i].url.scheme;
    chunk.url.domain = locp->chunks[i].url.domain;
    chunk.url.port   = locp->chunks[i].url.port;
    chunk.url.path   = locp->chunks[i].url.path;
    if (locp->chunks[i].url.query)
      chunk.url.query  = locp->chunks[i].url.query->extensible;

    chunk.offset = locp->chunks[i].offset;
    chunk.size   = locp->chunks[i].size;

    loc.push_back(chunk);
  }
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



int dmlite_pools_free(unsigned npools, dmlite_pool* pools)
{
  for (unsigned i = 0; i < npools; ++i)
    delete pools[i].extra;
  delete [] pools;
  return 0;
}



int dmlite_location_free(dmlite_location* loc)
{
  if (loc) {
    for (unsigned i = 0; i < loc->nchunks; ++i)
      dmlite_any_dict_free(loc->chunks[i].url.query);
    delete [] loc->chunks;
    delete    loc;
  }
  return 0;
}



dmlite_location* dmlite_get(dmlite_context* context, const char* path)
{
  TRY(context, get)
  NOT_NULL(path);
  dmlite::Location loc = context->stack->getPoolManager()->whereToRead(path);
  
  dmlite_location *locp = new dmlite_location();  
  memset(locp, 0, sizeof(dmlite_location));
  return dmlite_cpplocation_to_clocation(loc, locp);
  CATCH_POINTER(context, get)
}



dmlite_location* dmlite_iget(dmlite_context* context, ino_t inode)
{
  TRY(context, get)
  dmlite::Location loc = context->stack->getPoolManager()->whereToRead(inode);
  
  dmlite_location *locp = new dmlite_location();
  memset(locp, 0, sizeof(dmlite_location));
  return dmlite_cpplocation_to_clocation(loc, locp);
  CATCH_POINTER(context, get)
}



dmlite_location* dmlite_getlocation(dmlite_context* context, const dmlite_replica* replica)
{
  TRY(context, getlocation)
  NOT_NULL(replica);
  
  dmlite::Replica replicapp;
  dmlite_creplica_to_cppreplica(replica,
                                &replicapp);
  
  dmlite::Pool pool = context->stack->getPoolManager()->getPool(replicapp.getString("pool"));
  dmlite::PoolDriver*  driver = context->stack->getPoolDriver(pool.type);
  dmlite::PoolHandler* handler = driver->createPoolHandler(pool.name);
  
  try {
    dmlite::Location loc = handler->whereToRead(replicapp);
    delete handler;
    
    dmlite_location* locp = new dmlite_location();
    memset(locp, 0, sizeof(dmlite_location));
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
  memset(locp, 0, sizeof(dmlite_location));
  return dmlite_cpplocation_to_clocation(loc, locp);
  CATCH_POINTER(context, put)
}



int dmlite_put_abort(dmlite_context* context, const dmlite_location* loc)
{
  TRY(context, put_abort)
  NOT_NULL(loc);
  dmlite::Location location;
  dmlite_clocation_to_cpplocation(loc, location);
  context->stack->getPoolManager()->cancelWrite(location);
  CATCH(context, put_abort)
}
