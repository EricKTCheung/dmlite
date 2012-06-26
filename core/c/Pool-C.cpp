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
