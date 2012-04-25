/// @file   core/Pool-C.cpp
/// @brief  C wrapper for dmlite::Pool.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <dmlite/dmlite.h>
#include "Private.h"

int dm_freepools(dm_context* context, int npools, struct pool* pools)
{
  for (int i = 0; i < npools; ++i) {
    delete [] pools[i].gids;
  }
  delete [] pools;
  return 0;
}
