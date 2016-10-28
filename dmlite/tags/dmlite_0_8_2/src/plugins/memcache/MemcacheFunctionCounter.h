/// @file    plugins/memcache/MemcacheFunctionCounter.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_FUNCTIONCOUNTER_H
#define MEMCACHE_FUNCTIONCOUNTER_H

#include <boost/thread/mutex.hpp>
#include <map>

#include <dmlite/cpp/dmlite.h>

#include "MemcacheFunctions.h"

#include "utils/logger.h"

namespace dmlite {

  extern Logger::bitmask memcachelogmask;
  extern Logger::component memcachelogname;

  class MemcacheFunctionCounter {
    public:
      MemcacheFunctionCounter(int log_prob_indicator);
      ~MemcacheFunctionCounter();

      void incr(const int key, unsigned int *seed);
      unsigned int get(const int key);
      void reset();
    protected:
    private:
      int log_prob_indicator_;
      boost::mutex write_mutex_;
      int64_t counter_array_[NUM_CATALOG_API_FUNCTIONS];
  };
}

#endif // MEMCACHE_FUNCTIONCOUNTER_H
