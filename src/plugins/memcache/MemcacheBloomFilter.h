/// @file    plugins/memcache/MemcacheBloomFilter.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef MEMCACHE_BLOOMFILTER_H
#define MEMCACHE_BLOOMFILTER_H

#include <dmlite/cpp/dmlite.h>
#include "bloom_filter.hpp"

namespace dmlite {

  /*
   * MemcachedBloomFilter:
   *
   * The bloom filter is based on this code:
   * https://code.google.com/p/bloom/
   *
   * The bloom filter holds information about which elements
   * are in the memcached to minimize the failing get requests
   * from memcached.
   * This is very useful in the beginning, when the cache fills
   * up, but becomes less useful, when elements had been added
   * to memcached, but expires. Elements cannot be removed
   * from the bloom filter (without accidentially removing
   * other elements) so the behaviour in the steady state is
   * like running without the filter.
   *
   * Two possible changes could be made:
   * 1. reset the bloom filter after intervals. This would
   *  provoke lots of db access at this point, which is
   *  undesirable. Some sort of this might happen already
   *  with apache when it recycles its processes.
   * 2. add a remove function, which would also remove
   *  additional elements, producing false negatives. This
   *  would lead to continuously more db accesses than
   *  running without the filter.
   *
   * The bloom filter here is statically configured for
   * ~1 Million elements with a false positive probability of
   * 0.001. This leads to a data structure size of 1.8 MByte.
   *
   */
  class MemcacheBloomFilter {
    public:
      MemcacheBloomFilter();
      ~MemcacheBloomFilter();

      void insert(const std::string& key);
      bool contains(const std::string& key);

    protected:
    private:
      bloom_filter filter_;
  };
}
#endif // MEMCACHE_BLOOMFILTER_H
