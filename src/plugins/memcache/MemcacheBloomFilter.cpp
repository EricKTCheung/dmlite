/// @file    plugins/memcache/MemcacheBloomFilter.cpp
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include "MemcacheBloomFilter.h"

using namespace dmlite;

MemcacheBloomFilter::MemcacheBloomFilter()
{
  bloom_parameters parameters;
  parameters.projected_element_count = (1 << 20); /* 1 Million entries */
  parameters.false_positive_probability = 0.001;

  if (!parameters) {
    throw DmException(DMLITE_SYSERR(ENOSYS),
        "Could not instantiate the bloom filter for memcached");
  }

  parameters.compute_optimal_parameters();

  this->filter_ = bloom_filter(parameters);
}

MemcacheBloomFilter::~MemcacheBloomFilter()
{
  // Do nothing
}

void MemcacheBloomFilter::insert(const std::string& key)
{
  this->filter_.insert(key);
}

bool MemcacheBloomFilter::contains(const std::string& key)
{
  return this->filter_.contains(key);
}
