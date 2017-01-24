/// @file    plugins/memcache/MemcacheFunctionCounter.h
/// @brief   memcached plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include <boost/format.hpp>
#include <iostream>
#include <stdlib.h>

#include "MemcacheFunctionCounter.h"

using namespace dmlite;

MemcacheFunctionCounter::MemcacheFunctionCounter(int log_prob_indicator):
  log_prob_indicator_(log_prob_indicator)
{
  reset();
}

MemcacheFunctionCounter::~MemcacheFunctionCounter()
{
  // Nothing
}

void MemcacheFunctionCounter::incr(const int key, unsigned int *seed)
{
  {
    boost::mutex::scoped_lock lock(this->write_mutex_);
    this->counter_array_[key] += 1;
  }

  unsigned int random_print_int = rand_r(seed);
  // print with some probability: p = 1/2^(log_prob_indicator_)
  random_print_int >>= sizeof(unsigned int)*8 - this->log_prob_indicator_;

  if (random_print_int == static_cast<unsigned int>(0)) {
    bool do_reset = false;
    std::stringstream log_stream;
    {
      boost::mutex::scoped_lock lock(this->write_mutex_);
      for (int idx = 0; idx < NUM_CATALOG_API_FUNCTIONS; ++idx) {
        log_stream << catalog_func_names[idx];
        log_stream << ": " << this->counter_array_[idx] << std::endl;
        if ((this->counter_array_[idx] - (1LL << 40)) > 0) {
          do_reset = true;
        }
      }
    }
    Log(Logger::Lvl3, memcachelogmask, memcachelogname, log_stream.str().c_str());
    if (do_reset) {
      reset();
    }
  }
}

unsigned int MemcacheFunctionCounter::get(const int key)
{
  return this->counter_array_[key];
}

void MemcacheFunctionCounter::reset()
{
  {
    boost::mutex::scoped_lock lock(this->write_mutex_);
    std::fill_n(this->counter_array_, NUM_CATALOG_API_FUNCTIONS, 0LL);
  }
  Log(Logger::Lvl3, memcachelogmask, memcachelogname,
      "MemcacheFunctionCounter: " <<
      "reset counters to 0");
}
