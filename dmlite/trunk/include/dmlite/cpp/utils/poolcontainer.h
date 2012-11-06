/// @file    include/dmlite/cpp/utils/poolcontainer.h
/// @brief   Pooling
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_POOLCONTAINER_H
#define DMLITE_CPP_UTILS_POOLCONTAINER_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <map>
#include <syslog.h>
#include <queue>
#include "../exceptions.h"

namespace dmlite {

  /// Classes implementing this interface creates the actual element
  /// since the pool is agnosstic
  template <class E>
  class PoolElementFactory {
   public:
    /// Destructor
    virtual ~PoolElementFactory() {};

    /// Creates an element
    virtual E create() = 0;

    /// Destroys an element
    virtual void destroy(E) = 0;

    /// Check it is still valid
    virtual bool isValid(E) = 0;
  };


  /// Implements a pool of whichever resource
  template <class E>
  class PoolContainer {
   public:
    /// Constructor
    /// @param factory The factory to use when spawning a new resource.
    /// @param n       The number of resources to keep.
    PoolContainer(PoolElementFactory<E>* factory, int n): max_(n), factory_(factory), freeSlots_(n)
    {
    }

    /// Destructor
    ~PoolContainer()
    {
      // Free 'free'
      while (free_.size() > 0) {
        E e = free_.front();
        free_.pop();
        factory_->destroy(e);
      }
      // Freeing used is dangerous, as we might block if the client code
      // forgot about something. Assume the memory leak :(
      if (used_.size() > 0) {
        syslog(LOG_USER | LOG_WARNING, "%ld used elements from a pool not released on destruction!", (long)used_.size());
      }
    }

    /// Acquires a free resource.
    E  acquire(bool block = true)
    {
      E e;
      // Wait for one free
      if (!block && freeSlots_ == 0) {
        throw DmException(DMLITE_SYSERR(EBUSY),
                          std::string("No resources available"));
      }

      boost::mutex::scoped_lock lock(mutex_);
      while (freeSlots_ < 1)
        available_.wait(lock);

      // If there is any in the queue, give one from there
      if (free_.size() > 0) {
        e = free_.front();
        free_.pop();
        // May have expired!
        if (!factory_->isValid(e)) {
          factory_->destroy(e);
          e = factory_->create();
        }
      }
      else {
        // None created, so create it now
        e = factory_->create();
      }
      // Keep track of used
      used_.insert(std::pair<E, unsigned>(e, 1));
      --freeSlots_;

      return e;
    }

    /// Increases the reference count of a resource.
    E acquire(E e)
    {
      boost::mutex::scoped_lock lock(mutex_);

      // Make sure it is there
      typename std::map<E, unsigned>::const_iterator i = used_.find(e);
      if (i == used_.end())
        throw DmException(DMLITE_SYSERR(EINVAL),
                          std::string("The resource has not been locked previously!"));

      // Increase
      used_[e]++;

      // End
      return e;
    }

    /// Releases a resource
    /// @param e The resource to release.
    /// @return  The reference count after releasing.
    unsigned release(E e)
    {
      boost::mutex::scoped_lock lock(mutex_);
      // Decrease reference count
      unsigned remaining = --used_[e];
      // No one else using it (hopefully...)
      if (used_[e] == 0) {
        // Remove from used
        used_.erase(e);
        // If the free size is less than the maximum, push to free and notify
        if ((long)free_.size() < max_) {
          free_.push(e);
          available_.notify_one();
        }
        else {
          // If we are fine, destroy
          factory_->destroy(e);
        }
      }
      ++freeSlots_;

      return remaining;
    }

    /// Count the number of instances
    unsigned refCount(E e)
    {
      typename std::map<E, unsigned>::const_iterator i = used_.find(e);
      if (i == used_.end())
        return 0;
      return used_[e];
    }

    /// Change the pool size
    /// @param ns The new size.
    void resize(int ns)
    {
      int total, sv;
      // The resizing will be done as we get requests
      boost::mutex::scoped_lock lock(mutex_);
      max_ = ns;
      freeSlots_ = max_ - used_.size();
      // Increment the semaphore size if needed
      // Take into account the used
      if (freeSlots_ > 0)
        available_.notify_all();
    }

   private:
    int max_;

    PoolElementFactory<E> *factory_;

    std::queue<E>         free_;
    std::map<E, unsigned> used_;
    unsigned freeSlots_;

    boost::mutex              mutex_;
    boost::condition_variable available_;
  };

};

#endif // DMLITE_CPP_UTILS_POOLCONTAINER_H
