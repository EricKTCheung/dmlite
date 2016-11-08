/// @file    include/dmlite/cpp/utils/poolcontainer.h
/// @brief   Pooling
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_POOLCONTAINER_H
#define DMLITE_CPP_UTILS_POOLCONTAINER_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
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
    /// @param n       The number of resources to keep in the pool. Up to 2*n slots can be created without penalty (but only n will be pooled)
    PoolContainer(PoolElementFactory<E>* factory, int n): max_(n), factory_(factory), freeSlots_(2*n)
    {
    }

    /// Destructor
    ~PoolContainer()
    {
      boost::mutex::scoped_lock lock(mutex_);
      // Free 'free'
      while (free_.size() > 0) {
        E e = free_.front();
        free_.pop_front();
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
      bool found = false;
      E e;

      { // lock scope
        boost::mutex::scoped_lock lock(mutex_);

        // Wait for one free
        if (!block && (freeSlots_ == 0)) {
          throw DmException(DMLITE_SYSERR(EBUSY),
                            std::string("No resources available"));
        }

        boost::system_time const timeout = boost::get_system_time() + boost::posix_time::seconds(60);

        while (freeSlots_ < 1) {
          if (boost::get_system_time() >= timeout) {
            syslog(LOG_USER | LOG_WARNING, "Timeout...%d seconds in '%s'", 60, __PRETTY_FUNCTION__);
            break;
          }
          available_.timed_wait(lock, timeout);
        }

        // If there is any in the queue, give one from there
        if (free_.size() > 0) {
          e = free_.front();
          free_.pop_front();
          // May have expired! In this case the element as to be destroyed,
          // and the conclusion is that we have not found a good one in the pool
          if (!factory_->isValid(e)) {
            factory_->destroy(e);
          }
          else
            found = true;
        }

      } // lock

      // We create a new element out of the lock. This may help for elements that need other elements
      // of the same type to be constructed (sigh)
      if (!found)
        e = factory_->create();

      { // lock scope (again, sigh)
        boost::mutex::scoped_lock lock(mutex_);
        // Keep track of used
        used_.insert(std::pair<E, unsigned>(e, 1));

        // Note that in case of timeout freeSlots_ can become negative
        --freeSlots_;
      }
      return e;
    }

    /// Increases the reference count of a resource.
    E acquire(E e)
    {
      boost::mutex::scoped_lock lock(mutex_);

      // Make sure it is there
      typename std::map<E, unsigned>::const_iterator i = used_.find(e);
      if (i == used_.end()) {
        throw DmException(DMLITE_SYSERR(EINVAL), std::string("The resource has not been locked previously!"));
      }

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
          free_.push_back(e);
        }
        else {
          // If we are fine, destroy
          factory_->destroy(e);
        }
      }
      available_.notify_one();
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
      // The resizing will be done as we get requests
      boost::mutex::scoped_lock lock(mutex_);
      max_ = ns;


      freeSlots_ = 2*max_ - used_.size();
      // Increment the semaphore size if needed
      // Take into account the used
      if (freeSlots_ > 0)
        available_.notify_all();
    }

   private:
    // The max count of pooled instances
    int max_;

    PoolElementFactory<E> *factory_;

    std::deque<E>         free_;
    std::map<E, unsigned> used_;
    int freeSlots_;

    boost::mutex              mutex_;
    boost::condition_variable available_;
  };

  /// Convenience class that releases a resource on destruction
  template <class E>
  class PoolGrabber {
   public:
    PoolGrabber(PoolContainer<E>& pool, bool block = true): pool_(pool)
    {
      element_ = pool_.acquire(block);
    }

    ~PoolGrabber() {
      pool_.release(element_);
    }

    operator E ()
    {
      return element_;
    }

   private:
    PoolContainer<E>& pool_;
    E element_;
  };
};

#endif // DMLITE_CPP_UTILS_POOLCONTAINER_H
