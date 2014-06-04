/// @file    plugins/common/Pool.h
/// @brief   Pooling
/// @details This is not a plugin!
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef POOL_H
#define	POOL_H

#include <dmlite/dm_exceptions.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <set>
#include <syslog.h>
#include <queue>

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
protected:
private:
};


/// Implements a pool of whichever resource
template <class E>
class PoolContainer {
public:
  /// Constructor
  /// @param factory The factory to use when spawning a new resource.
  /// @param n       The number of resources to keep.
  PoolContainer(PoolElementFactory<E>* factory, int n): max_(n), factory_(factory)
  {
    pthread_mutex_init(&mutex_, NULL);
    sem_init(&available_, 0, n);
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
    // Destroy locks
    pthread_mutex_destroy(&mutex_);
    sem_destroy(&available_);
  }

  /// Acquires a free resource.
  E  acquire(bool block = true)
  {
    E e;
    // Wait for one free
    if (!block) {
      int v;
      sem_getvalue(&available_, &v);
      if (v <= 0)
        throw DmException(DM_RESOURCE_UNAVAILABLE, std::string("No resources available"));
    }
    sem_wait(&available_);
    // Critical section
    pthread_mutex_lock(&mutex_);
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
    used_.insert(e);
    // End of critical section
    pthread_mutex_unlock(&mutex_);
    return e;
  }

  /// Releases a resource
  /// @param e The resource to release.
  void release(E e)
  {
    // Critical section
    pthread_mutex_lock(&mutex_);
    // Remove from used
    used_.erase(e);
    // If the free size is less than the maximum, push to free and notify
    if ((long)free_.size() < max_) {
      free_.push(e);
      sem_post(&available_);
    }
    else {
      // If we are fine, destroy
      factory_->destroy(e);
    }
    // End of critical section
    pthread_mutex_unlock(&mutex_);
  }

  /// Change the pool size
  /// @param ns The new size.
  void resize(int ns)
  {
    int total, sv;
    // The resizing will be done as we get requests
    pthread_mutex_lock(&mutex_);
    max_ = ns;
    // Reduce the semaphore size if needed
    // Do NOT take into account used, as it will auto-regulate as they free
    sem_getvalue(&available_, &sv);
    while (sv > max_) {
      sem_wait(&available_);
      --sv;
    }
    // Increment the semaphore size if needed
    // Take into account the used
    total = sv + used_.size();
    while (total < max_) {
      sem_post(&available_);
      ++total;
    }
    // End of critical
    pthread_mutex_unlock(&mutex_);
  }

protected:
private:
  int max_;

  PoolElementFactory<E> *factory_;

  std::queue<E> free_;
  std::set<E>   used_;

  pthread_mutex_t mutex_;
  sem_t           available_;
};

};

#endif	// POOL_H