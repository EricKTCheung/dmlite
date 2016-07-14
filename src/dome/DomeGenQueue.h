/*
 * Copyright 2015 CERN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef DOMEGENQUEUE_H
#define DOMEGENQUEUE_H


/** @file   DomeGenQueue.h
 * @brief  A helper class that implements a thread safe, generic task queue
 * @author Fabrizio Furano
 * @date   Dec 2015
 */

#include <string>
#include <vector>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

typedef std::pair<int, int> GenPrioQueuePriority;
class GenPrioQueue;

class GenPrioQueueItem {
public:
  std::string namekey;
  std::vector<std::string> qualifiers;
  enum QStatus {
    Unknown = 0,
    Waiting = 1,
    Running = 2,
    Finished = 3
  };
  QStatus status;
  GenPrioQueueItem() : status(Unknown) {}

  int priority;
private:
  struct timespec insertiontime;
  struct timespec accesstime;
  void update(std::string, QStatus, int, const std::vector<std::string>&);

  friend class GenPrioQueue;
};

typedef boost::shared_ptr<GenPrioQueueItem> GenPrioQueueItem_ptr;

/// A complex task queue.
/// Each item:
///  - can be identified by its position in the queue or by a string key
///  - has a status like waiting, running, finished
///  - has a priority value in the form of an integer. 0 is low priority. Priority is applied while it's in waiting status
///  - carries a vector of strings that qualify the token with respect to the given queue limits. For example, a checksum operation
///   will have a servername in column 1 and a server:filesystem string in column 2. No more than N1 items can be running
///   in that server, and no more than N2 items can be running in that filesystem
///    - no more than N1 running items with the same value in column 1
///    - no more than N2 running items with the same value in column 2
///    - etc...
///  - is removed from the queue if it's not referenced since some time
///  - is inserted in the queue if it's referenced. This helps reconstructing the queue automatically.

class GenPrioQueue: public boost::recursive_mutex {
public:

  /// Ctor.
  /// @param timeoutsecs the maximum time an element can be not referenced. After this time it is automatically purged
  /// @param qualifiercountlimits the vector containing the limits for the various columns
  GenPrioQueue(int timeoutsecs, std::vector<size_t> qualifiercountlimits);
  virtual ~GenPrioQueue();



  /// Touching an item needs all the parameters because the queue might have been list and needs to
  /// be reconstructed incrementally. New items will be typically in waiting state
  int touchItemOrCreateNew(std::string namekey, GenPrioQueueItem::QStatus status, int priority, const std::vector<std::string> &qualifiers);

  /// Removes a specific item from the queue, whatever the status is. Gives back a reference to the item
  GenPrioQueueItem_ptr removeItem(std::string namekey);

  /// Gets the next item that can transition to status running. Automatically sets it to running too if it can
  GenPrioQueueItem_ptr getNextToRun();

  /// gives the number of jobs in the waiting queue
  size_t nWaiting();

  /// gives the total number of jobs in the queue
  size_t nTotal();

  /// Queues are alive, and purge unused elements
  int tick();

private:
  /// insert item into the internal data structures
  int insertItem(GenPrioQueueItem_ptr);

  void updateStatus(GenPrioQueueItem_ptr, GenPrioQueueItem::QStatus);

  void addToWaiting(GenPrioQueueItem_ptr);
  void addToRunning(GenPrioQueueItem_ptr);
  void addToTimesort(GenPrioQueueItem_ptr);

  void removeFromWaiting(GenPrioQueueItem_ptr);
  void removeFromRunning(GenPrioQueueItem_ptr);
  void removeFromTimesort(GenPrioQueueItem_ptr);

  /// verifies that adding this item doesn't violate a limit
  bool possibleToRun(GenPrioQueueItem_ptr);

  /// updates access time to now
  void updateAccessTime(GenPrioQueueItem_ptr);

  int timeout;
  std::vector<size_t> limits;

  /// The content of the queue. We just need quick access by namekey and to be able to loop through the items
  std::map<std::string, boost::shared_ptr<GenPrioQueueItem> > items;

  /// A structure designed to be able to execute the following operations quickly
  /// - Walk through all waiting items, sorted by priority and timespec.
  ///   Used when deciding which item to run next.
  /// - Remove an item, given that priority, timespec and namekey are known

  struct waitingKey {
    int priority;
    struct timespec insertiontime;
    std::string namekey;
    bool operator<(const waitingKey& src) const {
      if(priority != src.priority) {
        return priority > src.priority;
      }
      if(insertiontime.tv_sec != src.insertiontime.tv_sec) {
        return insertiontime.tv_sec < src.insertiontime.tv_sec;
      }
      if(insertiontime.tv_nsec != src.insertiontime.tv_nsec) {
        return insertiontime.tv_nsec < src.insertiontime.tv_nsec;
      }
      return namekey < src.namekey;
    }
    waitingKey(int prio, struct timespec it, std::string name) : priority(prio), insertiontime(it), namekey(name) {}
  };
  std::map<waitingKey, GenPrioQueueItem_ptr> waiting;

  /// A structure to track the number of currently running operations
  /// Contains one map for every column
  /// The map contains all different values currently in our queue, along with how many are active at this moment
  /// active[0]["hostname"] will give you how many active jobs have "hostname" as their first column, for example
  std::vector<std::map<std::string, size_t> > active;

  /// A structure to sort items based on access time - oldest go first
  struct accesstimeKey {
    struct timespec accesstime;
    std::string namekey;
    bool operator<(const accesstimeKey& src) const {
      if(accesstime.tv_sec != src.accesstime.tv_sec) {
        return accesstime.tv_sec < src.accesstime.tv_sec;
      }
      if(accesstime.tv_nsec != src.accesstime.tv_nsec) {
        return accesstime.tv_nsec < src.accesstime.tv_nsec;
      }
      return namekey < src.namekey;
    }
  };
  std::map<accesstimeKey, GenPrioQueueItem_ptr> timesort;
};

#endif
