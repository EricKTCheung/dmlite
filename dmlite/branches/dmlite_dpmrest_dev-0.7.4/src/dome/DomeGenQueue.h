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



/** @file   DomeGenQueue.h
 * @brief  A helper class that implements a thread safe, generic task queue
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include <string>
#include <vector>
#include <boost/thread.hpp>

class GenPrioQueueItem {
public:
  std::string namekey;
  std::vector<std::string> qualifiers;
  enum QStatus {
    Unknown = 0,
    Waiting,
    Running,
    Finished
  };
};


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
  GenPrioQueue(int timeoutsecs, std::vector<int> qualifiercountlimits);
  virtual ~GenPrioQueue();
  
  
  
  /// Touching an item needs all the parameters because the queue might have been list and needs to
  /// be reconstructed incrementally. New items will be typically in waiting state
  int touchItemOrCreateNew(std::string namekey, GenPrioQueueItem::QStatus status, int priority, std::vector<std::string> &qualifiers);
  
  /// Removes a specific item from the queue, whatever the status is. Gives back a reference to the item
  GenPrioQueueItem *removeItem(std::string namekey);
  
  /// Gets the next item that can transition to status running. Automatically sets it to running too if it can
  GenPrioQueueItem *getNextToRun();
  
  
  /// Queues are alive, and purge unused elements
  int tick();
  
private:
  int timeout;
  std::vector<int> limits;
  
  /// The content of the queue. We just need quick access by namekey and to be able to loop through the items
  std::map<std::string, GenPrioQueueItem *> items;
};
