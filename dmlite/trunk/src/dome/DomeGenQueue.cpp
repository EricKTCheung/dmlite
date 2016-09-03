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



/** @file   DomeGenQueue.cpp
 * @brief  A helper class that implements a thread safe, generic task queue
 * @author Fabrizio Furano
 * @date   Dec 2015
 */

#include "DomeGenQueue.h"
#include "utils/logger.h"
#include "DomeStatus.h"
#include "DomeLog.h"

#include <boost/make_shared.hpp>
#include <iostream>

// using namespace dmlite;

using boost::shared_ptr;
using boost::recursive_mutex;


void GenPrioQueueItem::update(std::string name, GenPrioQueueItem::QStatus st, int prio, const std::vector<std::string> &qual) {
  namekey = name;
  status = st;
  priority = prio;
  qualifiers = qual;
}

GenPrioQueue::GenPrioQueue(int timeoutsecs, std::vector<size_t> qualifiercountlimits): timeout(timeoutsecs),limits(qualifiercountlimits) {
  // populate the active structure
  for(unsigned int i = 0; i < limits.size(); i++) {
    active.push_back(std::map<std::string, size_t>());
  }
}

GenPrioQueue::~GenPrioQueue() {
}

void GenPrioQueue::addToTimesort(GenPrioQueueItem_ptr item) {
  accesstimeKey key;
  key.accesstime = item->accesstime;
  key.namekey = item->namekey;
  timesort[key] = item;
}

void GenPrioQueue::removeFromTimesort(GenPrioQueueItem_ptr item) {
  accesstimeKey key;
  key.accesstime = item->accesstime;
  key.namekey = item->namekey;
  timesort.erase(key);
}

void GenPrioQueue::updateAccessTime(GenPrioQueueItem_ptr item) {
  struct timespec newtime;
  clock_gettime(CLOCK_MONOTONIC, &newtime);

  accesstimeKey key;
  key.accesstime = item->accesstime;
  key.namekey = item->namekey;
  timesort.erase(key);

  key.accesstime = newtime;
  item->accesstime = newtime;
  timesort[key] = item;
}

int GenPrioQueue::insertItem(GenPrioQueueItem_ptr item) {
  clock_gettime(CLOCK_MONOTONIC, &item->insertiontime);
  item->accesstime = item->insertiontime;
  addToTimesort(item);

  if(item->status == GenPrioQueueItem::Waiting) {
    addToWaiting(item);
  }
  else if(item->status == GenPrioQueueItem::Running) {
    addToRunning(item);
  }
  else {
    Log(Logger::Lvl4, domelogmask, domelogname, " WARNING: Tried to add item with status neither Waiting nor Running");
    return -1;
  }

  items[item->namekey] = item;
  return 0;
}

void GenPrioQueue::updateStatus(GenPrioQueueItem_ptr item, GenPrioQueueItem::QStatus status) {
  if(item->status == status) return;

  if(item->status == GenPrioQueueItem::Waiting) {
    removeFromWaiting(item);
  }

  if(item->status == GenPrioQueueItem::Running) {
    removeFromRunning(item);
  }

  if(status == GenPrioQueueItem::Waiting) {
    addToWaiting(item);
  }

  if(status == GenPrioQueueItem::Running) {
    addToRunning(item);
  }

  item->status = status;
}

void GenPrioQueue::addToWaiting(GenPrioQueueItem_ptr item) {
  waitingKey key(item->priority, item->insertiontime, item->namekey);
  waiting[key] = item;
}

void GenPrioQueue::removeFromWaiting(GenPrioQueueItem_ptr item) {
  waitingKey key(item->priority, item->insertiontime, item->namekey);
  waiting.erase(key);
}

// this function makes no checks if the limits are violated - it is intentional
void GenPrioQueue::addToRunning(GenPrioQueueItem_ptr item) {
  for(unsigned int i = 0; i < item->qualifiers.size(); i++) {
    if(i >= limits.size()) break;
    active[i][item->qualifiers[i]]++;
  }
}

void GenPrioQueue::removeFromRunning(GenPrioQueueItem_ptr item) {
  for(unsigned int i = 0; i < item->qualifiers.size(); i++) {
    if(i >= limits.size()) break;
    active[i][item->qualifiers[i]]--;

    if(active[i][item->qualifiers[i]] == 0) {
      active[i].erase(item->qualifiers[i]);
    }
  }
}

bool GenPrioQueue::possibleToRun(GenPrioQueueItem_ptr item) {
  for(unsigned int i = 0; i < item->qualifiers.size(); i++) {
    if(i >= limits.size()) break;

    if(active[i][item->qualifiers[i]] >= limits[i]) {
      return false;
    }
  }
  return true;
}

int GenPrioQueue::touchItemOrCreateNew(std::string namekey, GenPrioQueueItem::QStatus status, int priority, const std::vector<std::string> &qualifiers) {
  scoped_lock lock(*this);
  Log(Logger::Lvl4, domelogmask, domelogname, " Touching new item to the queue with name: " << namekey << ", status: " << status <<
      "priority: " << priority);

  GenPrioQueueItem_ptr item = items[namekey];

  // is this a new item to add to the queue?
  if(item == NULL) {
    item = boost::make_shared<GenPrioQueueItem>();
    item->update(namekey, status, priority, qualifiers);
    insertItem(item);
  }
  // nope, but maybe I need to update it
  else {
    updateAccessTime(item);

    // is it finished? remove the item
    if(status == GenPrioQueueItem::Finished) {
      removeItem(namekey);
    }
    // difficult updates with consequences on internal data structures
    // need to remove and re-insert
    else if(priority != item->priority || qualifiers != item->qualifiers) {
      // only allow forward changes to the status, even in this case
      GenPrioQueueItem::QStatus newStatus = item->status;
      if(status > item->status) newStatus = status;

      removeItem(namekey);
      item->update(namekey, newStatus, priority, qualifiers);
      insertItem(item);
    }
    // easy update - only progress status
    else if(item->status < status) {
      updateStatus(item, status);
    }
    else {
      // nothing has changed, nothing to do
    }
  }

  return 0;
}

size_t GenPrioQueue::nWaiting() {
  return waiting.size();
}

size_t GenPrioQueue::nTotal() {
  return items.size();
}

GenPrioQueueItem_ptr GenPrioQueue::removeItem(std::string namekey) {
  scoped_lock lock(*this);

  GenPrioQueueItem_ptr item = items[namekey];
  if(item == NULL) return item;

  updateStatus(item, GenPrioQueueItem::Finished);
  removeFromTimesort(item);
  items.erase(namekey);

  return item;
}

GenPrioQueueItem_ptr GenPrioQueue::getNextToRun() {
  scoped_lock lock(*this);
  std::map<waitingKey, GenPrioQueueItem_ptr>::iterator it;
  for(it = waiting.begin(); it != waiting.end(); it++) {
    GenPrioQueueItem_ptr item = it->second;

    if(possibleToRun(item)) {
      updateStatus(item, GenPrioQueueItem::Running);
      return item;
    }
  }
  return GenPrioQueueItem_ptr();
}

int GenPrioQueue::tick() {
  scoped_lock lock(*this);
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);

  std::map<accesstimeKey, GenPrioQueueItem_ptr>::iterator it;

  for(it = timesort.begin(); it != timesort.end(); it++) {
    GenPrioQueueItem_ptr item = it->second;
    if(now.tv_sec > item->accesstime.tv_sec + timeout) {
       Log(Logger::Lvl1, domelogmask, domelogname, " Queue item with key '" << item->namekey << "' timed out after " << timeout << " seconds.");

      // don't modify status through removal
      GenPrioQueueItem::QStatus status = item->status;
      removeItem(item->namekey);
      item->status = status;
    }
    else {
      return 0; // the rest of the items are guaranteed to be newer
    }
  }
  return 0;
}
