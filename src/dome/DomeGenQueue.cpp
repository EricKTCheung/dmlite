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



/** @file   DpmrGenQueue.cpp
 * @brief  A helper class that implements a thread safe, generic task queue
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include "DomeGenQueue.h"
#include "utils/logger.h"

GenPrioQueue::GenPrioQueue(int timeoutsecs, std::vector<int> qualifiercountlimits): timeout(timeoutsecs),limits(qualifiercountlimits) {
}

GenPrioQueue::~GenPrioQueue() {
}



int GenPrioQueue::touchItemOrCreateNew(std::string namekey, GenPrioQueueItem::QStatus status, int priority, std::vector<std::string> &qualifiers) {
  return 0;
}


GenPrioQueueItem *GenPrioQueue::removeItem(std::string namekey) {
  return 0;
}


GenPrioQueueItem *GenPrioQueue::getNextToRun() {
  return 0;
}