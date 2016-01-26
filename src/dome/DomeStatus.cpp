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



/** @file   DomeStatus.cpp
 * @brief  A helper class that describes the internal status of the storage system, pools, disks, etc
 * @author Fabrizio Furano
 * @date   Jan 2016
 */


#include "DomeStatus.h"
#include "DomeMysql.h"

/// Helper function that adds a filesystem to the list and its corresponding server to the server list
/// Filesystems so far can't be deleted without a restart
int DomeStatus::addFilesystem(DomeFsInfo &fs) {
}

/// Helper function that reloads all the filesystems from the DB
int DomeStatus::loadFilesystems() {
  DomeMySql sql;
  
  return sql.getFilesystems(*this);
}

/// Helper function that reloads all the quotas from the DB
int DomeStatus::loadQuotatokens() {
  DomeMySql sql;
  
  return sql.getSpacesQuotas(*this);
}


int DomeStatus::tick() {
  checksumq->tick();
  filepullq->tick();
}

