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



/** @file   DpmrStatus.h
 * @brief  A helper class that describes the internal status of the storage system, pools, disks, etc
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include <boost/thread.hpp>
#include <set>
#include "DomeGenQueue.h"

/// We describe a filesystem where to put data. We also keep dynamic information here, e.g. the free/used space
class DpmrFsInfo {

public:

  /// The logical pool this fs belongs to. This is just a string tag. A pool does not need anything more.
  std::string poolname;

  /// The server that hosts this filesystem
  std::string server;

  /// The private path of the filesystem inside its server
  std::string fs;

  /// Status of the filesystem
  int status;
  int weight;

  /// Free space, in bytes
  long long freespace;
  /// Total size of this filesystem
  long long physicalsize;
};


/// Information about a quota on a directory. This comes from the historical spacetoken information
class DpmrQuotatoken {
public:
  /// An ID
  int64_t rowid;

  /// Spacetoken human name
  std::string u_token;

  /// Total space of this quota or spacetoken
  int64_t t_space;

  /// Path prefix this structure is assigned to, in the quotatoken use case
  /// No trailing slash !
  std::string path;

};




/// Class that contains the internal status of the storage system, pools, disks, etc
/// Accesses must lock/unlock it for operations where other threads may write
class DpmrStatus: public boost::recursive_mutex {
public:
  /// Trivial store for filesystems information
  std::vector <DpmrFsInfo> fslist;

  /// Simple keyvalue store for prefix-based quotas, that
  /// represent a simplification of spacetokens
  /// The key is the prefix without trailing slashes
  std::map <std::string, DpmrQuotatoken> quotas;

  /// List of all the servers that are involved. This list is built dynamically
  /// when populating the filesystems
  std::set <std::string> servers;

  /// Helper function that adds a filesystem to the list and its corresponding server to the server list
  /// Filesystems so far can't be deleted without a restart
  int addFilesystem(DpmrFsInfo &fs);

  /// Helper function that reloads all the filesystems from the DB
  int loadFilesystems();

  /// Helper function that reloads all the quotas from the DB
  int loadQuotatokens();

  /// The queue holding checksum requests
  GenPrioQueue *checksumq;

  /// The queue holding file pull requests
  GenPrioQueue *filepullq;

  /// The status lives
  int tick();
};
