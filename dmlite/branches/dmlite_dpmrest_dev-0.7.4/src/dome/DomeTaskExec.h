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



/** @file   DomeTaskExec.h
 * @brief  A class that spawns commands that perform actions
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include <boost/thread.hpp>
#include <signal.h>
#include <vector>
#include <string>
#include <algorithm>
#include <sstream>
#include <iterator>
#include <iostream>


class DomeTask: public boost::mutex {

protected:
    /// Threads waiting for result about this task will wait and synchronize here
    /// using something like
    /// boost::lock_guard< boost::mutex > l(workmutex);
    /// 
    boost::condition_variable condvar;
public:
  DomeTask();
  DomeTask(const DomeTask &o) {
    key = o.key;
    cmd = o.cmd;
    for(unsigned int i = 0; i < sizeof(parms); i++) parms[i] = NULL;
    resultcode = o.resultcode;
    starttime = o.starttime;
    endtime = o.endtime;
    finished = o.finished;
    fd[0] = 0; fd[1] = 0; fd[2] = 0;
    this->stdout = o.stdout;
  }
    
  ~DomeTask();
  int key;
  
  std::string cmd;
  const char *parms[64];

  int resultcode;
  
  time_t starttime, endtime;
  bool finished;
  
  int fd[3];
  pid_t pid;
  std::string stdout;

  /// Split che command string into the single parms
  void splitCmd();

  /// Wait until the task has finished or the timeout is expired
  int waitFinished(int tmout=5);
  
  void notifyAll() {
    condvar.notify_all();
  }

  
};


/// Allows to spawn commands, useful for checksum calculations or file pulling
/// The spawned commands are pollable, i.e. in a given moment it's possible to
/// know the list of commands that are still running.
/// Objects belonging to this class in general are created in the disk nodes,
/// e.g. for running checksums or file copies and pulls
class DomeTaskExec: public boost::recursive_mutex {
  
public:
  DomeTaskExec();
  ~DomeTaskExec(); 
  std::string instance;
  /// Executes a command. Returns a positive integer as a key to reference
  /// the execution status and the result
  /// The mechanics is that a detached thread is started. This guy invokes popen3
  /// and blocks waiting for the process to end. Upon end it updates the corresponding
  /// instance of DomeTask with the result and the stdout
  int submitCmd(std::string cmd);
	
	
  /// Executes a command. Returns a positive integer as a key to reference
  //  the execution status and the result
  //  The mechanics is that a detached thread is started. This guy invokes popen3
  //  and blocks waiting for the process to end. Upon end it updates the corresponding
  //   instance of DomeTask with the result and the stdout
  //   -1 is returned in case of error in the submission
  int submitCmd(std::vector<std::string> &args);

  /// Split che command string into the single parms
  void assignCmd(DomeTask *task, std::vector<std::string> &args);
  
  /// Get the results of a task.
  /// Wait at max tmout seconds until the task finishes
  /// Return 0 if the task has finished and there is a result
  /// Return nonzero if the task is still running
  int waitResult(int taskID, int tmout=5);
  
  //kill a specific task given the id
  int killTask(int taskID);
 
  //get a DomeTask given the id ( mainly for testing)
  DomeTask* getTask(int taskID);
 
  /// Loops over all the tasks and:
  ///  - send a notification to the head node about all the processes that are running or that have finished
  ///  - garbage collect the task list.
  ///   - Task that are finished since long (e.g. 1 hour)
  ///   - Tasks that are stuck (e.g. 1 day)
  void tick();
  
protected:
  
  /// event for immediate notifications when a task finishes
  /// Subclasses can specialize this and apply app-dependent behavior to
  /// perform actions when something has finished running
  /// NOTE the signature. This passes copies of Task objects, not the originals
  virtual void onTaskCompleted(DomeTask &task);
  
  // event that notifies that a task is running
  // This event can be invoked multiple times during the life of a task
  /// NOTE the signature. This passes copies of Task objects, not the originals
  virtual void onTaskRunning(DomeTask &task);
private:

  int popen3(int fd[3], pid_t *pid,  const char ** argv );
  
  /// Used to create keys to be inserted into the map. This has to be treated modulo MAXINT or similar big number
  int taskcnt;
  /// This map works like a sparse array :-)
  std::map<int, DomeTask*> tasks;
  
  
  /// Here we invoke popen3
  /// and block waiting for the process to end. Upon end it updates the corresponding
  /// instance of DomeTask with the result and the stdout
  virtual void run(DomeTask &task);
  
  friend void taskfunc(DomeTaskExec *, int);

  //kill a specific task
  int killTask(DomeTask *task);
};
