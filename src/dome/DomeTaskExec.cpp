
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



/** @file   DomeTaskExec.cpp
 * @brief  A class that spawns commands that perform actions
 * @author Fabrizio Furano
 * @author Andrea Manzi
 * @date   Dec 2015
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include "DomeLog.h"
#include "utils/Config.hh"
#include "DomeTaskExec.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>

using namespace boost;
using namespace std;



void taskfunc(DomeTaskExec *inst, int key) {
  
  Log(Logger::Lvl4, domelogmask, "taskfunc", "Starting task " << key << " on instance " << inst->instance);
  
  if (inst) {
    map <int, DomeTask *>::iterator i = inst->tasks.find(key);
    if ( i != inst->tasks.end() ) {
	    Log(Logger::Lvl3, domelogmask, "taskfunc", "Found task " << key << " on instance " << inst->instance);
	    inst->run(*i->second);
	    Log(Logger::Lvl3, domelogmask, "taskfunc", "Finished task " << key << " on instance " << inst->instance);
	    return;
   }
    
  }  
  Err("taskfunc", "Cannot start task " << key << " on instance " << inst->instance);
  
  
}

// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------

DomeTask::DomeTask(): finished(false) {
  starttime = time(0);
  endtime = 0;
}

DomeTask::~DomeTask() {
}


int DomeTask::waitFinished(int sectmout) {
  const char *fname = "DomeTaskExec::waitFinished";
  
  Log(Logger::Lvl4, domelogmask, fname, "Checking task termination. Key: " << key << " cmd: " << cmd << " finished: " << finished);
  
  system_time const timelimit = get_system_time() + posix_time::seconds(sectmout);
  Log(Logger::Lvl4, domelogmask, fname, "Time: " << get_system_time());
  Log(Logger::Lvl4, domelogmask, fname, "TimeLimit: " << timelimit);
  
  { 
    scoped_lock lck (*this);
    while(!finished && get_system_time() < timelimit) {
         Log(Logger::Lvl4, domelogmask, fname, "Task not finished at time " << get_system_time());
	condvar.wait(lck);
	}
  }
  // We are here either if timeout or something happened
  
  if (finished) {
    Log(Logger::Lvl3, domelogmask, fname, "Finished task. Key: " << key << " cmd: " << cmd);
    return 0;
  }
  
  Log(Logger::Lvl3, domelogmask, fname, "Still running task. Key: " << key << " cmd: " << cmd);
  
  return 1;
  
}


// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
DomeTaskExec::DomeTaskExec() {}

DomeTaskExec::~DomeTaskExec() {}

/// Taken from here and slightly modified.
/// https://sites.google.com/site/williamedwardscoder/popen3
int DomeTaskExec::popen3(int fd[3],  const char *cmd) {
  int i, e;
  int p[3][2];
  pid_t pid;
  //
  // set all the FDs to invalid
  for(i=0; i<3; i++)
    p[i][0] = p[i][1] = -1;
  
  // create the pipes
  for(int i=0; i<3; i++) 
    if(pipe(p[i]))
      goto error;
    
    // and fork
    pid = fork();
  
  if(-1 == pid)
    goto error;
  
  // in the parent?
  if(pid) {
    // parent
    fd[STDIN_FILENO] = p[STDIN_FILENO][1];
    close(p[STDIN_FILENO][0]);
    
    fd[STDOUT_FILENO] = p[STDOUT_FILENO][0];
    close(p[STDOUT_FILENO][1]);
    
    fd[STDERR_FILENO] = p[STDERR_FILENO][0];
    close(p[STDERR_FILENO][1]);
    
    // success
    return 0;
    
  } else {
    // child
    while ( (dup2(p[STDIN_FILENO][0],STDIN_FILENO) == -1) && (errno == EINTR) ) {};
    close(p[STDIN_FILENO][1]);
    
    while ( (dup2(p[STDOUT_FILENO][1],STDOUT_FILENO) == -1) && (errno == EINTR) ) {};
    close(p[STDOUT_FILENO][0]);
    
    while ( (dup2(p[STDERR_FILENO][1],STDERR_FILENO) == -1) && (errno == EINTR) ) {};
    close(p[STDERR_FILENO][0]);
    
    // here we try and run it
    char *const parmList[] = {strdup(cmd), NULL};
    execv(cmd,parmList);
    
    // if we are here, then we failed to launch our program
    Err("popen3", "Cannot launch cmd: " << cmd);
    fprintf(stderr," \"%s\"\n",cmd);
    _exit(EXIT_FAILURE);
  }
  
  
  error:
  
  // preserve original error
  e = errno;
  for(i=0; i<3; i++) {
    close(p[i][0]);
    close(p[i][1]);
  }
  errno = e;
  return -1;
}

void DomeTaskExec::run(DomeTask &task) {
  
  const char *c = task.cmd.c_str();
  Log(Logger::Lvl3, domelogmask, "run", "Starting command: " << c) ;
  //start time
  {
     boost::lock_guard<DomeTask> l(task);
     time(&task.starttime);
  }
  
  int r = popen3((int *)task.fd, c);
  
  /// It is then possible for the parent process to read the output of the child process from file descriptor
  char buffer[4096];
  while (1) {
    ssize_t count = read(task.fd[STDOUT_FILENO], buffer, sizeof(buffer));
    if (count == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        Err("popen3", "Cannot get output of cmd: " << task.cmd.c_str());
        break;
      }
      
    }
    
    if (count == 0) {
      Log(Logger::Lvl4, domelogmask, "run", "No Stdout") ;
      break;
    }
    else {
      boost::lock_guard<DomeTask> l(task);
      task.stdout.append(buffer, count);
      Log(Logger::Lvl4, domelogmask, "run", "Stdout: " << task.stdout.c_str()) ;
    }
      
  }
  
  for(int i=0; i<3; i++) 
    close(task.fd[i]);
  
  {
    boost::lock_guard<DomeTask> l(task);
    task.finished = true;
    //endtime
    time(&task.endtime);
    task.notifyAll();
  }
  
  
  onTaskCompleted(task);
}


int DomeTaskExec::submitCmd(string cmd) {
   DomeTask * task = new DomeTask();
   task->cmd= cmd;
   task->key = ++taskcnt;// TO DO concurrency check
   tasks.insert( std::pair<int,DomeTask*>(task->key,task));
   taskfunc(this,taskcnt);
   return taskcnt;
}

int DomeTaskExec::waitResult(int taskID, int tmout) {
  const char *fname = "DomeTaskExec::waitFinished";
  
  {
    map <int, DomeTask *>::iterator i = tasks.find(taskID);
    if ( i != tasks.end() ) {
           Log(Logger::Lvl4, domelogmask, "waitResult", "Found task " << taskID);
	   i->second->waitFinished(tmout);
	   if (i->second->finished) return 0;
    
    }else {
 	  Log(Logger::Lvl4, domelogmask, "waitResult", "Task with ID " << taskID << " not found");
	  return 1;
	}
  }
  return 1;
}

void DomeTaskExec::tick() {
   Log(Logger::Lvl4, domelogmask, "tick", "tick");
   map <int, DomeTask *>::iterator i;
    for( i = tasks.begin(); i != tasks.end(); ++i ) {
            Log(Logger::Lvl3, domelogmask, "tick", "Found task " << i->first << " with command " << i->second->cmd.c_str());
   }
   //should clean the long lasting tasks and the one completed since more thant 24 
}

void DomeTaskExec::onTaskRunning(DomeTask &task) {}
void DomeTaskExec::onTaskCompleted(DomeTask &task) {}
