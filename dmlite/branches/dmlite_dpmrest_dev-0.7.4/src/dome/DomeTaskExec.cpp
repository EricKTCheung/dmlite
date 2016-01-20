
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



/** @file   DpmrTaskExec.cpp
 * @brief  A class that spawns commands that perform actions
 * @author Fabrizio Furano
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



void taskfunc(DpmrTaskExec *inst, int key) {
  
  Log(Logger::Lvl4, domelogmask, "taskfunc", "Starting task " << key << " on instance " << inst);
  
  if (inst) {
    map <int, DpmrTask >::iterator i = inst->tasks.find(key);
  
    if ( i != inst->tasks.end() ) {
      inst->run( i->second );
      Log(Logger::Lvl3, domelogmask, "taskfunc", "Finished task " << key << " on instance " << inst);
      return;
    }
  
    
  }  
  Err("taskfunc", "Cannot start task " << key << " on instance " << inst);
  
  
}










// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------

DpmrTask::DpmrTask(): finished(false) {
  starttime = time(0);
  endtime = 0;
}

int DpmrTask::waitFinished(int sectmout) {
  const char *fname = "DpmrTaskExec::waitFinished";
  
  Log(Logger::Lvl4, domelogmask, fname, "Checking task termination. Key: " << key << " cmd: " << cmd);
  
  system_time const timelimit = get_system_time() + posix_time::seconds(sectmout);
  
  {
    unique_lock<mutex> lck(*this);
  while (get_system_time() < timelimit) {
    system_time const timeout = get_system_time() + posix_time::seconds(1);
    if (!condvar.timed_wait(lck, timelimit))
      continue; // timeout
      
      break;
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

DpmrTaskExec::DpmrTaskExec() {};
DpmrTaskExec::~DpmrTaskExec() {};
  
/// Taken from here and slightly modified.
/// https://sites.google.com/site/williamedwardscoder/popen3
int DpmrTaskExec::popen3(int fd[3], const char **const cmd) {
  int i, e;
  int p[3][2];
  pid_t pid;
  
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
    execv(*cmd,const_cast<char*const*>(cmd));
    
    // if we are there, then we failed to launch our program
    Err("popen3", "Cannot launch cmd: " << cmd);
    fprintf(stderr," \"%s\"\n",*cmd);
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

void DpmrTaskExec::run(DpmrTask &task) {
  
  const char *c = task.cmd.c_str();
  int r = popen3((int *)task.fd, &c);
  
  /// It is then possible for the parent process to read the output of the child process from file descriptor
  char buffer[4096];
  while (1) {
    ssize_t count = read(task.fd[STDOUT_FILENO], buffer, sizeof(buffer));
    if (count == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        Err("popen3", "Cannot get output of cmd: " << task.cmd);
        break;
      }
      
    }
    
    if (count == 0)
      break;
    else {
      boost::lock_guard<DpmrTask> l(task);
      task.stdout.append(buffer, count);
    }
      
  }
  
  for(int i=0; i<3; i++) 
    close(task.fd[i]);

  
  {
    boost::lock_guard<DpmrTask> l(task);
    task.finished = true;
    task.notifyAll();
  }
  
  
  onTaskCompleted(task);
}



int DpmrTaskExec::submitCmd(const char *cmd) {
  
}

int DpmrTaskExec::waitResult(DpmrTask &task, int tmout) {
  const char *fname = "DpmrTaskExec::waitFinished";
  
  
  
  {
    boost::lock_guard<DpmrTask> l(task);
    
    (task).waitFinished(tmout);
    
    if (task.finished) return 0;
    
  }
  
  return 1;
}

void DpmrTaskExec::tick() {}

void DpmrTaskExec::onTaskRunning(DpmrTask &task) {}


void DpmrTaskExec::onTaskCompleted(DpmrTask &task) {}