
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
  pid = -1;

  // Initialize placeholders for individual parms for execv
  for (int i =0; i < 64; i++)
    parms[i] = NULL;
}

DomeTask::~DomeTask() {
for (int i =0; i < 64; i++)
    // Free the execv parms, if we need...
    if (parms[i]) free((void *)parms[i]);
    else break;
}


void DomeTask::splitCmd()
{
  const char *tok;
  char *saveptr, *str = (char *)cmd.c_str();
  int i = 0;
  
  while ( (tok = strtok_r(str, " ", &saveptr)) ) {
    parms[i++] = strdup(tok);
    str = 0;
  }

  return;

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
int DomeTaskExec::popen3(int fd[3], pid_t *pid,  const  char ** argv) {
  int i, e;
  int p[3][2];
  //
  // set all the FDs to invalid
  for(i=0; i<3; i++)
    p[i][0] = p[i][1] = -1;
  
  // create the pipes
  for(int i=0; i<3; i++) 
    if(pipe(p[i]))
      goto error;
    
    // and fork
    *pid = fork();
  
  if(-1 == *pid)
    goto error;
  
  // in the parent?
  if(*pid) {
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
 
    execv(argv[0],(char **)argv);
    
    // if we are here, then we failed to launch our program
    Err("popen3", "Cannot launch cmd: " << argv[0]);
    fprintf(stderr," \"%s\"\n",argv[0]);
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
  
  Log(Logger::Lvl3, domelogmask, "run", "Starting command: " << task.cmd) ;
  //start time
  {
     boost::lock_guard<DomeTask> l(task);
     time(&task.starttime);
  }
  
  task.splitCmd();

  task.resultcode = popen3((int *)task.fd, &task.pid, task.parms);
  
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
      Log(Logger::Lvl4, domelogmask, "run", "Pid " << task.pid) ;
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
   {  
     boost::lock_guard<DomeTask> l(*task);
     task->key = ++taskcnt; 
     tasks.insert( std::pair<int,DomeTask*>(task->key,task));
   }
   boost::thread workerThread( taskfunc,this,taskcnt);
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

int DomeTaskExec::killTask(int taskID){
   map <int, DomeTask *>::iterator i = tasks.find(taskID);
    if ( i != tasks.end() ) {
           boost::lock_guard<DomeTask> l(*i->second);
           Log(Logger::Lvl4, domelogmask, "killTask", "Found task " << taskID);
	   if (i->second->finished){
		Log(Logger::Lvl4, domelogmask, "killTask", "Task " << taskID << " already finished");
		return 0;
	   } else if ( i->second->pid == -1) {
		Log(Logger::Lvl4, domelogmask, "killTask", "Task " << taskID << " not yet started");
		return 0;
	   } else {
		kill(i->second->pid, SIGKILL);
		Log(Logger::Lvl4, domelogmask, "killedTask", "Task " << taskID);
		return 0;
	   }
		
    }else {
          Log(Logger::Lvl4, domelogmask, "waitTask", "Task with ID " << taskID << " not found");
          return 1;
        }
  }


DomeTask* DomeTaskExec::getTask(int taskID) {
   map <int, DomeTask *>::iterator i = tasks.find(taskID);
   if ( i != tasks.end() ) 
   	return i->second;
   else 
	return NULL;
}



void DomeTaskExec::tick() {
   int maxruntime = CFG->GetLong("glb.task.maxrunningtime", 3600);
   int purgetime = CFG->GetLong("glb.task.purgetime", 3600);

   Log(Logger::Lvl4, domelogmask, "tick", "tick");
   map <int, DomeTask *>::iterator i;
    for( i = tasks.begin(); i != tasks.end(); ++i ) {
            Log(Logger::Lvl3, domelogmask, "tick", "Found task " << i->first << " with command " << i->second->cmd);
	    Log(Logger::Lvl3, domelogmask, "tick", "The status of the task is " << i->second->finished);
	    Log(Logger::Lvl3, domelogmask, "tick", "StartTime " << i->second->starttime << " EndTime " << i->second->endtime);
	    Log(Logger::Lvl3, domelogmask, "tick", "Pid " << i->second->pid << " resultcode " << i->second->resultcode);
 	    time_t timenow;
	    time(&timenow);

	    //lock
	    {
	       //boost::lock_guard<DomeTask> l(*i->second);
               //cannot delete task when locked TO CHECK
               
               if (!i->second->finished && ( (i->second->starttime< (timenow - (maxruntime*1000))))) {
			Log(Logger::Lvl3, domelogmask, "tick", "endtime " << i->second->endtime<< " timelimit " << (timenow - (maxruntime*1000)));
			//we kill the task
			killTask(i->first);
			Log(Logger::Lvl3, domelogmask, "tick", "Task with id  " << i->first << " exceed maxrunnngtime");
			Log(Logger::Lvl3, domelogmask, "tick", "Task killed ");
	       }
	
    	       //check if purgetime has exceeded and clean
    	       if (i->second->finished && ( i->second->endtime < (timenow - (purgetime*1000)))) {
			Log(Logger::Lvl3, domelogmask, "tick", "Task with id  " << i->first << " to purge");
			//delete the task
			delete i->second;
			//remove from map
			tasks.erase(i);
			Log(Logger::Lvl3, domelogmask, "tick", "Task with id  " << i->first << " purged");
		}
	    }
		
   }

}

void DomeTaskExec::onTaskRunning(DomeTask &task) {
             Log(Logger::Lvl3, domelogmask, "onTaskRunning", "task " << task.key << " with command " << task.cmd);    
	}
void DomeTaskExec::onTaskCompleted(DomeTask &task) {
	    Log(Logger::Lvl3, domelogmask, "onTaskCompleted", "task " << task.key << " with command " << task.cmd);
	}
