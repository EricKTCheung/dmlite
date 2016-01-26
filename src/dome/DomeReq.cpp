/*
 * Copyright 2015 <copyright holder> <email>
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
 

 
/** @file   DomeReq.cpp
 * @brief  A helper class that describes a request to dpmrest, with helpers to parse it
 * @author Fabrizio Furano
 * @date   Aug 2015
 */

#include "DomeReq.h"
#include "DomeLog.h"


DomeReq::DomeReq(FCGX_Request &request) {
  Log(Logger::Lvl4, domelogmask, domelogname, "Ctor");
  char *s;
  
  if ( (s = FCGX_GetParam("REQUEST_METHOD", request.envp)) )
    verb = s;
  if ( (s = FCGX_GetParam("PATH_INFO", request.envp)) )
    object = s;
  if ( (s = FCGX_GetParam("HTTP_CMD", request.envp)) )
    domecmd = s;

  
  
  //takeJSONbodyfields( request.what?! );
}
    

int DomeReq::takeJSONbodyfields(std::string &body) {
  
  return 0;
}


int DomeReq::getJSONbodyfields(std::string &body) {
  
  return 0;
}

