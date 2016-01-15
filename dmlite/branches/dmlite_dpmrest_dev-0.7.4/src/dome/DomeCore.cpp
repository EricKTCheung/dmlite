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



/** @file   DpmrCore.cpp
 * @brief  The main class where operation requests are applied to the status
 * @author Fabrizio Furano
 * @date   Dec 2015
 */


#include "DomeCore.h"








int DpmrCore::init(char *cfgfile) {
  
  std::vector<int> limits;
  limits.push_back(1);
  
  status.checksumq = new GenPrioQueue(30, limits);
  status.filepullq = new GenPrioQueue(30, limits);
      
  return 0;
}

int DpmrCore::tick() {
  
}










  int DpmrCore::dpmr_put(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_putdone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_getspaceinfo(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_getquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_setquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_delquotatoken(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_chksum(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_chksumdone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_ispullable(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_get(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_pulldone(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_statpool(DpmrReq &req, FCGX_Request &request) {};

  
  int DpmrCore::dpmr_pull(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_dochksum(DpmrReq &req, FCGX_Request &request) {};
  int DpmrCore::dpmr_statfs(DpmrReq &req, FCGX_Request &request) {};











