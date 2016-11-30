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



/** @file   DomeMain.cpp
 * @brief  main() function for dome
 * @author Fabrizio Furano
 * @date   Jan 2016
 */

#include "DomeLog.h"
#include "DomeCore.h"
#include <iostream>
#include <stdlib.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <unistd.h>


using namespace std;

int main(int argc, char **argv) {
  std::string cfgfile;

  if (argc < 2) {
    char *c = getenv("DOME_CFGFILE");

    if (!c) {
      cerr << "Usage: dome <config file>" << endl;
      cerr << "  or set the envvar $DOME_CFGFILE" << endl;
      return -1;
    }
    cfgfile = c;
  }
  else cfgfile = argv[1];

  cout << "Welcome to dome" << endl;
  cout << "Cfg file: " << cfgfile << endl;

  if(!getenv("DOME_NODAEMON")) {
    daemon(0, 0);
  }

  domelogmask = Logger::get()->getMask(domelogname);
  DomeCore core;

  if ( core.init(cfgfile.c_str()) ) {
    cout << "Cannot start :-(" << endl;
    return -1;
  }


  while(1) {
    sleep(1);
  }

  cout << "yay" << endl;

}
