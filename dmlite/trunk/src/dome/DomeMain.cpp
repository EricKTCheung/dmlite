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





  std::string response = "{\"title\":\"Foo\",\"items\":[{\"id\":123,\"name\":\"test1\"},{\"id\":456,\"name\":\"test2\"}]}";

        boost::property_tree::ptree pt;
        std::stringstream ss; ss << response;
        boost::property_tree::read_json(ss, pt);









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

  domelogmask = Logger::get()->getMask(domelogname);
  DomeCore core;

  if ( core.init(cfgfile.c_str()) ) {
    cout << "Cannot start :-(" << endl;
    daemon(0, 0);
    return -1;
  }

  daemon(0, 0);

  while(1) {
    sleep(1);
  }

  cout << "yay" << endl;

}
