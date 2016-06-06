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

/** @file   DomeUtils.h
 * @brief  Small utilities used throughout dome
 * @author Georgios Bitzes
 * @date   Feb 2016
 */

#ifndef DOMEUTILS_H
#define DOMEUTILS_H

#include <string>
#include <vector>

namespace DomeUtils {

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

inline std::string join(const std::string &separator, const std::vector<std::string> &arr) {
  if(arr.empty()) return std::string();
  
  std::stringstream ss;
  for(size_t i = 0; i < arr.size()-1; i++) {
    ss << arr[i];
    ss << separator;
  }
  ss << arr[arr.size()-1];
  return ss.str();
}

inline std::vector<std::string> split(std::string data, std::string token) {
    std::vector<std::string> output;
    size_t pos = std::string::npos;
    do {
        pos = data.find(token);
        output.push_back(data.substr(0, pos));
        if(std::string::npos != pos)
            data = data.substr(pos + token.size());
    } while (std::string::npos != pos);
    return output;
}

inline std::string bool_to_str(bool b) {
  if(b) return "true";
  else return "false";
}

inline bool str_to_bool(const std::string &str) {
  bool value = false;

  if(str == "false" || str == "0" || str == "no") {
    value = false;
  } else if(str == "true" || str == "1" || str == "yes") {
    value = true;
  }
  return value;
}

inline std::string pfn_from_rfio_syntax(const std::string &rfn) {
  size_t pos = rfn.find(":");
  if(pos == std::string::npos)
    return rfn;
  return rfn.substr(pos+1, rfn.size());
}

inline std::string server_from_rfio_syntax(const std::string &rfn) {
  size_t pos = rfn.find(":");
  if(pos == std::string::npos)
    return rfn;
  return rfn.substr(0, pos);
}

inline std::string unescape_forward_slashes(const std::string &str) {
  std::ostringstream ss;
  for(size_t i = 0; i < str.size(); i++) {
    if(i != str.size()-1 && str[i] == '\\' && str[i+1] == '/') {
      ss << "/";
      i++;
    }
    else {
      ss << str[i];
    }
  }
  return ss.str();
}

}


#endif
