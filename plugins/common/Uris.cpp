/// @file   plugins/common/Uris.cpp
/// @brief  Common methods and functions for URI's.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <assert.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>

#include "Uris.h"

Uri dmlite::splitUri(const std::string& uri)
{
  regex_t     regexp;
  regmatch_t  matches[7];
  const char *p = uri.c_str();
  Uri         parsed;

  // Compile the first time
  assert(regcomp(&regexp,
                 "(([[:alnum:]]+):/{2})?([[:alnum:]]+(\\.[[:alnum:]]+)*)?(:[[:digit:]]*)?(/.*)?",
                 REG_EXTENDED | REG_ICASE) == 0);

  // Match and extract
  if (regexec(&regexp, p, 7, matches, 0) == 0) {
    int len;

    // Scheme
    len = matches[2].rm_eo - matches[2].rm_so;
    memcpy(parsed.scheme, p + matches[2].rm_so, len);
    parsed.scheme[len] = '\0';

    // Host
    len = matches[3].rm_eo - matches[3].rm_so;
    memcpy(parsed.host, p + matches[3].rm_so, len);
    parsed.host[len] = '\0';

    // Port
    len = matches[5].rm_eo - matches[5].rm_so;
    if (len > 0)
      parsed.port = atoi(p + matches[5].rm_so + 1);
    else
      parsed.port = 0;

    // Rest
    strncpy(parsed.path, p + matches[6].rm_so, PATH_MAX);
  }

  return parsed;
}


std::list<std::string> dmlite::splitPath(const std::string& path)
{
  std::list<std::string> components;
  size_t s, e;

  s = path.find_first_not_of('/');
  while (s != std::string::npos) {
    e = path.find('/', s);
    if (e != std::string::npos) {
      components.push_back(path.substr(s, e - s));
      s = path.find_first_not_of('/', e);
    }
    else {
      components.push_back(path.substr(s));
      s = e;
    }
  }

  return components;
}
