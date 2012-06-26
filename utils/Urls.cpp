/// @file   common/Uris.cpp
/// @brief  Common methods and functions for URI's.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <dmlite/cpp/utils/dm_urls.h>

Url dmlite::splitUrl(const std::string& uri)
{
  regex_t     regexp;
  regmatch_t  matches[8];
  const char *p = uri.c_str();
  Url         parsed;

  // Compile the first time
  regcomp(&regexp,
          "(([[:alnum:]]+):/{2})?([[:alnum:]][-_[:alnum:]]*(\\.[-_[:alnum:]]+)*)?(:[[:digit:]]*)?([^?]*)?(.*)",
          REG_EXTENDED | REG_ICASE);

  // Match and extract
  memset(matches, 0, sizeof(matches));
  if (regexec(&regexp, p, 8, matches, 0) == 0) {
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

    // Path
    len = matches[6].rm_eo - matches[6].rm_so;
    memcpy(parsed.path, p + matches[6].rm_so, len);
    parsed.path[len] = '\0';
    
    // Query
    len = matches[7].rm_eo - matches[7].rm_so;
    if (len > 0)
      strncpy(parsed.query, p + matches[7].rm_so + 1, QUERY_MAX);
    else
      parsed.query[0] = '\0';
  }

  regfree(&regexp);
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



char* dmlite::normalizePath(char *path)
{
  int   i = 0;
  char *p = path;

  while (*p != '\0') {
    path[i] = *p;
    ++i;
    if (*p == '/') {
      while (*p == '/')
        ++p;
    }
    else {
      ++p;
    }
  }
  path[i] = '\0';
  return path;
}
