/// @file   include/dmlite/common/Urls.h
/// @brief  Common methods and functions for URL and path.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef URLS_H
#define	URLS_H

#include <list>
#include <string>
#include "../../common/dm_types.h"

namespace dmlite {

/// Split a turl into host and path, removing the protocol.
/// @param uri The turl to split.
/// @return    The parsed URI.
Url splitUrl(const std::string& url);

/// Split a path into a list of components.
/// @param path The path to split.
/// @return     A list with the extracted components.
std::list<std::string> splitPath(const std::string& path);

/// Remove multiple slashes.
char* normalizePath(char *path);

};


#endif	// URLS_H
