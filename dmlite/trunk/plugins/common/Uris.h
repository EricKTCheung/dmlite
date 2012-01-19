/// @file   plugins/common/Uris.h
/// @brief  Common methods and functions for URI's.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef URIS_H
#define	URIS_H

#include <dmlite/dm_types.h>
#include <list>
#include <string>

namespace dmlite {

/// Split a turl into host and path, removing the protocol.
/// @param uri The turl to split.
/// @return    The parsed URI.
Uri splitUri(const std::string& uri);

/// Split a path into a list of components.
/// @param path The path to split.
/// @return     A list with the extracted components.
std::list<std::string> splitPath(const std::string& path);

};


#endif	// URIS_H
