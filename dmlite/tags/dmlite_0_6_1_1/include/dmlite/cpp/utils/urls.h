/// @file   include/dmlite/cpp/utils/urls.h
/// @brief  Common methods and functions for URL and path.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_URLS_H
#define DMLITE_CPP_UTILS_URLS_H

#include <string>
#include <vector>
#include "extensible.h"

namespace dmlite {
  
  struct Url {
    std::string scheme;
    std::string domain;
    unsigned    port;
    std::string path;
    Extensible  query;
    
    Url() throw();
    explicit Url(const std::string& url) throw ();
    
    // Operators
    bool operator == (const Url&) const;
    bool operator != (const Url&) const;
    bool operator <  (const Url&) const;
    bool operator >  (const Url&) const;
    
    std::string queryToString(void) const;
    void        queryFromString(const std::string& str);


    std::string toString(void) const;

    /// Split a path into a list of components.
    /// @param path The path to split.
    /// @return     A list with the extracted components.
    static std::vector<std::string> splitPath(const std::string& path) throw ();
    
    /// Build a path from a list of components
    static std::string joinPath(const std::vector<std::string>& components) throw();
    
    /// Remove multiple slashes.
    static std::string normalizePath(const std::string& path) throw ();
  };
};

#endif // DMLITE_CPP_UTILS_URLS_H
