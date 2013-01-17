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

  /// To be used internally by the plug-ins that need to deal
  /// with the legacy-style stored checksums.
  /// @note AD => ADLER32
  /// @note CS => UNIXcksum (RFC 3230)
  /// @note MD => MD5 (RFC 3230)
  /// @note Any other is left as is
  std::string fullChecksumName(const std::string& cs);

  /// Inverse of fullChecksumName
  /// This should eventually disappear, once the backends can deal with
  /// full checksum names.
  std::string shortChecksumName(const std::string& cs);
};

#endif // DMLITE_CPP_UTILS_URLS_H
