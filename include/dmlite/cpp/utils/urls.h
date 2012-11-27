/// @file   include/dmlite/cpp/utils/urls.h
/// @brief  Common methods and functions for URL and path.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_UTILS_URLS_H
#define DMLITE_CPP_UTILS_URLS_H

#include <string>
#include <vector>

namespace dmlite {
  
  struct Url {
    struct QueryField {
      std::string field;
      std::string value;

      explicit QueryField(const std::string& field);
      QueryField(const std::string& field, const std::string& value);
      QueryField(const QueryField&);

      bool operator == (const QueryField& b) const;
      bool operator != (const QueryField& b) const;
      bool operator <  (const QueryField& b) const;
      bool operator >  (const QueryField& b) const;
    };

    std::string scheme;
    std::string domain;
    unsigned    port;
    std::string path;
    std::vector<QueryField> query;
    
    explicit Url(const std::string& url) throw ();
    
    // Operators
    bool operator == (const Url&) const;
    bool operator != (const Url&) const;
    bool operator <  (const Url&) const;
    bool operator >  (const Url&) const;
    
    // To string
    std::string queryToString(void) const;
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
