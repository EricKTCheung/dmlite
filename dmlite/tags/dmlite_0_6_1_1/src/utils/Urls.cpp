/// @file   utils/Urls.cpp
/// @brief  Common methods and functions for URI's.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <cstdlib>
#include <cstring>
#include <dmlite/cpp/utils/urls.h>
#include <iomanip>
#include <sstream>

using namespace dmlite;



Url::Url() throw (): port(0)
{
}



Url::Url(const std::string& url) throw (): port(0) 
{
  boost::regex regexp("(([[:alnum:]]+):/{2})?([[:alnum:]][-_[:alnum:]]*(\\.[-_[:alnum:]]+)*)?(:[[:digit:]]*)?(:)?([^?]*)?(.*)",
                      boost::regex::extended);
  boost::smatch what;
  
  if (boost::regex_match(url, what, regexp, boost::match_perl)) {
    this->scheme = what[2];
    this->domain = what[3];
    
    std::string portStr(what[5]);
    if (portStr.length() > 1)
      this->port = std::atol(portStr.c_str() + 1);
    
    this->path   = what[7];

    this->queryFromString(what[8]);
  }
}



bool Url::operator == (const Url& u) const
{
  return this->domain == u.domain &&
         this->path   == u.path &&
         this->port   == u.port &&
         this->query  == u.query &&
         this->scheme == u.scheme;
}



bool Url::operator != (const Url& u) const
{
  return !(*this == u);
}



bool Url::operator <  (const Url& u) const
{
  if (this->scheme < u.scheme)
    return true;
  else if (this->scheme > u.scheme)
    return false;

  if (this->domain < u.domain)
    return true;
  if (this->domain > u.domain)
    return false;

  if (this->port < u.port)
    return true;
  else if (this->port > u.port)
    return false;

  if (this->path < u.path)
    return true;
  else if (this->path > u.path)
    return false;

  if (this->query < u.query)
    return true;


  return false;
}



bool Url::operator >  (const Url& u) const
{
  return (*this < u);
}



static std::string urlencode(const std::string& str)
{
  std::ostringstream stream;
  std::string::const_iterator i;

  for (i = str.begin(); i != str.end(); ++i) {
    std::string::value_type c = *i;
    if (isalnum(c)) {
      stream << c;
    }
    else {
      switch (c) {
        case '.': case '-': case '~': case '_':
          stream << c;
          break;
        default:
          stream << '%' << std::setw(2) << std::setfill('0')
                 << std::uppercase << std::hex
                 << (int)static_cast<unsigned char>(c);
      }
    }
  }


  return stream.str();
}



std::string Url::queryToString(void) const
{
  Extensible::const_iterator i;
  std::ostringstream queryStr;

  for (i = query.begin(); i != query.end(); ++i) {
    queryStr << i->first;
    if (i->second.type() != typeid(bool) || Extensible::anyToBoolean(i->second) == false)
      queryStr << "=" << urlencode(Extensible::anyToString(i->second));
    queryStr << "&";
  }

  std::string str = queryStr.str();
  return str.substr(0, str.size() - 1);
}



static unsigned int extractHex(std::istream& istream)
{
  std::string::value_type c[3];

  istream.read(c, 2);
  if (istream.fail() || istream.eof())
    return '%';
  c[2] = '\0';

  return ::strtol(c, NULL, 16);
}



static std::string urldecode(const std::string& str)
{
  std::ostringstream ostream;
  std::istringstream istream(str);
  std::string::const_iterator i;
  std::string::value_type c;

  istream >> c;
  while (!istream.fail() && !istream.eof()) {
    switch (c) {
      case '+':
        ostream << ' ';
        break;
      case '%':
        ostream << static_cast<unsigned char>(extractHex(istream));
        break;
      default:
        ostream << c;
    }
    istream >> c;
  }

  return ostream.str();
}



void Url::queryFromString(const std::string& str)
{
  if (str.empty())
    return;

  std::string queryStr;
  if (str[0] == '?')
    queryStr = str.substr(1);
  else
    queryStr = str;

  typedef boost::split_iterator<std::string::iterator> string_split_iterator;

  for(string_split_iterator i = boost::algorithm::make_split_iterator(queryStr, boost::algorithm::first_finder("&", boost::algorithm::is_iequal()));
     i != string_split_iterator(); ++i) {
    std::vector<std::string> pair;
    boost::algorithm::split(pair, *i, boost::algorithm::is_any_of("="));

    if (pair.size() == 1)
      this->query[pair[0]] = true;
    else
      this->query[pair[0]] = urldecode(pair[1]);
  }
}



std::string Url::toString(void) const
{
  std::ostringstream str;
  if (!scheme.empty())
    str << scheme << "://";
  if (!domain.empty())
    str << domain;
  if (port > 0)
    str << ":" << port;
  if (scheme.empty() && !domain.empty())
    str << ":";
  str << path;

  if (this->query.size() > 0) {
    str << "?";
    str << queryToString();
  }

  return str.str();
}



std::vector<std::string> Url::splitPath(const std::string& path) throw()
{
  std::vector<std::string> components;
  size_t s, e;
  
  if (!path.empty() && path[0] == '/')
    components.push_back("/");

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



std::string Url::joinPath(const std::vector<std::string>& components) throw()
{
  std::vector<std::string>::const_iterator i;
  std::string path;

  for (i = components.begin(); i != components.end(); ++i) {
    if (*i != "/")
      path += *i + "/";
    else
      path += "/";
  }
  
  if (!path.empty())
    path.erase(--path.end());
    
  return path;
}



std::string Url::normalizePath(const std::string& path) throw ()
{
  std::vector<std::string> components = Url::splitPath(path);
  std::string              result;
  
  result.reserve(path.length());
  
  unsigned i;
  if (components[0] == "/") {
    i      = 1;
    result = "/";
  } else {
    i = 0;
  }
  
  for ( ; i < components.size(); ++i) {
    result.append(components[i]);
    if (i < components.size() - 1)
      result.append("/");
  }
  
  if (components.size() > 1 && path[path.length() - 1] == '/')
    result.append("/");
  
  return result;
}

