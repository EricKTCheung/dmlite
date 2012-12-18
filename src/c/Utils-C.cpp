/// @file   c/PluginManager-C.cpp
/// @brief  C wrapper for dmlite::PluginManager.
/// @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#include <cstring>
#include <dmlite/c/utils.h>
#include <dmlite/cpp/utils/security.h>
#include <dmlite/cpp/utils/urls.h>
#include "Private.h"



dmlite_url* dmlite_url_new(void)
{
  dmlite_url* newUrl = new dmlite_url();
  ::memset(newUrl, 0, sizeof(dmlite_url));
  newUrl->query = dmlite_any_dict_new();
  return newUrl;
}



dmlite_url* dmlite_parse_url(const char* source)
{
  dmlite_url* newUrl = dmlite_url_new();

  dmlite::Url url(source);
  
  std::strncpy(newUrl->domain, url.domain.c_str(), sizeof(newUrl->domain));
  std::strncpy(newUrl->path,   url.path.c_str(),   sizeof(newUrl->path));
  std::strncpy(newUrl->scheme, url.scheme.c_str(), sizeof(newUrl->scheme));
  newUrl->port = url.port;
  newUrl->query->extensible = url.query;

  return newUrl;
}



void dmlite_url_free(dmlite_url* url)
{
  dmlite_any_dict_free(url->query);
  delete url;
}



char* dmlite_url_serialize(dmlite_url* url, char* buffer, size_t bsize)
{
  if (!url || !buffer || bsize == 0) return NULL;

  dmlite::Url urlpp;
  urlpp.domain = url->domain;
  urlpp.path   = url->path;
  urlpp.port   = url->port;
  urlpp.scheme = url->scheme;
  if (url->query)
    urlpp.query  = url->query->extensible;

  strncpy(buffer, urlpp.toString().c_str(), bsize);
  return buffer;
}



void dmlite_serialize_acls(unsigned nEntries, dmlite_aclentry* acl,
                           char* buffer, size_t bsize)
{
  dmlite::Acl aclpp;
  
  for (unsigned i = 0; i < nEntries; ++i) {
    dmlite::AclEntry e;
    
    e.id   = acl[i].id;
    e.perm = acl[i].perm;
    e.type = acl[i].type;
    
    aclpp.push_back(e);
  }
  
  std::strncpy(buffer, aclpp.serialize().c_str(), bsize);
}

void dmlite_deserialize_acl(const char* buffer, unsigned* nEntries,
                             dmlite_aclentry** acl)
{
  dmlite::Acl aclpp(buffer);
  *nEntries = aclpp.size();
  
  *acl = new dmlite_aclentry[*nEntries];
  
  for (unsigned i = 0; i < *nEntries; ++i) {
    (*acl)[i].id   = aclpp[i].id;
    (*acl)[i].perm = aclpp[i].perm;
    (*acl)[i].type = aclpp[i].type;
  }
}



void dmlite_acl_free(unsigned nEntries, struct dmlite_aclentry* acl)
{
  delete [] acl;
}
