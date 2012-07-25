/** @file   c/any.cpp
 *  @brief  Opaque handler to pass different types of values to the API.
 *  @author Alejandro Álvarez Ayllon <aalvarez@cern.ch>
 *  @note   Basically it wraps boost::any and dmlite::Extensible.
 */
#include <cstring>
#include <dmlite/c/any.h>
#include <dmlite/cpp/utils/extensible.h>
#include <string>
#include <vector>
#include "Private.h"



dmlite_any* dmlite_any_new_string(const char* str)
{
  dmlite_any* any = new dmlite_any();
  any->value = std::string(str);
  return any;
}



dmlite_any* dmlite_any_new_string_array(unsigned n, const char** strv)
{
  dmlite_any* any = new dmlite_any();
  std::vector<boost::any> av;
  
  for (unsigned i = 0; i < n; ++i)
    av.push_back(std::string(strv[i]));
  
  any->value = av;
  return any;
}



dmlite_any* dmlite_any_new_long(long l)
{
  dmlite_any* any = new dmlite_any();
  any->value = l;
  return any;
}



dmlite_any* dmlite_any_new_long_array(unsigned n, long* lv)
{
  dmlite_any* any = new dmlite_any();
  std::vector<boost::any> av;
  
  for (unsigned i = 0; i < n; ++i)
    av.push_back(lv[i]);
  
  any->value = av;
  return any;
}



void dmlite_any_free(dmlite_any* any)
{
  delete any;
}



void dmlite_any_to_string(const dmlite_any* any, char* buffer, size_t bsize)
{
  std::string str;
  
  try {
    str = dmlite::Extensible::anyToString(any->value);
    std::strncpy(buffer, str.c_str(), bsize);
  }
  catch (...) {
    buffer[0] = '\0';
  }
}



long dmlite_any_to_long(const dmlite_any* any)
{
  try {
    return dmlite::Extensible::anyToLong(any->value);
  }
  catch (...) {
    return 0;
  }
}



dmlite_any_dict* dmlite_any_dict_new()
{
  return new dmlite_any_dict();
}


void dmlite_any_dict_free(dmlite_any_dict* d)
{
  delete d;
}



void dmlite_any_dict_insert(dmlite_any_dict* d, const char* k, const dmlite_any* v)
{
  d->extensible[k] = v->value;
}



unsigned long dmlite_any_dict_count(const dmlite_any_dict* d)
{
  return d->extensible.size();
}



dmlite_any* dmlite_any_dict_get(const dmlite_any_dict* d, const char* k)
{
  if (!d->extensible.hasField(k))
    return NULL;

  dmlite_any* any;
  any = new dmlite_any();
  any->value = d->extensible[k];
  return any;
}



void dmlite_any_dict_to_json(const dmlite_any_dict* d, char* buffer, size_t bsize)
{
  strncpy(buffer, d->extensible.serialize().c_str(), bsize);
}



dmlite_any_dict* dmlite_any_dict_from_json(const char* json)
{
  dmlite_any_dict* dict;
  dict = new dmlite_any_dict();
  dict->extensible.deserialize(json);
  return dict;
}
