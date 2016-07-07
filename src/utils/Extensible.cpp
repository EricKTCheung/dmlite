/// @file    utils/Extensible.cpp
/// @brief   Extensible types (hold metadata).
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/version.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <ctype.h>
#include <dmlite/cpp/utils/extensible.h>
#include <iterator>
#include <sstream>

using namespace dmlite;

static inline bool compare_types(const std::type_info& a, const std::type_info& b)
{
#ifdef BOOST_AUX_ANY_TYPE_ID_NAME
  return (::strcmp(a.name(), b.name()) == 0);
#else
  return a == b;
#endif
}


// Check json.org to see the diagram
static void jsonifyString(const std::string& str, std::ostream& out)
{
  out << '"';

  std::string::const_iterator i;
  for (i = str.begin(); i != str.end(); ++i) {
    char c = *i;
    switch(c) {
      case '"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '/':
        out << "\\/";
        break;
      case '\b':
        out << "\\b";
        break;
      case '\f':
        out << "\\f";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (iscntrl(c))
          out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
        else
          out << c;
    }
  }

  out << '"';
}



static std::string serializeAny(const boost::any& value)
{
  unsigned    i;
  long long   intValue;
  double      doubleValue;
  std::string strValue;
  std::vector<boost::any> vValue;
  enum {kLong, kDouble, kString, kUnknown, kObj, kVector} whatIs = kUnknown;

  // String types are easy
  if (compare_types(value.type(), typeid(std::string))) {
    strValue = boost::any_cast<std::string>(value);
    whatIs = kString;
  }
  else if (compare_types(value.type(), typeid(const char*))) {
    strValue = boost::any_cast<const char*>(value);
    whatIs = kString;
  }
  else if (compare_types(value.type(), typeid(char*))) {
    strValue = boost::any_cast<char*>(value);
    whatIs = kString;
  }
  // Treat all integers as long
  else if (compare_types(value.type(), typeid(bool))) {
    bool b = boost::any_cast<bool>(value);
    return b ? "true" : "false";
  }
  else if (compare_types(value.type(), typeid(char))) {
    intValue = boost::any_cast<char>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(short))) {
    intValue = boost::any_cast<short>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(int))) {
    intValue = boost::any_cast<int>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(unsigned))) {
    intValue = boost::any_cast<unsigned>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(long))) {
    intValue = boost::any_cast<long>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(unsigned long))) {
    intValue = boost::any_cast<unsigned long>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(long long))) {
    intValue = boost::any_cast<long long>(value);
    whatIs = kLong;
  }
  else if (compare_types(value.type(), typeid(unsigned long long))) {
    intValue = boost::any_cast<unsigned long long>(value);
    whatIs = kLong;
  }
  // Treat all float as double
  else if (compare_types(value.type(), typeid(float))) {
    doubleValue = boost::any_cast<float>(value);
    whatIs = kDouble;
  }
  else if (compare_types(value.type(), typeid(double))) {
    doubleValue = boost::any_cast<double>(value);
    whatIs = kDouble;
  }
  // Nested types
  else if (compare_types(value.type(), typeid(Extensible))) {
    Extensible e = boost::any_cast<Extensible>(value);
    strValue = e.serialize();
    whatIs = kObj;
  }
  else if (compare_types(value.type(), typeid(std::vector<boost::any>))) {
    vValue = boost::any_cast<std::vector<boost::any> >(value);
    whatIs = kVector;
  }

  // To string
  std::ostringstream final;
  switch (whatIs) {
    case kLong:
      final << intValue;
      break;
    case kDouble:
      final << doubleValue;
      break;
    case kString:
      jsonifyString(strValue, final);
      break;
    case kObj:
      final << strValue;
      break;
    case kVector:
      final << '[';
      for (i = 0; (int) i < (int) vValue.size() - 1; ++i)
        final << serializeAny(vValue[i]) << ", ";
      if (i < vValue.size())
        final << serializeAny(vValue[i]);
      final << ']';
      break;
    default:
      final << "\"Unknown:" << value.type().name() << '"';
  }

  return final.str();
}



bool Extensible::anyToBoolean(const boost::any& any)
{
  if (compare_types(any.type(), typeid(bool)))
    return boost::any_cast<bool>(any);
  else if (compare_types(any.type(), typeid(std::string)))
      return boost::iequals(boost::any_cast<std::string>(any), "true");
  else if (compare_types(any.type(), typeid(char*)))
        return strcasecmp(boost::any_cast<char*>(any), "true") == 0;
  else if (compare_types(any.type(), typeid(const char*)))
          return strcasecmp(boost::any_cast<const char*>(any), "true") == 0;
  else
    return (anyToDouble(any) != 0.0);
}



double Extensible::anyToDouble(const boost::any& any)
{
  if (compare_types(any.type(), typeid(double)))
    return boost::any_cast<double>(any);
  else if (compare_types(any.type(), typeid(float)))
    return boost::any_cast<float>(any);
  else if (compare_types(any.type(), typeid(long)))
    return boost::any_cast<long>(any);
  else if (compare_types(any.type(), typeid(int)))
    return boost::any_cast<int>(any);
  else if (compare_types(any.type(), typeid(short)))
    return boost::any_cast<short>(any);
  else if (compare_types(any.type(), typeid(char)))
    return boost::any_cast<char>(any);
  else if (compare_types(any.type(), typeid(unsigned)))
    return boost::any_cast<unsigned>(any);
  // As a string, or maybe an integer?
  else {
    std::istringstream str(anyToString(any));
    double any;
    str >> any;
    return any;
  }
}



long Extensible::anyToLong(const boost::any& any)
{
  if (compare_types(any.type(), typeid(long)))
    return boost::any_cast<long>(any);
  else if (compare_types(any.type(), typeid(int)))
    return boost::any_cast<int>(any);
  else if (compare_types(any.type(), typeid(short)))
    return boost::any_cast<short>(any);
  else if (compare_types(any.type(), typeid(char)))
    return boost::any_cast<char>(any);
  else if (compare_types(any.type(), typeid(unsigned)))
    return boost::any_cast<unsigned>(any);
  // Try as a string, and parse
  else {
    std::istringstream str(anyToString(any));
    long any;
    str >> any;
    return any;
  }
}



unsigned Extensible::anyToUnsigned(const boost::any& any)
{
  if (compare_types(any.type(), typeid(unsigned)))
    return boost::any_cast<unsigned>(any);
  else
    return anyToLong(any);
}



std::string Extensible::anyToString(const boost::any& value)
{
  if (compare_types(value.type(), typeid(const char*)))
    return std::string(boost::any_cast<const char*>(value));
  else if (compare_types(value.type(), typeid(char*)))
    return std::string(boost::any_cast<char*>(value));
  else if (compare_types(value.type(), typeid(std::string)))
    return boost::any_cast<std::string>(value);
  else if (compare_types(value.type(), typeid(char)))
    return std::string(1, boost::any_cast<char>(value));
  else
    return serializeAny(value);
}



int64_t Extensible::anyToS64(const boost::any& value)
{
  if (compare_types(value.type(), typeid(int64_t)))
    return boost::any_cast<int64_t>(value);
  else
    return anyToLong(value);
}



uint64_t Extensible::anyToU64(const boost::any& value)
{
  if (compare_types(value.type(), typeid(uint64_t)))
    return boost::any_cast<uint64_t>(value);
  else
    return anyToLong(value);
}



bool Extensible::hasField(const std::string& key) const
{
  for (DictType_::const_iterator i = dictionary_.begin();
       i != dictionary_.end(); ++i) {
    if (i->first == key) return true;
  }
  return false;
}



const boost::any& Extensible::operator [] (const std::string& key) const throw (DmException)
{
  DictType_::const_iterator i;
  for (i = dictionary_.begin(); i != dictionary_.end(); ++i) {
    if (i->first == key) {
      return i->second;
    }
  }
  throw DmException(DMLITE_SYSERR(EINVAL),
                    "Key '" + key + "' not found");
}



boost::any& Extensible::operator [] (const std::string& key)
{
  DictType_::iterator i;
  for (i = dictionary_.begin(); i != dictionary_.end(); ++i) {
    if (i->first == key) {
      return i->second;
    }
  }

  boost::any any;
  dictionary_.push_back(std::make_pair(key, any));
  return dictionary_.back().second;
}



bool Extensible::operator == (const Extensible& e) const
{
  return !(*this < e) && !(e < *this);
}



bool Extensible::operator != (const Extensible& e) const
{
  return !(*this == e);
}



bool Extensible::operator < (const Extensible& e) const
{
  return this->serialize() < e.serialize();
}



bool Extensible::operator >(const Extensible& e) const
{
  return *this < e;
}



unsigned long Extensible::size() const
{
  return dictionary_.size();
}



void Extensible::clear()
{
  dictionary_.clear();
}



void Extensible::copy(const Extensible& s)
{
  clear();
  std::copy(s.dictionary_.begin(), s.dictionary_.end(),
            std::back_inserter(this->dictionary_));
}



void Extensible::erase(const std::string& s)
{
  DictType_::iterator i;
  for (i = dictionary_.begin(); i != dictionary_.end(); ++i) {
    if (i->first == s) {
      dictionary_.erase(i);
      return;
    }
  }
}



std::string Extensible::serialize() const
{
  std::ostringstream str;

  str << "{";
  if (dictionary_.size() > 0) {
    DictType_::const_iterator i, lastOne;
    lastOne = --dictionary_.end();
    for (i = dictionary_.begin(); i != lastOne; ++i) {
      str << '"' << i->first << "\": " << serializeAny(i->second) << ", ";
    }
    str << '"' << i->first << "\": " << serializeAny(i->second);
  }
  str << "}";

  return str.str();
}



void Extensible::populate(const boost::property_tree::ptree& root)
{
  boost::property_tree::ptree::const_iterator i;
  std::vector<boost::any> auxV;
  boost::any v;

  for (i = root.begin(); i != root.end(); ++i) {
    // Process value
    if (i->second.empty()) {
      v = i->second.data();
    }
    else {
      Extensible nested;
      nested.populate(i->second.get_child(""));
      // Now, if there is a pair with no key, it is an array
      if (nested.hasField(""))
        v = nested.getVector("");
      else
        v = nested;
    }
    // Array elements are key->value with no key!
    if (i->first.empty())
      auxV.push_back(v);
    else
      this->dictionary_.push_back(std::make_pair(i->first, v));
  }

  // If auxV is not empty, push it with no key
  if (!auxV.empty())
    this->dictionary_.push_back(std::make_pair("", auxV));
}



void Extensible::deserialize(const std::string& serial) throw (DmException)
{
  if (serial.empty())
    return;

  // Old Boost versions have a bug related with escaped '/'
  // https://svn.boost.org/trac/boost/ticket/4326
#if BOOST_VERSION < 104400
  std::ostringstream patchedSerial;

  unsigned i;
  for (i = 0; i < serial.size() - 1; ++i) {
    if (serial[i] == '\\' && serial[i + 1] == '/') {
      patchedSerial << '/';
      ++i;
    }
    else {
      patchedSerial << serial[i];
    }
  }
  if (i < serial.size())
    patchedSerial << serial[i];

  std::istringstream stream(patchedSerial.str());
#else
  std::istringstream stream(serial);
#endif

  boost::property_tree::ptree tree;

  try {
    boost::property_tree::read_json(stream, tree);
  }
  catch (boost::property_tree::json_parser::json_parser_error e) {
    throw DmException(DMLITE_SYSERR(DMLITE_MALFORMED),
                      "Probably malformed JSON data (%s)", e.what());
  }

  this->populate(tree);
}



std::vector<std::string> Extensible::getKeys(void) const throw (DmException)
{
  std::vector<std::string> keys;

  for (DictType_::const_iterator i = dictionary_.begin();
       i != dictionary_.end(); ++i) {
    keys.push_back(i->first);
  }

  return keys;
}



bool Extensible::getBool(const std::string& key, bool defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToBoolean(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to bool (it is %s)",
                      key.c_str(), value.type().name());
  }
}



long Extensible::getLong(const std::string& key, long defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToLong(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to long (it is %s)",
                      key.c_str(), value.type().name());
  }
}



unsigned long Extensible::getUnsigned(const std::string& key, unsigned long defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToUnsigned(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to unsigned (it is %s)",
                      key.c_str(), value.type().name());
  }
}



double Extensible::getDouble(const std::string& key, double defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToDouble(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to double (it is %s)",
                      key.c_str(), value.type().name());
  }
}



int64_t Extensible::getS64(const std::string& key, int64_t defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToS64(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to int64_t (it is %s)",
                      key.c_str(), value.type().name());
  }
}



uint64_t Extensible::getU64(const std::string& key, uint64_t defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToU64(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to uint64_t (it is %s)",
                      key.c_str(), value.type().name());
  }
}



std::string Extensible::getString(const std::string& key, const std::string& defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return anyToString(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to string (it is %s)",
                      key.c_str(), value.type().name());
  }
}



Extensible Extensible::getExtensible(const std::string& key, const Extensible& defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return boost::any_cast<Extensible>(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to dmlite::Extensible (it is %s)",
                      key.c_str(), value.type().name());
  }
}



std::vector<boost::any> Extensible::getVector(const std::string& key, const std::vector<boost::any>& defaultValue) const throw (DmException)
{
  if (!hasField(key)) return defaultValue;

  boost::any value = (*this)[key];

  try {
    return boost::any_cast<std::vector<boost::any> >(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DMLITE_SYSERR(EINVAL),
                      "'%s' can not be cast to std::vector<boost:any> (it is %s)",
                      key.c_str(), value.type().name());
  }
}
