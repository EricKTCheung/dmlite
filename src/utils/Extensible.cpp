/// @file    utils/Extensible.cpp
/// @brief   Extensible types (hold metadata).
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <ctype.h>
#include <dmlite/cpp/utils/extensible.h>
#include <sstream>

using namespace dmlite;


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



static std::string toString(const boost::any& value)
{
  long        intValue;
  double      doubleValue;
  std::string strValue;
  enum {kLong, kDouble, kString, kUnknown} whatIs = kUnknown;
  
  // String types are easy
  if (value.type() == typeid(std::string)) {
    strValue = boost::any_cast<std::string>(value);
    whatIs = kString;
  }
  else if (value.type() == typeid(const char*)) {
    strValue = boost::any_cast<const char*>(value);
    whatIs = kString;
  }
  else if (value.type() == typeid(char*)) {
    strValue = boost::any_cast<char*>(value);
    whatIs = kString;
  }
  // Treat all integers as long
  else if (value.type() == typeid(bool)) {
    bool b = boost::any_cast<bool>(value);
    return b ? "true" : "false";
  }
  else if (value.type() == typeid(char)) {
    intValue = boost::any_cast<char>(value);
    whatIs = kLong;
  }
  else if (value.type() == typeid(short)) {
    intValue = boost::any_cast<short>(value);
    whatIs = kLong;
  }
  else if (value.type() == typeid(int)) {
    intValue = boost::any_cast<int>(value);
    whatIs = kLong;
  }
  else if (value.type() == typeid(long)) {
    intValue = boost::any_cast<long>(value);
    whatIs = kLong;
  }
  else if (value.type() == typeid(unsigned)) {
    intValue = boost::any_cast<unsigned>(value);
    whatIs = kLong;
  }
  // Treat all float as double
  else if (value.type() == typeid(float)) {
    doubleValue = boost::any_cast<float>(value);
    whatIs = kDouble;
  }
  else if (value.type() == typeid(double)) {
    doubleValue = boost::any_cast<double>(value);
    whatIs = kDouble;
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
    default:
      final << "\"Unknown:" << value.type().name() << '"';
  }
  
  return final.str();
}



bool Extensible::hasField(const std::string& key) const
{
  return (dictionary_.find(key) != dictionary_.end());
}



const boost::any& Extensible::operator [] (const std::string& key) const throw (DmException)
{
  DictType_::const_iterator i;
  i = dictionary_.find(key);
  if (i != dictionary_.end())
    return (*i).second;
  else
    throw DmException(DM_INVALID_VALUE, "Key %s not found", key.c_str());
}



boost::any& Extensible::operator [] (const std::string& key)
{
  return dictionary_[key];
}


void Extensible::clear()
{
  dictionary_.clear();
}



std::string Extensible::serialize() const
{
  std::ostringstream str;
  DictType_::const_iterator i, lastOne;
  
  str << "{";
  lastOne = --dictionary_.end();
  for (i = dictionary_.begin(); i != lastOne; ++i) {
    str << '"' << i->first << "\": " << toString(i->second) << ", ";
  }
  str << '"' << i->first << "\": " << toString(i->second) << "}";
  
  return str.str();
}



void Extensible::deserialize(const std::string& serial)
{
  if (serial.empty())
    return;
  
  std::istringstream stream(serial);
  boost::property_tree::ptree tree;
  
  try {
    boost::property_tree::read_json(stream, tree);
  }
  catch (boost::property_tree::json_parser::json_parser_error e) {
    throw DmException(DM_INTERNAL_ERROR,
                      "Probably malformed JSON data(%s)", e.what());
  }
  
  boost::property_tree::ptree::const_iterator i;
  for (i = tree.begin(); i != tree.end(); ++i)
    dictionary_.insert(std::make_pair(i->first, i->second.data()));
}



bool Extensible::getBool(const std::string& key) const throw (DmException)
{
  if (!hasField(key)) return 0;

  boost::any value = (*this)[key];
  
  try {
    if (value.type() == typeid(bool))
      return boost::any_cast<bool>(value);
    else
      return (getLong(key) != 0);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to bool (it is %s)",
                      key.c_str(), value.type().name());
  }
  catch (DmException) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to bool (it is %s)",
                      key.c_str(), value.type().name());
  }
}



long Extensible::getLong(const std::string& key) const throw (DmException)
{
  if (!hasField(key)) return 0;

  boost::any value = (*this)[key];

  try {
    if (value.type() == typeid(long))
      return boost::any_cast<long>(value);
    else if (value.type() == typeid(int))
      return boost::any_cast<int>(value);
    else if (value.type() == typeid(short))
      return boost::any_cast<short>(value);
    else if (value.type() == typeid(char))
      return boost::any_cast<char>(value);
    else if (value.type() == typeid(unsigned))
      return boost::any_cast<unsigned>(value);
    // Try as a string, and parse
    else {
      std::istringstream str(getString(key));
      long value;
      str >> value;
      return value;
    }
  }
  catch (boost::bad_any_cast) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to long (it is %s)",
                      key.c_str(), value.type().name());
  }
}



unsigned long Extensible::getUnsigned(const std::string& key) const throw (DmException)
{
  if (!hasField(key)) return 0;

  boost::any value = (*this)[key];
  
  try {
    if (value.type() == typeid(unsigned))
      return boost::any_cast<unsigned>(value);
    else
      return static_cast<unsigned long>(getLong(key));
  }
  catch (boost::bad_any_cast) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to unsigned (it is %s)",
                      key.c_str(), value.type().name());
  }
  catch (DmException) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to unsigned (it is %s)",
                      key.c_str(), value.type().name());
  }
}



double Extensible::getDouble(const std::string& key) const throw (DmException)
{
  if (!hasField(key)) return 0;
  
  boost::any value = (*this)[key];
  
  try {
    if (value.type() == typeid(double))
      return boost::any_cast<double>(value);
    else if (value.type() == typeid(float))
      return boost::any_cast<float>(value);
    // As a string, or maybe an integer?
    else {
      try {
        std::istringstream str(getString(key));
        double value;
        str >> value;
        return value;
      }
      catch (boost::bad_any_cast) {
        return getLong(key);
      }
    }  
  }
  catch (boost::bad_any_cast) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to double (it is %s)",
                      key.c_str(), value.type().name());
  }
  catch (DmException) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to double (it is %s)",
                      key.c_str(), value.type().name());
  }
}



std::string Extensible::getString(const std::string& key) const throw (DmException)
{
  if (!hasField(key)) return std::string();

  boost::any value = (*this)[key];

  try {
    if (value.type() == typeid(const char*))
      return std::string(boost::any_cast<const char*>(value));
    else if (value.type() == typeid(char*))
      return std::string(boost::any_cast<char*>(value));
    else
      return boost::any_cast<std::string>(value);
  }
  catch (boost::bad_any_cast) {
    throw DmException(DM_INVALID_VALUE, "'%s' can not be cast to string (it is %s)",
                      key.c_str(), value.type().name());
  }
}
