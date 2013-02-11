#include <boost/algorithm/string.hpp>
#include <dmlite/cpp/utils/checksums.h>



std::string dmlite::checksums::fullChecksumName(const std::string& cs)
{
  if (boost::iequals(cs, "AD"))
    return "ADLER32";
  else if (boost::iequals(cs, "CS"))
    return "UNIXcksum";
  else if (boost::iequals(cs, "MD"))
    return "MD5";
  else
    return cs;
}



std::string dmlite::checksums::shortChecksumName(const std::string& cs)
{
  if (boost::iequals(cs, "ADLER32"))
    return "AD";
  else if (boost::iequals(cs, "UNIXcksum"))
    return "CS";
  else if (boost::iequals(cs, "MD5"))
    return "MD";
  else
    return cs;
}
