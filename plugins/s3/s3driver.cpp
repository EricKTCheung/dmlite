/// @file    plugins/s3/S3.h
/// @brief   plugin to store data in a s3 backend.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include "s3driver.h"

/**
  The functionality is mainly inspired by libaws http://sourceforge.net/projects/libaws/
  The decision to recreate some functionality comes from
  (1) we only need the s3 part and not a whole aws lib
  (2) development of libaws is discontinued and
      the developers don't react to requests
**/

using namespace dmlite;

S3Driver::S3Driver(std::string s3AccessKeyID, std::string s3SecretAccessKey):
  s3AccessKeyID_(s3AccessKeyID),
  s3SecretAccessKey_(s3SecretAccessKey)
{
  // Nothing
}

Location S3Driver::getQueryString(std::string method, std::string host, 
                                 std::string bucket, std::string key,
                                 time_t expirationDate)
{
  Location rloc;
  std::map<std::string, std::string> headerMap;
  std::string stringToSign;
  std::string expirationString;
  std::string signature;
  unsigned char encryptedResult[1024];
  unsigned int encryptedResultSize;
  long base64EncodedStringLength;

  std::stringstream conversionStream;

  conversionStream << expirationDate;
  expirationString = conversionStream.str();
  headerMap["expires"] = expirationString;

  stringToSign = canonicalize(method, bucket, key, headerMap, "");

  HMAC(EVP_sha1(), s3SecretAccessKey_.c_str(), s3SecretAccessKey_.size(),
          (const unsigned char *) stringToSign.c_str(), stringToSign.size(), encryptedResult, &encryptedResultSize);
  signature = base64Encode(encryptedResult, encryptedResultSize,
                 base64EncodedStringLength);
  signature = urlEncode(signature);

  std::string path;
  conversionStream.str("");
  conversionStream << "/" << bucket << "/" << key;
  path = conversionStream.str();

  std::string token;
  conversionStream.str("");
  conversionStream << "?AWSAccessKeyId=" << s3AccessKeyID_ << "&Expires=" << expirationString << "&Signature=" << signature;
  token = conversionStream.str();

  rloc = Location(host.c_str(), path.c_str(), 3, true, "AWSAccessKeyId", s3AccessKeyID_.c_str(),
                                                       "Expires", expirationString.c_str(),
                                                       "Signature", signature.c_str());

  return rloc;
}

std::string S3Driver::canonicalize(std::string method, std::string bucket, 
                         std::string key, std::map<std::string,
                         std::string> headerMap, std::string param)
{
  std::stringstream stringToSign;
  
  // add method
  stringToSign << method << '\n';

  // add headers
  // Assume that Content-MD5 and Content-Length do not exist
  std::map<std::string, std::string>::iterator itHeaderMap;
  stringToSign << '\n' << '\n';

  if (headerMap.count("expires") > 0) {
    itHeaderMap = headerMap.find("expires");
    stringToSign << itHeaderMap->first << ":" << itHeaderMap->second;
  }

  // add bucket
  if (bucket.size() > 0)
    stringToSign << '/' << bucket;

  // add key
  stringToSign << '/';
  if (key.size() > 0)
    stringToSign << key;

  // add param
  if (param.size() > 0)
    stringToSign << param;

  return stringToSign.str();
}

std::string S3Driver::urlEncode(const std::string& aContent)
{
  std::string encoded;
  unsigned char c;
  unsigned char low, high;

  for (size_t i = 0; i < aContent.size(); i++) {
    c = aContent[i];
    if (isalnum(c))
       encoded += c;
    else {
       high = c / 16;
       low = c % 16;
       encoded += '%';
       encoded += (high < 10 ? '0' + high : 'A' + high - 10);
       encoded += (low < 10 ? '0' + low : 'A' + low - 10);
    }
  }
  return encoded;
}

std::string S3Driver::base64Encode(const unsigned char* aContent, size_t aContentSize, long& aBase64EncodedStringLength)
{
  char* lEncodedString;

  // initialization for base64 encoding stuff
  BIO* lBio = BIO_new(BIO_s_mem());
  BIO* lB64 = BIO_new(BIO_f_base64());
  BIO_set_flags(lB64, BIO_FLAGS_BASE64_NO_NL);
  lBio = BIO_push(lB64, lBio);

  BIO_write(lBio, aContent, aContentSize);
  BIO_flush(lBio);
  aBase64EncodedStringLength = BIO_get_mem_data(lBio, &lEncodedString);

  // ensures null termination
  std::stringstream lTmp;
  lTmp.write(lEncodedString, aBase64EncodedStringLength);

  BIO_free_all(lBio);

  return lTmp.str(); // copy
}

