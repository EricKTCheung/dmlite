/// @file    plugins/s3/S3.h
/// @brief   plugin to store data in a s3 backend.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef S3Driver_H
#define	S3Driver_H

#include <map>
#include <sstream>
#include <iostream>
#include <openssl/hmac.h>
#include <dmlite/common/dm_types.h>

namespace dmlite {

class S3Driver {
public:
  S3Driver() {};
  S3Driver(std::string s3AccessKeyID, std::string s3SecretAccessKey);

  ~S3Driver() {};
  
  Location getQueryString(std::string method, std::string host, 
                          std::string bucket, std::string key,
                          time_t expirationDate);

  std::string canonicalize(std::string method, std::string bucket, 
                           std::string key, std::map<std::string,
                           std::string> headerMap, std::string param);

  private:

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;

  std::string urlEncode(const std::string& aContent);

  std::string base64Encode(const unsigned char* aContent, size_t aContentSize, long& aBase64EncodedStringLength);
};

};

#endif // S3Driver_H
