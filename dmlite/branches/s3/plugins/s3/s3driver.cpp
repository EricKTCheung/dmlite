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
  if(ne_sock_init() != 0) {
    throw DmException(DM_UNKNOWN_ERROR, std::string("Could not initialize libneon"));
  }
}

S3Driver::~S3Driver()
{
  ne_sock_exit();
}

Location S3Driver::getQueryString(std::string method, std::string host, 
                                 std::string bucket, std::string key,
                                 time_t expirationDate)
{
  Location rloc;
  std::map<std::string, std::string> headerMap;
  std::string expirationString;
  std::string signature;

  std::stringstream conversionStream;

  conversionStream << expirationDate;
  expirationString = conversionStream.str();
  headerMap["expires"] = expirationString;

  signature = getSignature(method, bucket, key, headerMap, "");
  signature = urlEncode(signature);

  std::string path;
  conversionStream.str("");
  conversionStream << "/" << bucket << "/" << key;
  path = conversionStream.str();

  std::string token;
  conversionStream.str("");
  conversionStream << "?AWSAccessKeyId=" << s3AccessKeyID_ << "&Expires=" << expirationString << "&Signature=" << signature;
  token = conversionStream.str();

  rloc = Location(host.c_str(), path.c_str(), true, 3, "AWSAccessKeyId", s3AccessKeyID_.c_str(),
                                                       "Expires", expirationString.c_str(),
                                                       "Signature", signature.c_str());

  return rloc;
}

std::string S3Driver::getSignature(std::string method, std::string bucket, 
                                  std::string key, std::map<std::string,
                                  std::string> headerMap, std::string param)
{
  std::string stringToSign;
  std::string signature;
  unsigned char encryptedResult[1024];
  unsigned int encryptedResultSize;
  long base64EncodedStringLength;

  stringToSign = canonicalize(method, bucket, key, headerMap, param);
  HMAC(EVP_sha1(), s3SecretAccessKey_.c_str(), s3SecretAccessKey_.size(),
          (const unsigned char *) stringToSign.c_str(), stringToSign.size(), encryptedResult, &encryptedResultSize);
  signature = base64Encode(encryptedResult, encryptedResultSize,
                 base64EncodedStringLength);
//  signature = urlEncode(signature);

  return signature;
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
    stringToSign << itHeaderMap->second << '\n';
  } else if (headerMap.count("Date") > 0) {
    itHeaderMap = headerMap.find("Date");
    stringToSign << itHeaderMap->second << '\n';
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

std::string S3Driver::s3TimeStringFromNow()
{
  time_t rawTime;
  tm *gmTime;
  time(&rawTime);
  gmTime = gmtime(&rawTime);

  char dateString[31];
  strftime(dateString, 31, DATE_FORMAT, gmTime);

  return std::string(dateString);
}

S3Error S3Driver::getS3Error(ne_request *request) {
  S3Error errorMsg;
  char buffer[512+1];
  ssize_t bytesRead;
  while ((bytesRead = ne_read_response_block(request, buffer, 512)) > 0) {
    buffer[512] = '\0';
    printf("%s", buffer);
  }
  if (bytesRead < 0) {
    // TODO: Error handling
  }

  return errorMsg;
}

S3RequestResponse S3Driver::headObject(std::string host, std::string bucket, std::string key)
{
  ne_session* session;
  ne_request* request;

  std::map<std::string, std::string> headerMap;
  std::string signature;
  std::stringstream authorizationStream;
  std::stringstream path;
  S3RequestResponse response;
  S3ObjectMetadata *pntMeta = response.mutable_s3object_meta();

  session = ne_session_create("http", host.c_str(), 80);

  path << "/" << bucket << "/" << key;
  request = ne_request_create(session, "HEAD", path.str().c_str());

  headerMap["Date"] = s3TimeStringFromNow().c_str();

  signature = getSignature("HEAD", bucket, key, headerMap, "");
  authorizationStream << "AWS " << s3AccessKeyID_ << ":" << signature;

  ne_add_request_header(request, "Date", headerMap["Date"].c_str());
  ne_add_request_header(request, "Authorization", authorizationStream.str().c_str());

  if (ne_begin_request(request) == NE_OK) {
    const ne_status *requestStatus = ne_get_status(request);
    response.set_http_code(requestStatus->code);
    response.set_http_reason(requestStatus->reason_phrase);
    const char *clength = ne_get_response_header(request, "Content-Length");
    if (clength) {
      pntMeta->set_content_length(atoi(clength));
    }
    if (!ne_accept_2xx(NULL, request, requestStatus)) {
      getS3Error(request);
    }
    if (ne_end_request(request) != NE_OK) {
      throw DmException(DM_UNKNOWN_ERROR, std::string(ne_get_error(session)));  
    }
  } else {
    throw DmException(DM_UNKNOWN_ERROR, std::string(ne_get_error(session)));
  }

  ne_close_connection(session);
  ne_session_destroy(session);

  return response;
}


