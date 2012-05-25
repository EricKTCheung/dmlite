/// @file    plugins/s3/S3.h
/// @brief   s3 plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include "S3.h"

using namespace dmlite;

S3IOHandler::S3IOHandler(const std::string& uri, std::iostream::openmode openmode) throw (DmException):
  path_(uri)
{
/*
NOT THREADSAFE
S3Status S3_initialize(const char *userAgentInfo, int flags,
                       const char *defaultS3HostName);
*/
}

S3IOHandler::~S3IOHandler()
{
/*
void S3_deinitialize();
*/
}

void S3IOHandler::close(void) throw (DmException)
{
  // copy the remaining bytes with put and get respectively
}

size_t S3IOHandler::read(char* buffer, size_t count) throw (DmException)
{
/*
void S3_get_object(const S3BucketContext *bucketContext, const char *key,
                   const S3GetConditions *getConditions,
                   uint64_t startByte, uint64_t byteCount,
                   S3RequestContext *requestContext,
                   const S3GetObjectHandler *handler, void *callbackData);
*/
  return count;
}

/* Write a chunk of a file in a S3 FS*/
size_t S3IOHandler::write(const char* buffer, size_t count) throw (DmException)
{
/*
void S3_put_object(const S3BucketContext *bucketContext, const char *key,
                   uint64_t contentLength,
                   const S3PutProperties *putProperties,
                   S3RequestContext *requestContext,
                   const S3PutObjectHandler *handler, void *callbackData);
*/
	return count;
}

/* Position the reader pointer to the desired offset */
void S3IOHandler::seek(long offset, std::ios_base::seekdir whence) throw (DmException)
{
}

long S3IOHandler::tell(void) throw (DmException)
{
	return 0;
}

void S3IOHandler::flush(void) throw (DmException)
{
}

bool S3IOHandler::eof(void) throw (DmException)
{
	return true;
}

struct stat S3IOHandler::pstat(void) throw (DmException)
{
  struct stat meta;
  return meta;
}


/* S3 pool handling */
S3PoolHandler::S3PoolHandler(StackInstance* si, const Pool& pool,
                             std::string host, 
                             std::string bucketName,
                             std::string s3AccessKeyID,
                             std::string s3SecretAccessKey) throw (DmException):
            stack(si), pool(pool), host_(host),
            bucketName_(bucketName),
            s3AccessKeyID_(s3AccessKeyID),
            s3SecretAccessKey_(s3SecretAccessKey)
{
  std::cout << "Poolhost = " << this->host_ << std::endl;
  PoolMetadata* meta = this->stack->getPoolManager()->getPoolMetadata(pool);
  
  this->host_ = meta->getString("host");
  this->s3AccessKeyID_ = meta->getString("s3_access_key_id");
  this->s3SecretAccessKey_ = meta->getString("s3_secret_access_key");
/*
NOT THREADSAFE
  S3_initialize(NULL, S3_INIT_ALL, this->host_.c_str());
*/
  AWSConnectionFactory* lFactory = aws::AWSConnectionFactory::getInstance();
  this->lS3Rest =  lFactory->createS3Connection(this->s3AccessKeyID_, this->s3SecretAccessKey_, this->host_);
}

S3PoolHandler::~S3PoolHandler()
{
  S3_deinitialize();
}

void S3PoolHandler::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // TODO
}

std::string S3PoolHandler::getPoolType(void) throw (DmException)
{
  return this->pool.pool_type;
}

std::string S3PoolHandler::getPoolName(void) throw (DmException)
{
  return this->pool.pool_name;
}

uint64_t S3PoolHandler::getTotalSpace(void) throw (DmException)
{
  return 0;
}

uint64_t S3PoolHandler::getUsedSpace(void) throw (DmException)
{
  return 0;
}

uint64_t S3PoolHandler::getFreeSpace(void) throw (DmException)
{
  return 0;
}

bool S3PoolHandler::isAvailable(bool write) throw (DmException)
{
  // TODO
/*
void S3_test_bucket(S3Protocol protocol, S3UriStyle uriStyle,
                    const char *accessKeyId, const char *secretAccessKey,
                    const char *hostName, const char *bucketName,
                    int locationConstraintReturnSize,
                    char *locationConstraintReturn,
                    S3RequestContext *requestContext,
                    const S3ResponseHandler *handler, void *callbackData);
*/

  ListBucketResponsePtr lListBucket; 
  try {
    lListBucket = this->lS3Rest->listBucket(this->bucketName_, "", "", "", -1);
//  } catch (S3Exception &e) {
//    std::cerr << e.what() << std::endl;
  } catch (...) {
    return false;
  }
  return true;
}

bool S3PoolHandler::replicaAvailable(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
/*
void S3_head_object(const S3BucketContext *bucketContext, const char *key,
                    S3RequestContext *requestContext,
                    const S3ResponseHandler *handler, void *callbackData);
*/

  try {
    this->lS3Rest->head(this->bucketName_, sfn);
//  } catch (S3Exception &e) {
//    std::cerr << e.what() << std::endl;
  } catch (...) {
    return false;
  }
  return true;
}

Uri S3PoolHandler::getLocation(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
  // Start by giving the client the direct link to S3,
  // later link to our IOHandler implementation somewhere
/*
  S3Status S3_generate_authenticated_query_string
      (char *buffer, const S3BucketContext *bucketContext,
       const char *key, int64_t expires, const char *resource);
*/      

  std::string strUri;
  time_t expiration = time(NULL) + static_cast<time_t>(60);
  strUri = this->lS3Rest->getQueryString(this->bucketName_, sfn, expiration);
  std::cout << "string uri from s3 = " << strUri << std::endl;

  return dmlite::splitUri(strUri);  
}

void S3PoolHandler::remove(const std::string& sfn, const FileReplica& replica) throw (DmException)
{
/*
void S3_delete_object(const S3BucketContext *bucketContext, const char *key,
                      S3RequestContext *requestContext,
                      const S3ResponseHandler *handler, void *callbackData);
*/

  try {
    DeleteResponsePtr lDel = this->lS3Rest->del(this->bucketName_, sfn);
//  } catch (S3Exception &e) {
//    std::cerr << e.what() << std::endl;
  } catch (...) {
  }

}

std::string S3PoolHandler::putLocation(const std::string& sfn, Uri* uri) throw (DmException)
{
  // To be done
  throw DmException(DM_NOT_IMPLEMENTED, "s3::putLocation");
}

void S3PoolHandler::putDone(const std::string& sfn, const Uri& pfn, const std::string& token) throw (DmException)
{
}

/* S3IOFactory implementation */
S3IOFactory::S3IOFactory() throw (DmException):
      bucketName_("mhellmic-dpm"),
      s3host_("s3.amazonaws.com"),
      s3AccessKeyID_("userID"),
      s3SecretAccessKey_("password")

{
  // Nothing
}

void S3IOFactory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  /*
	if (key == "S3Host")
    this->s3host_ = value;
  else if (key == "S3AccessKeyID")
    this->s3AccessKeyID_ = value;
  else if (key == "S3SecretAccessKey")
    this->s3SecretAccessKey_ = value;
	else
  	throw DmException(DM_UNKNOWN_OPTION, std::string("Unknown option ") + key);
  */
}

IOHandler *S3IOFactory::createIO(const std::string& uri, std::iostream::openmode openmode) throw (DmException)
{
	return new S3IOHandler(uri, openmode);
}

std::string S3IOFactory::implementedPool() throw ()
{
  return "s3";
}

PoolHandler* S3IOFactory::createPoolHandler(StackInstance* si, const Pool& pool) throw (DmException)
{
  if (this->implementedPool() != std::string(pool.pool_type))
    throw DmException(DM_UNKNOWN_POOL_TYPE, "S3 does not recognise the pool type %s", pool.pool_type);
  return new S3PoolHandler(si, pool, this->s3host_,
                           this->bucketName_,
                           this->s3AccessKeyID_,
                           this->s3SecretAccessKey_);
}

static void registerPluginS3(PluginManager* pm) throw (DmException)
{
  pm->registerPoolHandlerFactory(new S3IOFactory());
}

static void registerIOS3(PluginManager* pm) throw (DmException)
{
  pm->registerIOFactory(new S3IOFactory());
}

/// This is what the PluginManager looks for
PluginIdCard plugin_s3 = {
  API_VERSION,
  registerPluginS3
};

PluginIdCard plugin_s3_io = {
  API_VERSION,
  registerIOS3
};
