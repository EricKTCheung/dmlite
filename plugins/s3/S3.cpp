/// @file    plugins/s3/S3.h
/// @brief   s3 plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>

#include "S3.h"

using namespace dmlite;

/* S3 pool handling */
S3PoolDriver::S3PoolDriver(std::string host, std::string bucket, std::string s3AccessKeyID, std::string s3SecretAccessKey) throw (DmException):
            stack(0x00),
            host_(host),
            bucketName_(bucket),
            s3AccessKeyID_(s3AccessKeyID),
            s3SecretAccessKey_(s3SecretAccessKey)
{
  // Nothing
}

S3PoolDriver::~S3PoolDriver()
{
  // Nothing
}

void S3PoolDriver::setSecurityContext(const SecurityContext*) throw (DmException)
{
  // TODO
}

PoolHandler* S3PoolDriver::createPoolHandler(const std::string& poolName) throw (DmException)
{
  return new S3PoolHandler(this, poolName, this->stack);
}

void S3PoolDriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack = si;
}

S3PoolHandler::S3PoolHandler(S3PoolDriver* driver, const std::string& poolName, StackInstance* si):
  driver_(driver),
  poolName(poolName),
  stack(si)
{
  this->driver_->s3connection_ = S3Driver(
            this->driver_->s3AccessKeyID_,
            this->driver_->s3SecretAccessKey_);
}

S3PoolHandler::~S3PoolHandler()
{
  // Nothing
}


std::string S3PoolHandler::getPoolType(void) throw (DmException)
{
  return "s3";
}

std::string S3PoolHandler::getPoolName(void) throw (DmException)
{
  return this->poolName;
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
  return true;
}

bool S3PoolHandler::replicaAvailable(const FileReplica& replica) throw (DmException)
{
  return true;
}

Location S3PoolHandler::getLocation(const FileReplica& replica) throw (DmException)
{
  // if PENDING, check the file on S3
  S3ObjectMetadata meta;
  bool isReplicaComplete = false;
  if (replica.status == 'P') {
    meta = 
       this->driver_->s3connection_.headObject(this->driver_->host_,
                                               this->driver_->bucketName_,
                                               replica.rfn);

    // if the response was successful (file copmlete), change the db entry
    if (meta.has_contentlength()) {
      this->stack->getCatalog()->replicaSetStatus(replica.rfn, '-');
    } else {
      throw DmException(DM_NO_SUCH_REPLICA, std::string("The Replica is not yet completed or failed to complete or doesnt exist"));
    }
  }

  // if successful (file exists), create GET link
  Location rloc;
  time_t expiration = time(NULL) + static_cast<time_t>(60);
  rloc = this->driver_->s3connection_.getQueryString(
              "GET",
              this->driver_->host_,
              this->driver_->bucketName_,
              replica.rfn,
              expiration);

  return rloc;
}

void S3PoolHandler::remove(const FileReplica& replica) throw (DmException)
{
}

Location S3PoolHandler::putLocation(const std::string& fn) throw (DmException)
{
  // create database entry with state PENDING
  struct stat st = this->stack->getCatalog()->extendedStat(fn).stat;
  this->stack->getCatalog()->addReplica(std::string(), st.st_ino,
                                        std::string(),
                                        fn, 'P', 'P',
                                        this->poolName, std::string());

  // create PUT link
  Location rloc;
  time_t expiration = time(NULL) + static_cast<time_t>(60);
  rloc = this->driver_->s3connection_.getQueryString(
              "PUT",
              this->driver_->host_,
              this->driver_->bucketName_,
              fn,
              expiration);

  return rloc;
}

void S3PoolHandler::putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException)
{
}

/* S3Factory implementation */
S3Factory::S3Factory() throw (DmException):
     bucketName_("default"),
     host_("s3.amazonaws.com"),
     s3AccessKeyID_("userID"),
     s3SecretAccessKey_("password")
{
  // Nothing
}

void S3Factory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "S3Host") {
    this->host_ = value;
  } else if (key == "S3Bucket") {
    this->bucketName_ = value;
  } else if (key == "S3AccessKeyID") {
    this->s3AccessKeyID_ = value;
  } else if (key == "S3SecretAccessKey") {
    this->s3SecretAccessKey_ = value;
  } else {
    throw DmException(DM_UNKNOWN_OPTION, "Option %s not recognised", key.c_str());
  }
}

std::string S3Factory::implementedPool() throw ()
{
  return "s3";
}

PoolDriver* S3Factory::createPoolDriver() throw (DmException)
{
  return new S3PoolDriver(this->host_, this->bucketName_, this->s3AccessKeyID_, this->s3SecretAccessKey_);
}

static void registerPluginS3(PluginManager* pm) throw (DmException)
{
  pm->registerFactory(static_cast<PoolDriverFactory*>(new S3Factory()));
}

/// This is what the PluginManager looks for
PluginIdCard plugin_s3_pooldriver = {
  API_VERSION,
  registerPluginS3
};
