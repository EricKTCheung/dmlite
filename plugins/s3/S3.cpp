/// @file    plugins/s3/S3.h
/// @brief   s3 plugin.
/// @author  Alexandre Beche <abeche@cern.ch>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>

#include "S3.h"

using namespace dmlite;

/* S3 pool handling */
S3PoolDriver::S3PoolDriver(S3Factory *factory) throw (DmException):
            stack(0x00),
            factory_(factory)
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
  return new S3PoolHandler(this->factory_, poolName, this->stack);
}

void S3PoolDriver::setStackInstance(StackInstance* si) throw (DmException)
{
  this->stack = si;
}

S3PoolHandler::S3PoolHandler(S3Factory *factory, const std::string& poolName, StackInstance* si):
  factory_(factory),
  poolName(poolName),
  stack(si),
  bucket_("mhellmic-dpm")
{
  this->conn_ = factory_->getConnection();
}

S3PoolHandler::~S3PoolHandler()
{
  this->factory_->releaseConnection(this->conn_);
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
  // TODO: check connection to S3
  return true;
}

bool S3PoolHandler::replicaAvailable(const FileReplica& replica) throw (DmException)
{
  bool isAvailable = false;
  // if PENDING, check the file on S3
  S3ObjectMetadata meta;
  S3RequestResponse response;

  switch (replica.status) {
  case '-':
    isAvailable = true;
    break;
  case 'P':
    response = 
       this->conn_->headObject(this->bucket_,
                              replica.rfn);
    if (response.http_code() == 200) {
      meta = response.s3object_meta();
      // if the response was successful (file complete), change the db entry
      if (meta.has_content_length()) {
        this->stack->getCatalog()->replicaSetStatus(replica.rfn, '-');
        this->stack->getINode()->changeSize(replica.fileid, meta.content_length());
        isAvailable = true;
      }
    }
    break;
  case 'D':
    isAvailable = false;
    break;
  }

  return isAvailable;
}

Location S3PoolHandler::getLocation(const FileReplica& replica) throw (DmException)
{
  Location rloc;
  time_t expiration = time(NULL) + static_cast<time_t>(60);
  rloc = this->conn_->getQueryString(
              "GET",
              this->bucket_,
              replica.rfn,
              expiration);

  rloc.available = static_cast<uint8_t>(replicaAvailable(replica));

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
  rloc = this->conn_->getQueryString(
              "PUT",
              this->bucket_,
              fn,
              expiration);

  return rloc;
}

void S3PoolHandler::putDone(const FileReplica& replica, const std::map<std::string, std::string>& extras) throw (DmException)
{
}
