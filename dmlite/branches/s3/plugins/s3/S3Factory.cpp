/// @file    plugins/s3/S3.h
/// @brief   s3 plugin.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>

#include "S3.h"
#include "S3Factory.h"

using namespace dmlite;

S3ConnectionFactory::S3ConnectionFactory(const std::string& host, unsigned int port,
                         const std::string& s3AccessKeyID,
                         const std::string& s3SecretAccessKey):
host_(host), port_(port), s3AccessKeyID_(s3AccessKeyID), s3SecretAccessKey_(s3SecretAccessKey)
{
  // Nothing
}

S3ConnectionFactory::~S3ConnectionFactory()
{
  // Nothing
}

S3Driver* S3ConnectionFactory::create()
{
  S3Driver *conn = new S3Driver(host_, port_, s3AccessKeyID_, s3SecretAccessKey_);

  return conn;
}

void S3ConnectionFactory::destroy(S3Driver* conn)
{
  delete conn;
}

bool S3ConnectionFactory::isValid(S3Driver* conn)
{
  // TODO
  return true;
}

void S3ConnectionFactory::setS3SecretAccessKey(const std::string& key)
{
  this->s3SecretAccessKey_ = key;
}

S3Factory::S3Factory() throw (DmException):
  connectionFactory_(std::string("s3.amazonaws.com"), 80, std::string(), std::string()),
  connectionPool_(&connectionFactory_, 2)
{
  pthread_key_create(&this->thread_s3_conn_, NULL);
}

S3Factory::~S3Factory() throw (DmException)
{
  pthread_key_delete(this->thread_s3_conn_);
}

void S3Factory::configure(const std::string& key, const std::string& value) throw (DmException)
{
  if (key == "S3Host") {
    this->connectionFactory_.host_ = value;
  } else if (key == "S3Bucket") {
    this->connectionFactory_.bucket_ = value;
  } else if (key == "S3AccessKeyID") {
    this->connectionFactory_.s3AccessKeyID_ = value;
  } else if (key == "S3SecretAccessKey") {
    this->connectionFactory_.setS3SecretAccessKey(value);
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
  return new S3PoolDriver(this);
}

S3Driver* S3Factory::getConnection(void) throw (DmException)
{
  S3Driver* conn;
  
  // Try from the thread local storage
  conn = static_cast<S3Driver*>(pthread_getspecific(this->thread_s3_conn_));
  
  // If NULL, instantiate and set in the thread local storage
  if (conn == NULL) {
    conn = this->connectionPool_.acquire();
    pthread_setspecific(this->thread_s3_conn_, conn);
  }
  // Increase reference count otherwise
  else {
    this->connectionPool_.acquire(conn);
  }
  
  // Return
  return conn;
}

void S3Factory::releaseConnection(S3Driver* conn) throw (DmException)
{
  if (this->connectionPool_.release(conn) == 0)
    pthread_setspecific(this->thread_s3_conn_, NULL);
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

