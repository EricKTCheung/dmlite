/// @file    plugins/s3/S3Factory.h
/// @brief   plugin to store data in a s3 backend.
/// @author  Martin Philipp Hellmich <mhellmic@cern.ch>
#ifndef S3Factory_H
#define	S3Factory_H

#include <vector>

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/utils/dm_security.h>
#include <dmlite/cpp/utils/dm_urls.h>
#include <dmlite/cpp/utils/dm_poolcontainer.h>

#include "s3driver.h"

namespace dmlite {

class S3ConnectionFactory: public PoolElementFactory<S3Driver*> {
public:
  S3ConnectionFactory(const std::string& host, unsigned int port,
                         const std::string& s3AccessKeyID,
                         const std::string& s3SecretAccessKey);
  ~S3ConnectionFactory();

  S3Driver* create();
  void   destroy(S3Driver* conn);
  bool   isValid(S3Driver* conn);

  void   setS3SecretAccessKey(const std::string& key);

  /// The S3 host to connect to.
  std::string host_;

  /// The port to connect to.
  unsigned int port_;

  /// Obsolete.
  std::string bucket_;

  /// The Amazon Access ID.
  std::string s3AccessKeyID_;
protected:
private:
  
  /// The Secret Access Key.
  std::string s3SecretAccessKey_;

};


class S3Factory: public PoolDriverFactory {
public:
  S3Factory() throw (DmException);
  ~S3Factory() throw (DmException);
  void configure(const std::string& key, const std::string& value) throw (DmException);

  std::string implementedPool() throw();
  PoolDriver* createPoolDriver() throw (DmException);

  S3Driver* getConnection() throw (DmException);

  /// Release an S3 connection. If the reference counts drops to 0,
  /// the connection will be released on the pool.
  void releaseConnection(S3Driver*) throw (DmException);
protected:

private:
  /// Connection factory.
  S3ConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<S3Driver*> connectionPool_;
  
  /// Key used to keep only one connection per thread and factory
  pthread_key_t thread_s3_conn_;
};

};
#endif	// S3Factory_H
