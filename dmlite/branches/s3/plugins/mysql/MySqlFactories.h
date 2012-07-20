/// @file    plugins/mysql/MySqlFactories.h
/// @brief   MySQL backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQL_H
#define	MYSQL_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/dummy/Dummy.h>
#include <dmlite/cpp/utils/dm_poolcontainer.h>
#include <mysql/mysql.h>


namespace dmlite {

/// Factory for Pool resources
class MySqlConnectionFactory: public PoolElementFactory<MYSQL*> {
public:
  MySqlConnectionFactory(const std::string& host, unsigned int port,
                         const std::string& user, const std::string& passwd);
  ~MySqlConnectionFactory();

  MYSQL* create();
  void   destroy(MYSQL*);
  bool   isValid(MYSQL*);

  // Attributes
  std::string  host;
  unsigned int port;
  std::string  user;
  std::string  passwd;

protected:
private:
};

/// Concrete factory for DPNS/LFC.
class NsMySqlFactory: public INodeFactory, public UserGroupDbFactory {
public:
  /// Constructor
  NsMySqlFactory() throw(DmException);
  /// Destructor
  ~NsMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  INode*       createINode(PluginManager* pm)       throw (DmException);
  UserGroupDb* createUserGroupDb(PluginManager* pm) throw (DmException);
  
  /// Get a MYSQL connection. It will allocate only one per thread from the
  /// pool, and just increase the reference count on later requests from the same
  /// thread.
  MYSQL* getConnection(void)     throw (DmException);
  
  /// Release a MYSQL connection. If the reference counts drops to 0,
  /// the connection will be released on the pool.
  void releaseConnection(MYSQL*) throw (DmException);

protected:
  /// Connection factory.
  MySqlConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<MYSQL*> connectionPool_;
  
  /// Key used to keep only one connection per thread and factory
  pthread_key_t thread_mysql_conn_;

  /// NS db.
  std::string nsDb_;
  
  /// Mapfile
  std::string mapFile_;

private:
};



class DpmMySqlFactory: public NsMySqlFactory, public PoolManagerFactory {
public:
  /// Constructor
  DpmMySqlFactory() throw(DmException);
  
  /// Destructor
  ~DpmMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  PoolManager* createPoolManager(PluginManager* pm) throw (DmException);

protected:  
  /// DPM db.
  std::string dpmDb_;
};

};

#endif	// MYSQL_H
