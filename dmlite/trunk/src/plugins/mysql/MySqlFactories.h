/// @file   MySqlFactories.h
/// @brief  MySQL backend for libdm.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQL_H
#define	MYSQL_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/catalog.h>
#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/utils/poolcontainer.h>
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
class NsMySqlFactory: public INodeFactory, public AuthnFactory {
public:
  /// Constructor
  NsMySqlFactory() throw(DmException);
  /// Destructor
  ~NsMySqlFactory();

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  INode* createINode(PluginManager* pm) throw (DmException);
  Authn* createAuthn(PluginManager* pm) throw (DmException);
  
  PoolContainer<MYSQL*>& getPool(void) throw ();

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
  
  /// Map the host DN to root
  bool hostDnIsRoot_;

  /// Host DN
  std::string hostDn_;

private:
};



class DpmMySqlFactory: public NsMySqlFactory, public PoolManagerFactory {
public:
  /// Constructor
  DpmMySqlFactory() throw(DmException);
  
  /// Destructor
  ~DpmMySqlFactory();

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  PoolManager* createPoolManager(PluginManager* pm) throw (DmException);

protected:  
  /// DPM db.
  std::string dpmDb_;

  /// Admin username for replication.
  std::string adminUsername_;
};

};

#endif	// MYSQL_H
