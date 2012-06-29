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

protected:
  /// Connection factory.
  MySqlConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<MYSQL*> connectionPool_;

  /// NS db.
  std::string nsDb_;
  
  /// Mapfile
  std::string mapFile_;

private:
};



class DpmMySqlFactory: public PoolManagerFactory {
public:
  /// Constructor
  DpmMySqlFactory() throw(DmException);
  
  /// Destructor
  ~DpmMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  PoolManager* createPoolManager(PluginManager* pm) throw (DmException);

protected:
  /// Connection factory.
  MySqlConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<MYSQL*> connectionPool_;
  
  /// DPM db.
  std::string dpmDb_;
};

};

#endif	// MYSQL_H
