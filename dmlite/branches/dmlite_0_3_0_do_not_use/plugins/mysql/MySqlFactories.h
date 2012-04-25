/// @file    plugins/mysql/MySqlFactories.h
/// @brief   MySQL backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQL_H
#define	MYSQL_H

#include <dmlite/dmlite++.h>
#include <dmlite/common/PoolContainer.h>
#include <dmlite/dummy/Dummy.h>
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
class NsMySqlFactory: public CatalogFactory {
public:
  /// Constructor
  NsMySqlFactory() throw(DmException);
  /// Destructor
  ~NsMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  Catalog* createCatalog(StackInstance* si) throw(DmException);

protected:
  /// Connection factory.
  MySqlConnectionFactory connectionFactory_;

  /// Connection pool.
  PoolContainer<MYSQL*> connectionPool_;

  /// NS db.
  std::string nsDb_;

  /// The recursion limit following symbolic links.
  unsigned int symLinkLimit_;

private:
};



class DpmMySqlFactory: public NsMySqlFactory, public PoolManagerFactory {
public:
  /// Constructor
  DpmMySqlFactory() throw(DmException);
  
  /// Destructor
  ~DpmMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  Catalog* createCatalog(StackInstance* si) throw(DmException);
  PoolManager* createPoolManager(StackInstance* si) throw (DmException);

protected:
  /// DPM db.
  std::string dpmDb_;
};

};

#endif	// MYSQL_H
