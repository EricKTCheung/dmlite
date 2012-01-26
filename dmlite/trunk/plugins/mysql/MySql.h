/// @file    plugins/mysql/MySql.h
/// @brief   MySQL backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQL_H
#define	MYSQL_H

#include <dmlite/dmlite++.h>
#include <dmlite/dummy/Dummy.h>
#include <mysql/mysql.h>

#include "../common/PoolContainer.h"

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

/// Lock and transaction handler
/// Used to avoid manual tracking of exit paths, so any destruction
/// before a commit() call will be a rollback.
class TransactionAndLock {
public:
  TransactionAndLock(MYSQL* conn, ...) throw (DmException);
  ~TransactionAndLock() throw (DmException);

  void commit(void) throw (DmException);
  void rollback(void) throw (DmException);
private:
  bool   pending;
  MYSQL *connection;
};

/// Concrete factory for DPNS/LFC.
class NsMySqlFactory: public CatalogFactory {
public:
  /// Constructor
  NsMySqlFactory(CatalogFactory* catalogFactory) throw(DmException);
  /// Destructor
  ~NsMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  Catalog* createCatalog() throw(DmException);

protected:
  /// Decorated
  CatalogFactory* nestedFactory_;

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



class DpmMySqlFactory: public NsMySqlFactory {
public:
  /// Constructor
  DpmMySqlFactory(CatalogFactory* catalogFactory) throw(DmException);
  /// Destructor
  ~DpmMySqlFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  Catalog* createCatalog() throw(DmException);

protected:
  /// DPM db.
  std::string dpmDb_;
private:
};

};

#endif	// MYSQL_H
