/// @file   MySqlFactories.h
/// @brief  MySQL backend for dmlite.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef MYSQL_H
#define	MYSQL_H

#include "dmlite/cpp/dmlite.h"
#include "dmlite/cpp/catalog.h"
#include "dmlite/cpp/poolmanager.h"
#include "dmlite/cpp/io.h"
#include "dmlite/cpp/utils/poolcontainer.h"
#include <mysql/mysql.h>
#include "utils/mysqlpools.h"

namespace dmlite {

  
extern Logger::bitmask mysqllogmask;
extern Logger::component mysqllogname;


/// Concrete dmlite factory for name space stuff, e.g. DPNS/LFC.
class NsMySqlFactory: public INodeFactory, public AuthnFactory {
public:
  /// Constructor
  NsMySqlFactory() throw(DmException);
  /// Destructor
  ~NsMySqlFactory();

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  virtual INode* createINode(PluginManager* pm) throw (DmException);
  virtual Authn* createAuthn(PluginManager* pm) throw (DmException);
  
  
  int dirspacereportdepth;
protected:

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


/// Concrete dmlite factory for disk pool manager stuff
class DpmMySqlFactory: public NsMySqlFactory, public PoolManagerFactory {
public:
  /// Constructor
  DpmMySqlFactory() throw(DmException);
  
  /// Destructor
  ~DpmMySqlFactory();

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  virtual PoolManager* createPoolManager(PluginManager* pm) throw (DmException);

protected:  
  /// DPM db.
  std::string dpmDb_;

  /// Admin username for replication.
  std::string adminUsername_;
};



class MysqlIOPassthroughFactory: public IODriverFactory {
public:
  /// Constructor
  MysqlIOPassthroughFactory(IODriverFactory *ioFactory) throw(DmException);
  
  /// Destructor
  ~MysqlIOPassthroughFactory() {}

  void configure(const std::string& key, const std::string& value) throw(DmException);
  
  virtual IODriver* createIODriver(PluginManager* pm) throw (DmException);
  
protected:  
  /// DPM db.
  std::string dpmDb_;

  /// Admin username for replication.
  std::string adminUsername_;
  
  
  int dirspacereportdepth;
  
  /// The decorated IODriver factory
  IODriverFactory* nestedIODriverFactory_;
};

};



#endif	// MYSQL_H
