/// @file    plugins/oracle/OracleFactories.h
/// @brief   Oracle backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef ORACLE_H
#define	ORACLE_H

#include <dmlite/cpp/dmlite.h>
#include <dmlite/cpp/dummy/Dummy.h>
#include <dmlite/cpp/utils/dm_poolcontainer.h>
#include <occi.h>


namespace dmlite {

/// Concrete factory for DPNS/LFC.
class NsOracleFactory: public INodeFactory, public UserGroupDbFactory {
public:
  /// Constructor
  NsOracleFactory() throw(DmException);
  /// Destructor
  ~NsOracleFactory() throw(DmException);

  void configure(const std::string& key, const std::string& value) throw(DmException);
  INode* createINode(PluginManager*) throw(DmException);
  UserGroupDb* createUserGroupDb(PluginManager*) throw (DmException);
  
protected:
  /// NS db.
  std::string nsDb_;
  /// User
  std::string user_;
  /// Password
  std::string passwd_;
  
  /// Oracle environment
  oracle::occi::Environment* env_;

  /// Oracle connection pool
  oracle::occi::ConnectionPool* pool_;

  /// Minimum pool size
  unsigned int minPool_;

  /// Maximum pool size
  unsigned int maxPool_;
  
  /// Mapfile
  std::string mapFile_;

private:
};


};

#endif	// ORACLE_H
