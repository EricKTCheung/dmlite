/// @file    plugins/oracle/OracleFactories.h
/// @brief   Oracle backend for libdm.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef ORACLE_H
#define	ORACLE_H

#include <dmlite/dmlite++.h>
#include <dmlite/common/PoolContainer.h>
#include <dmlite/dummy/Dummy.h>
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
  INode* createINode(StackInstance*) throw(DmException);
  UserGroupDb* createUserGroupDb(StackInstance*) throw (DmException);
  
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

  /// The recursion limit following symbolic links.
  unsigned int symLinkLimit_;

private:
};


};

#endif	// ORACLE_H
