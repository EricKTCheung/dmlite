/// @file    plugins/mysql/DpmMySql.h
/// @brief   MySQL DPM Implementation.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DPMMYSQL_H
#define	DPMMYSQL_H

#include "NsMySql.h"

namespace dmlite {

const int FS_ENABLED  = 0;
const int FS_DISABLED = 1;
const int FS_RDONLY   = 2;

/// Struct used internally to store information about a DPM
/// filesystem.
struct DpmFileSystem {
  char poolName[16];           ///< The pool name.
  char server  [HOST_NAME_MAX];///< The server where the filesystem is.
  char fs      [80];           ///< The file system whithin the server.
  int  status;                 ///< The status of the filesystem.
  int  weight;                 ///< The associated weight.
};

/// Implementation of DPM MySQL backend.
class DpmMySqlCatalog: public NsMySqlCatalog {
public:

  /// Constructor
  /// @param conn      The MySQL connection pool.
  /// @param nsDb      The MySQL DB name for the NS.
  /// @param dpmDb     The MySQL DB name for DPM.
  /// @param decorates The underlying decorated catalog.
  /// @param symLimit  The recursion limit for symbolic links.
  DpmMySqlCatalog(PoolContainer<MYSQL*>* connPool,
                  const std::string& nsDb, const std::string& dpmDb,
                  Catalog* decorates, unsigned int symLimit) throw(DmException);

  /// Destructor
  ~DpmMySqlCatalog() throw (DmException);

  // Overloading
  std::string getImplId(void) throw ();

  void set(const std::string& key, va_list varg) throw (DmException);
  
  FileReplica get (const std::string&) throw (DmException);

  void unlink (const std::string&) throw (DmException);

  std::string put      (const std::string&, Uri*)                     throw (DmException);
  std::string put      (const std::string&, Uri*, const std::string&) throw (DmException);
  void        putStatus(const std::string&, const std::string&, Uri*) throw (DmException);
  void        putDone  (const std::string&, const std::string&)       throw (DmException);
  
  int getFsStatus(const std::string&, const std::string& server, const std::string& fs) throw(DmException);

  void setSecurityCredentials(const SecurityCredentials&) throw (DmException);
  void setSecurityContext(const SecurityContext&);

protected:
private:
  /// DPM DB.
  std::string dpmDb_;

  /// Decorated catalog
  Catalog* decorated_;

  // Private methods

  /// Returns the preparted statement with the specified ID.
  /// @param stId The statement ID (see STMT_*)
  /// @return     A pointer to a MySQL statement.
  MYSQL_STMT* getPreparedStatement(unsigned stId);

};

};

#endif	// DPMMYSQL_H

