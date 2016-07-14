/// @file   NsMySql.h
/// @brief  MySQL NS Implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef NSMYSQL_H
#define	NSMYSQL_H

#include <dirent.h>
#include <dmlite/cpp/utils/poolcontainer.h>
#include <dmlite/cpp/inode.h>
#include <mysql/mysql.h>
#include <vector>

#include "utils/MySqlWrapper.h"

namespace dmlite {

  /// Struct used internally to bind when reading
  struct CStat {
    ino_t       parent;
    struct stat stat;
    char        status;
    short       type;
    char        name[256];
    char        guid[37];
    char        csumtype[4];
    char        csumvalue[34];
    char        acl[300 * 13]; // Maximum 300 entries of 13 bytes each
    char        xattr[1024];
  };

  /// Struct used internally to read drectories.
  struct NsMySqlDir: public IDirectory {
    virtual ~NsMySqlDir() {};

    ExtendedStat  dir;           ///< Directory being read.
    CStat         cstat;         ///< Used for the binding
    ExtendedStat  current;       ///< Current entry metadata.
    struct dirent ds;            ///< The structure used to hold the returned data.
    Statement    *stmt;          ///< The statement.
    bool          eod;           ///< True when end of dir is reached.
    MYSQL *conn_;                ///< The connection the statement is relying on.
  };

  // Forward declaration
  class NsMySqlFactory;

  /// Implementation of INode MySQL backend.
  class INodeMySql: public INode {
   public:

    /// Constructor
    INodeMySql(NsMySqlFactory* factory,
              const std::string& db) throw (DmException);

    /// Destructor
    ~INodeMySql();

    // Overloading
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);
    void setSecurityContext(const SecurityContext* ctx) throw (DmException);

    void begin(void) throw (DmException);
    void commit(void) throw (DmException);
    void rollback(void) throw (DmException);

    ExtendedStat create(const ExtendedStat&) throw (DmException);

    void symlink(ino_t inode, const std::string &link) throw (DmException);

    void unlink(ino_t inode) throw (DmException);

    void move  (ino_t inode, ino_t dest) throw (DmException);
    void rename(ino_t inode, const std::string& name) throw (DmException);

    ExtendedStat extendedStat(ino_t inode) throw (DmException);
    ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException);
    ExtendedStat extendedStat(const std::string& guid) throw (DmException);

    DmStatus extendedStat(ExtendedStat &xstat, ino_t inode) throw (DmException);
    DmStatus extendedStat(ExtendedStat &xstat, ino_t parent, const std::string& name) throw (DmException);

    SymLink readLink(ino_t inode) throw (DmException);

    void addReplica   (const Replica&) throw (DmException);
    void deleteReplica(const Replica&) throw (DmException);

    std::vector<Replica> getReplicas(ino_t inode) throw (DmException);

    Replica  getReplica   (int64_t rid) throw (DmException);

    DmStatus getReplica   (Replica &r, const std::string& sfn) throw (DmException);
    Replica getReplica    (const std::string& sfn) throw (DmException);

    void    updateReplica(const Replica& replica) throw (DmException);

    void utime(ino_t inode, const struct utimbuf* buf) throw (DmException);

    void setMode(ino_t inode, uid_t uid, gid_t gid,
                mode_t mode, const Acl& acl) throw (DmException);

    void setSize    (ino_t inode, size_t size) throw (DmException);
    void setChecksum(ino_t inode, const std::string& csumtype,
                                 const std::string& csumvalue) throw (DmException);

    std::string getComment   (ino_t inode) throw (DmException);
    void        setComment   (ino_t inode, const std::string& comment) throw (DmException);
    void        deleteComment(ino_t inode) throw (DmException);

    void setGuid(ino_t inode, const std::string& guid) throw (DmException);

    void updateExtendedAttributes(ino_t inode,
                                  const Extensible& attr) throw (DmException);

    IDirectory*    openDir (ino_t inode) throw (DmException);
    void           closeDir(IDirectory* dir) throw (DmException);
    ExtendedStat*  readDirx(IDirectory* dir) throw (DmException);
    struct dirent* readDir (IDirectory* dir) throw (DmException);

   protected:
    /// The corresponding factory.
    NsMySqlFactory* factory_;

    /// Transaction level, so begins and commits can be nested.
    unsigned transactionLevel_;

   private:
    /// NS DB.
    std::string nsDb_;

    // Connection
    MYSQL *conn_;


  };

  /// Convenience class that releases a resource on destruction
  class InodeMySqlTrans {
  public:
    InodeMySqlTrans(INodeMySql *o)
    {
      obj = o;
      obj->begin();
    }

    ~InodeMySqlTrans() {
      if (obj != 0) obj->rollback();
      obj = 0;
    }

    void Commit() {
      if (obj != 0) obj->commit();
      obj = 0;
    }

  private:
    INodeMySql *obj;

  };


};

#endif	// NSMYSQL_H
