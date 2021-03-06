/// @file   include/dmlite/cpp/inode.h
/// @brief  Low-level access API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITE_CPP_INODE_H
#define DMLITE_CPP_INODE_H

#include "dmlite/common/config.h"
#include "base.h"
#include "exceptions.h"
#include "status.h"
#include "utils/extensible.h"
#include "utils/security.h"
#include "utils/checksums.h"

#include <dirent.h>
#include <utime.h>
#include <string>
#include <vector>

namespace dmlite {

  // Forward declarations.
  class StackInstance;

  /// Typedef for directories.
  struct IDirectory { virtual ~IDirectory(); };

  /// File/directory metadata.
    struct ExtendedStat: public Extensible {
      enum FileStatus { kOnline = '-',
                        kMigrated = 'm'
                      };

      ino_t         parent;
      struct stat stat;
      FileStatus    status;
      std::string   name;
      std::string   guid;
      std::string   csumtype;
      std::string   csumvalue;
      Acl           acl;

      bool operator == (const ExtendedStat&) const;
      bool operator != (const ExtendedStat&) const;
      bool operator <  (const ExtendedStat&) const;
      bool operator >  (const ExtendedStat&) const;

      void fixchecksums();

      /// gets a checksum of type csumtype
      /// if csumtype is empty, then it gets the legacy one (i.e. the easiest to get)
      /// Please note that this function recognizes long checksum name
      /// e.g. "adler32" , which internally will be looked up as "checksum.adler32'
      int getchecksum(std::string &cktype, std::string &ckvalue);

    };

  /// Symbolic link
  struct SymLink: public Extensible {
    ino_t       inode;
    std::string link;

    bool operator == (const SymLink&) const;
    bool operator != (const SymLink&) const;
    bool operator <  (const SymLink&) const;
    bool operator >  (const SymLink&) const;
  };

  /// File replica metadata
  struct Replica: public Extensible {
    enum ReplicaStatus { kAvailable      = '-',
                         kBeingPopulated = 'P',
                         kToBeDeleted    = 'D'
                       };
    enum ReplicaType   { kVolatile  = 'V',
                         kPermanent = 'P'
                       };

    int64_t    replicaid;
    int64_t    fileid;

    int64_t    nbaccesses;
    time_t     atime;
    time_t     ptime;
    time_t     ltime;

    ReplicaStatus status;
    ReplicaType   type;

    /// Historical field containing the uuid of the spacetoken that was chosen when
    /// writing the replica. This is used for accounting
    std::string setname;

    std::string server;
    std::string rfn;

    bool operator == (const Replica&) const;
    bool operator != (const Replica&) const;
    bool operator <  (const Replica&) const;
    bool operator >  (const Replica&) const;
  };

  /// Low-level interface. Based on i-nodes.
  /// @note Security checks NOT done on this level.
  class INode: public virtual BaseInterface {
   public:
    /// Destructor
    virtual ~INode();

    /// Start a transaction
    virtual void begin(void) throw (DmException);

    /// Commit a transaction
    virtual void commit(void) throw (DmException);

    /// Rollback changes
    virtual void rollback(void) throw (DmException);

    /// Create a new file or directory
    /// @param f  The file that will be inserted. Its fields must be initialized.
    /// @return   An stat of the created file.
    virtual ExtendedStat create(const ExtendedStat& f) throw (DmException);

    /// Create or modify the file inode to point to another file.
    /// @param inode The file to modify.
    /// @param link  The new symbolic link.
    /// @note This does NOT create the file. Use create first.
    virtual void symlink(ino_t inode, const std::string &link) throw (DmException);

    /// Remove a file or directory. It will fail if it is a directory and it is not empty,
    /// or if it a file and it has replicas.
    /// @param inode The inode of the file.
    /// @note This will check for non empty directories.
    /// @note This will remove associated comments and replicas.
    virtual void unlink(ino_t inode) throw (DmException);

    /// Move a file between two directories.
    /// @param inode  File to be moved.
    /// @param dest   The new parent.
    virtual void move(ino_t inode, ino_t dest) throw (DmException);

    /// Change the name of a file.
    /// @param inode The inode of the file.
    /// @param name  New name.
    virtual void rename(ino_t inode, const std::string& name) throw (DmException);

    /// Do an extended stat of an entry using its inode.
    /// @param inode The inode of the file.
    /// @return      The extended status of the file.
    virtual ExtendedStat extendedStat(ino_t inode) throw (DmException);

    /// Do an extended stat of an entry using its inode, exception-safe version.
    /// @param xstat The extended status of the file.
    /// @param inode The inode of the file.
    /// @return      The status of the operation.
    virtual DmStatus extendedStat(ExtendedStat &xstat, ino_t inode) throw (DmException);

    /// Do an extended stat of an entry using the parent inode and the name.
    /// @param parent The parent inode.
    /// @param name   The file or directory name.
    /// @note         No security check will be done.
    virtual ExtendedStat extendedStat(ino_t parent,
                                      const std::string& name) throw (DmException);

    /// Do an extended stat of an entry using the parent inode and the name, exception-safe version.
    /// @param xstat The extended status of the file.
    /// @param parent The parent inode.
    /// @param name   The file or directory name.
    /// @return       The status of the operation.
    /// @note         No security check will be done.
    virtual DmStatus extendedStat(ExtendedStat &xstat, ino_t parent,
                                      const std::string& name) throw (DmException);

    /// Do an extended stat using the GUID.
    /// @param guid The file GUID.
    virtual ExtendedStat extendedStat(const std::string& guid) throw (DmException);

    /// Get the symlink associated with a inode.
    /// @param inode The inode of the file.
    /// @return      A SymLink struct.
    /// @note        If inode is not a symlink, an exception will be thrown.
    virtual SymLink readLink(ino_t inode) throw (DmException);

    /// Add a new replica for a file.
    /// @param replica Stores the data that is going to be added. fileid must
    ///                point to the id of the logical file in the catalog.
    virtual void addReplica(const Replica& replica) throw (DmException);

    /// Delete a replica.
    /// @param replica The replica to remove.
    virtual void deleteReplica(const Replica& replica) throw (DmException);

    /// Get a replica using the replica ID.
    /// @param rid The replica ID.
    virtual Replica getReplica(int64_t rid) throw (DmException);

    /// Get a replica.
    /// @param rfn The replica to retrieve.
    virtual Replica getReplica(const std::string& rfn) throw (DmException);

    /// Modify a replica.
    /// @param replica The replica data.
    virtual void updateReplica(const Replica& replica) throw (DmException);

    /// Get replicas for a file.
    /// @param inode The entry inode.
    virtual std::vector<Replica> getReplicas(ino_t inode) throw (DmException);

    /// Change access and/or modification time.
    /// @param inode The inode of the file.
    /// @param buf   A struct holding the new times.
    virtual void utime(ino_t inode,
                       const struct utimbuf* buf) throw (DmException);

    /// Set the mode of a file.
    /// @param inode The inode of the file.
    /// @param uid   The owner. If -1, not changed.
    /// @param gid   The group. If -1, not changed.
    /// @param mode  The new mode. S_IFMT bits are cleared, and kept as they
    ///              are in the DB.
    /// @param acl   The new ACL. If empty, not changed.
    virtual void setMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode,
                         const Acl& acl) throw (DmException);

    /// Set the size of a file.
    /// @param inode The inode of the file.
    /// @param size  The new size.
    virtual void setSize(ino_t inode, size_t size) throw (DmException);

    /// Set the checksum of a file.
    /// @param inode     The inode of the file.
    /// @param csumtype  The checksum type.
    /// @param csumvalue The checksum value.
    virtual void setChecksum(ino_t inode, const std::string& csumtype,
                             const std::string& csumvalue) throw (DmException);

    /// Get the comment associated to a file.
    /// @param inode The inode of the file.
    /// @return The comment.
    virtual std::string getComment(ino_t inode) throw (DmException);

    /// Set the comment associated to a file.
    /// @param inode   The inode of the file.
    /// @param comment The new comment.
    virtual void setComment(ino_t inode,
                            const std::string& comment) throw (DmException);

    /// Remove the associated comment.
    /// @param inode The file whose comment will be removed.
    virtual void deleteComment(ino_t inode) throw (DmException);

    /// Set the GUID of a file.
    /// @param inode The inode of the file.
    /// @param guid  The new GUID.
    virtual void setGuid(ino_t inode,
                         const std::string& guid) throw (DmException);

    /// Update extended metadata on the catalog.
    /// @param attr The extended attributes struct.
    virtual void updateExtendedAttributes(ino_t inode,
                                          const Extensible& attr) throw (DmException);

    /// Open a directory.
    /// @param inode The inode of the directory.
    /// @return An opaque pointer to a directory.
    virtual IDirectory* openDir(ino_t inode) throw (DmException);

    /// Close a directory.
    /// @param dir The opaque structure to close.
    virtual void closeDir(IDirectory* dir) throw (DmException);

    /// Read the next entry.
    /// @param dir The opaque structure of a directory.
    /// @return NULL when finished. Extended stat of the next entry otherwise.
    virtual ExtendedStat* readDirx(IDirectory* dir) throw (DmException);

    /// Read the next entry.
    /// @param dir The opaque structure of a directory.
    /// @return NULL when finished. Extended stat of the next entry otherwise.
    virtual struct dirent* readDir (IDirectory* dir) throw (DmException);
  };

  /// INodeFactory
  class INodeFactory: public virtual BaseFactory {
   public:
    /// Destructor
    virtual ~INodeFactory();

   protected:
    // Stack instance is allowed to instantiate INodes
    friend class StackInstance;

    /// Children of INodeFactory are allowed to instantiate too (decorator)
    static INode* createINode(INodeFactory* factory,
                              PluginManager* pm) throw (DmException);

    /// Instantiate a implementation of INode
    virtual INode* createINode(PluginManager* pm) throw (DmException);
  };




  /// Convenience class that releases a resource on destruction
  class InodeTrans {
  public:
    InodeTrans(INode *o)
    {
      obj = o;
      obj->begin();
    }

    ~InodeTrans() {
      if (obj != 0) obj->rollback();
      obj = 0;
    }

    void Commit() {
      if (obj != 0) obj->commit();
      obj = 0;
    }

  private:
    INode *obj;

  };



};

#endif // DMLITE_CPP_INODE_H
