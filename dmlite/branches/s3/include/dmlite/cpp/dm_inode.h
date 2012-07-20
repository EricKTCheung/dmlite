/// @file   include/dmlite/dm_inode.h
/// @brief  Low-level access API.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef DMLITEPP_INODE_H
#define	DMLITEPP_INODE_H

#include <cstdarg>
#include <string>
#include <vector>
#include <utime.h>
#include "dm_auth.h"
#include "dm_exceptions.h"
#include "../common/dm_types.h"

namespace dmlite {
  
/// Typedef for directories
typedef void IDirectory;

// Advanced declaration
class StackInstance;

/// Low-level interface. Based on i-nodes.
/// @note Security checks NOT done on this level.
class INode {
public:
  /// Destructor
  virtual ~INode();
  
  /// String ID of the inode implementation.
  virtual std::string getImplId(void) throw() = 0;
  
  /// Set the StackInstance.
  /// Some plugins may need to access other stacks (i.e. the pool may need the catalog)
  /// However, at construction time not all the stacks have been populated, so this will
  /// be called once all are instantiated.
  virtual void setStackInstance(StackInstance* si) throw (DmException) = 0;
  
  /// Set the security context.
  virtual void setSecurityContext(const SecurityContext* ctx) throw (DmException) = 0;
  
  /// Start a transaction
  virtual void begin(void) throw (DmException) = 0;
  
  /// Commit a transaction
  virtual void commit(void) throw (DmException) = 0;
  
  /// Rollback changes
  virtual void rollback(void) throw (DmException) = 0;
  
  /// Create a new file or directory
  /// @param parent    Parent id
  /// @param name      New file name
  /// @param uid       The owner user id.
  /// @param gid       The group id.
  /// @param mode      Creation mode. For directories, pass S_IFDIR & mode
  /// @param size      File size in bytes.
  /// @param type      File type.
  /// @param status    File status.
  /// @param csumtype  Checksum type. Only 2 bytes are stored!
  /// @param csumvalue Checksum value. Only 32 bytesa re stored!
  /// @param acl       Access Control List. This method will NOT proccess inheritance.
  /// @return          An stat of the created file.
  virtual ExtendedStat create(ino_t parent, const std::string& name,
                              uid_t uid, gid_t gid, mode_t mode,
                              size_t size, short type, char status,
                              const std::string& csumtype, const std::string& csumvalue,
                              const std::string& acl) throw (DmException) = 0;
  
  /// Create or modify the file inode to point to another file.
  /// @param inode The file to modify.
  /// @param link  The new symbolic link.
  /// @note This does NOT create the file. Use create first.
  virtual void symlink(ino_t inode, const std::string &link) throw (DmException) = 0;
  
  /// Remove a file or directory. It will fail if it is a directory and it is not empty,
  /// or if it a file and it has replicas.
  /// @param inode The inode of the file.
  /// @note This will check for non empty directories.
  /// @note This will remove associated comments and replicas.
  virtual void unlink(ino_t inode) throw (DmException) = 0;
  
  /// Move a file between two directories.
  /// @param inode  File to be moved.
  /// @param dest   The new parent.
  virtual void move(ino_t inode, ino_t dest) throw (DmException) = 0;
  
  /// Change the name of a file.
  /// @param inode The inode of the file.
  /// @param name  New name.
  virtual void rename(ino_t inode, const std::string& name) throw (DmException) = 0;
  
  /// Do an extended stat of en entry using its inode.
  /// @param inode The inode of the file.
  /// @return      The extended status of the file.
  virtual ExtendedStat extendedStat(ino_t inode) throw (DmException) = 0;
  
  /// Do an extended stat of an entry using the parent inode and the name.
  /// @param parent The parent inode.
  /// @param name   The file or directory name.
  /// @note         No security check will be done.
  virtual ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException) = 0;
  
  /// Do an extended stat using the GUID.
  /// @param guid The file GUID.
  virtual ExtendedStat extendedStat(const std::string& guid) throw (DmException) = 0;
  
  /// Get the symlink associated with a inode.
  /// @param inode The inode of the file.
  /// @return      A SymLink struct.
  /// @note        If inode is not a symlink, an exception will be thrown.
  virtual SymLink readLink(ino_t inode) throw (DmException) = 0;
  
  /// Add a new replica for a file.
  /// @param inode      The inode of the file.
  /// @param server     The SE that hosts the file (if empty, it will be retrieved from the sfn).
  /// @param sfn        The SURL or physical path of the replica being added.
  /// @param status     '-' for available, 'P' for being populated, 'D' for being deleted.
  /// @param fileType   'V' for volatile, 'D' for durable, 'P' for permanent.
  /// @param poolName   The pool where the replica is (not used for LFCs)
  /// @param fileSystem The filesystem where the replica is (not used for LFCs)
  virtual void addReplica(ino_t inode, const std::string& server,
                          const std::string& sfn, char status, char fileType,
                          const std::string& poolName, const std::string& fileSystem) throw (DmException) = 0;
  
  /// Delete a replica.
  /// @param inode The inode of the file.
  /// @param sfn   The replica being removed.
  virtual void deleteReplica(ino_t inode,
                             const std::string& sfn) throw (DmException) = 0;
  
  /// Get a replica.
  /// @param rfn The replica to retrieve.
  virtual FileReplica getReplica(const std::string& rfn) throw (DmException) = 0;
  
  /// Modify a replica.
  /// @param replica The replica data.
  virtual void setReplica(const FileReplica& replica) throw (DmException) = 0;
  
  /// Get replicas for a file.
  /// @param inode The entry inode.
  virtual std::vector<FileReplica> getReplicas(ino_t inode) throw (DmException) = 0;
  
  /// Change access and/or modification time.
  /// @param inode The inode of the file.
  /// @param buf   A struct holding the new times.
  virtual void utime(ino_t inode, const struct utimbuf* buf) throw (DmException) = 0;
  
  /// Change the mode of a file.
  /// @param inode The inode of the file.
  /// @param uid   The owner.
  /// @param gid   The group.
  /// @param mode  The new mode.
  /// @param acl   The new ACL.
  virtual void changeMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const std::string& acl) throw (DmException) = 0;
  
  /// Change the size of a file.
  /// @param inode The inode of the file.
  /// @param size  The new size.
  virtual void changeSize(ino_t inode, size_t size) throw (DmException) = 0;
  
  /// Change the checksum of a file.
  /// @param inode     The inode of the file.
  /// @param csumtype  The checksum type.
  /// @param csumvalue The checksum value.
  virtual void changeChecksum(ino_t inode, const std::string& csumtype, const std::string& csumvalue) throw (DmException) = 0;
  
  /// Get the comment associated to a file.
  /// @param inode The inode of the file.
  /// @return The comment.
  virtual std::string getComment(ino_t inode) throw (DmException) = 0;
  
  /// Change the comment associated to a file.
  /// @param inode   The inode of the file.
  /// @param comment The new comment.
  virtual void setComment(ino_t inode, const std::string& comment) throw (DmException) = 0;
  
  /// Remove the associated comment.
  /// @param inode The file whose comment will be removed.
  virtual void deleteComment(ino_t inode) throw (DmException) = 0;
  
  /// Set the GUID of a file.
  /// @param inode The inode of the file.
  /// @param guid  The new GUID.
  virtual void setGuid(ino_t inode, const std::string& guid) throw (DmException) = 0;
  
  /// Open a directory.
  /// @param inode The inode of the directory.
  /// @return An opaque pointer to a directory.
  virtual IDirectory* openDir(ino_t inode) throw (DmException) = 0;
  
  /// Close a directory.
  /// @param dir The opaque structure to close.
  virtual void closeDir(IDirectory* dir) throw (DmException) = 0;
  
  /// Read the next entry.
  /// @param dir The opaque structure of a directory.
  /// @return NULL when finished. Extended stat of the next entry otherwise.
  virtual ExtendedStat* readDirx(IDirectory* dir) throw (DmException) = 0;
  
  /// Read the next entry.
  /// @param dir The opaque structure of a directory.
  /// @return NULL when finished. Extended stat of the next entry otherwise.
  virtual struct dirent* readDir (IDirectory* dir) throw (DmException) = 0;
};

/// INodeFactory
class INodeFactory: public virtual BaseFactory {
public:
  /// Destructor
  virtual ~INodeFactory();
  
  /// Set a configuration parameter
  /// @param key   The configuration parameter
  /// @param value The value for the configuration parameter
  virtual void configure(const std::string& key, const std::string& value) throw (DmException) = 0;

protected:
  // Stack instance is allowed to instantiate INodes
  friend class StackInstance;  
  
  /// Children of INodeFactory are allowed to instantiate too (decorator)
  static INode* createINode(INodeFactory* factory, PluginManager* pm) throw (DmException);
  
  /// Instantiate a implementation of INode
  virtual INode* createINode(PluginManager* pm) throw (DmException) = 0;

private:
};
  
};

#endif	// DMLITEPP_INODE_H
