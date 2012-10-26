/// @file   python/embedding/include/dmlite/python/python_catalog.h
/// @brief  Python Catalog API.
/// @author Martin Philipp Hellmich <martin.hellmich@cern.ch>
#ifndef DMLITE_PYTHON_PYTHONCATALOG_H
#define DMLITE_PYTHON_PYTHONCATALOG_H

#include <boost/python.hpp>

#include <dmlite/python/python_common.h>

#include <dirent.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <utime.h>
#include "base.h"
#include "exceptions.h"
#include "inode.h"
#include "utils/extensible.h"

namespace dmlite {

  // Forward declarations.
  class StackInstance;
  class PluginManager;
  
  /// Typedef for directories.
  struct Directory { ~Directory(); };
  
  /// Interface for Catalog (Namespaces).
  class PythonCatalog: public Catalog {
   public:    
    PythonCatalog(boost::python::object catalog_obj);
    /// Destructor.
    ~PythonCatalog();

    /// Change the working dir. Future not-absolute paths will use this as root.
    /// @param path The new working dir.
    void changeDir(const std::string& path) throw (DmException);
    
    /// Get the current working dir.
    /// @return The current working dir.
    std::string getWorkingDir(void) throw (DmException);

    /// Do an extended stat of a file or directory.
    /// @param path      The path of the file or directory.
    /// @param followSym If true, symlinks will be followed.
    /// @return          The extended status of the file.
    ExtendedStat extendedStat(const std::string& path,
                                      bool followSym = true) throw (DmException);

    /// Add a new replica for a file.
    /// @param replica Stores the data that is going to be added. fileid must
    ///                point to the id of the logical file in the catalog.
    void addReplica(const Replica& replica) throw (DmException);

    /// Delete a replica.
    /// @param replica The replica to remove.
    void deleteReplica(const Replica& replica) throw (DmException);

    /// Get replicas for a file.
    /// @param path The file for which replicas will be retrieved.
    std::vector<Replica> getReplicas(const std::string& path) throw (DmException);

    /// Creates a new symlink.
    /// @param path    The existing path.
    /// @param symlink The new access path.
    void symlink(const std::string& path,
                         const std::string& symlink) throw (DmException);
    
    /// Returns the path pointed by the symlink path
    /// @param path The symlink file.
    /// @return     The symlink target.
    std::string readLink(const std::string& path) throw (DmException);

    /// Remove a file.
    /// @param path The path to remove.
    void unlink(const std::string& path) throw (DmException);

    /// Creates an entry in the catalog.
    /// @param path The new file.
    /// @param mode The creation mode.
    void create(const std::string& path,
                        mode_t mode) throw (DmException);

    /// Sets the calling process’s file mode creation mask to mask & 0777.
    /// @param mask The new mask.
    /// @return     The value of the previous mask.
    mode_t umask(mode_t mask) throw ();

    /// Set the mode of a file.
    /// @param path The file to modify.
    /// @param mode The new mode as an integer (i.e. 0755)
    void setMode(const std::string& path,
                         mode_t mode) throw (DmException);

    /// Set the owner of a file.
    /// @param path   The file to modify.
    /// @param newUid The uid of the new owneer.
    /// @param newGid The gid of the new group.
    /// @param followSymLink If set to true, symbolic links will be followed.
    void setOwner(const std::string& path, uid_t newUid, gid_t newGid,
                          bool followSymLink = true) throw (DmException);

    /// Set the size of a file.
    /// @param path    The file to modify.
    /// @param newSize The new file size.
    void setSize(const std::string& path,
                         size_t newSize) throw (DmException);

    /// Set the checksum of a file.
    /// @param path      The file to modify.
    /// @param csumtype  The checksum type (CS, AD or MD).
    /// @param csumvalue The checksum value.
    void setChecksum(const std::string& path,
                             const std::string& csumtype,
                             const std::string& csumvalue) throw (DmException);

    /// Set the ACLs
    /// @param path The file to modify.
    /// @param acl  The Access Control List.
    void setAcl(const std::string& path,
                        const Acl& acl) throw (DmException);

    /// Set access and/or modification time.
    /// @param path The file path.
    /// @param buf  A struct holding the new times.
    void utime(const std::string& path,
                       const struct utimbuf* buf) throw (DmException);

    /// Get the comment associated with a file.
    /// @param path The file or directory.
    /// @return     The associated comment.
    std::string getComment(const std::string& path) throw (DmException);

    /// Set the comment associated with a file.
    /// @param path    The file or directory.
    /// @param comment The new comment.
    void setComment(const std::string& path,
                            const std::string& comment) throw (DmException);

    /// Set GUID of a file.
    /// @param path The file.
    /// @param guid The new GUID.
    void setGuid(const std::string& path,
                         const std::string &guid) throw (DmException);
    
    /// Update extended metadata on the catalog.
    /// @param path The file to update.
    /// @param attr The extended attributes struct.
    void updateExtendedAttributes(const std::string& path,
                                          const Extensible& attr) throw (DmException);

    /// Open a directory for reading.
    /// @param path The directory to open.
    /// @return     A pointer to a handle that can be used for later calls.
    Directory* openDir(const std::string& path) throw (DmException);

    /// Close a directory opened previously.
    /// @param dir The directory handle as returned by NsInterface::openDir.
    void closeDir(Directory* dir) throw (DmException);

    /// Read next entry from a directory (simple read).
    /// @param dir The directory handle as returned by NsInterface::openDir.
    /// @return    0x00 on failure or end of directory.
    struct dirent* readDir(Directory* dir) throw (DmException);

    /// Read next entry from a directory (stat information added).
    /// @param dir The directory handle as returned by NsInterface::openDir.
    /// @return    0x00 on failure (and errno is set) or end of directory.
    ExtendedStat* readDirx(Directory* dir) throw (DmException);

    /// Create a new empty directory.
    /// @param path The path of the new directory.
    /// @param mode The creation mode.
    void makeDir(const std::string& path,
                         mode_t mode) throw (DmException);

    /// Rename a file or directory.
    /// @param oldPath The old name.
    /// @param newPath The new name.
    void rename(const std::string& oldPath,
                        const std::string& newPath) throw (DmException);

    /// Remove a directory.
    /// @param path The path of the directory to remove.
    void removeDir(const std::string& path) throw (DmException);
    
    /// Get a replica.
    /// @param rfn The replica file name.
    Replica getReplica(const std::string& rfn) throw (DmException);

    /// Update a replica.
    /// @param replica The replica to modify.
    /// @return        0 on success, error code otherwise.
    void updateReplica(const Replica& replica) throw (DmException);

  private:
    PythonMain py;
  };

  /// Plug-ins must implement a concrete factory to be instantiated.
  class PythonCatalogFactory: public CatalogFactory {
   public:
    PythonCatalogFactory(std::string pymodule);
    /// Virtual destructor
    ~PythonCatalogFactory();

   protected:
    // Stack instance is allowed to instantiate catalogs
    friend class StackInstance;  

    /// Children of CatalogFactory are allowed to instantiate too (decorator)
    static PythonCatalog* createCatalog(PythonCatalogFactory* factory,
                                  PluginManager* pm) throw (DmException);

    /// Instantiate a implementation of Catalog
    PythonCatalog* createCatalog(PluginManager* pm) throw (DmException);

  private:
    PythonMain py;
  };

};

#endif // DMLITE_CPP_CATALOG_H
