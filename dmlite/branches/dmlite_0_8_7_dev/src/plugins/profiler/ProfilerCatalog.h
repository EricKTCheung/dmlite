/// @file   ProfilerCatalog.h
/// @brief  Profiler plugin.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILERCATALOG_H
#define	PROFILERCATALOG_H

#include "ProfilerXrdMon.h"

#include <dmlite/cpp/catalog.h>

namespace dmlite {

  // Profiler catalog.
  class ProfilerCatalog: public Catalog, private ProfilerXrdMon
  {
   public:

    /// Constructor.
    /// @param decorates The underlying decorated catalog.
    ProfilerCatalog(Catalog* decorates) throw (DmException);

    /// Destructor.
    ~ProfilerCatalog();

    // Overloading
    std::string getImplId(void) const throw ();

    void setStackInstance(StackInstance* si) throw (DmException);

    void setSecurityContext(const SecurityContext*) throw (DmException);

    void        changeDir     (const std::string&) throw (DmException);
    std::string getWorkingDir (void)               throw (DmException);

    DmStatus extendedStat(ExtendedStat &xstat, const std::string& path,
                          bool followSym = true) throw (DmException);
                          
    ExtendedStat extendedStat(const std::string&, bool) throw (DmException);
    ExtendedStat extendedStatByRFN(const std::string& rfn) throw (DmException);

    bool access(const std::string& path, int mode) throw (DmException);
    bool accessReplica(const std::string& replica, int mode) throw (DmException);

    void addReplica(const Replica&) throw (DmException);
    void deleteReplica(const Replica&) throw (DmException);
    std::vector<Replica> getReplicas(const std::string&) throw (DmException);

    void        symlink (const std::string&, const std::string&) throw (DmException);
    std::string readLink(const std::string& path) throw (DmException);

    void unlink(const std::string&)                     throw (DmException);

    void create(const std::string&, mode_t) throw (DmException);

    mode_t umask          (mode_t)                           throw ();
    void   setMode     (const std::string&, mode_t)       throw (DmException);
    void   setOwner    (const std::string&, uid_t, gid_t, bool) throw (DmException);

    void setSize    (const std::string&, size_t) throw (DmException);
    void setChecksum(const std::string&, const std::string&, const std::string&) throw (DmException);

    void getChecksum(const std::string& path, const std::string& csumtype,
                     std::string& csumvalue, const std::string& pfn,
                     const bool forcerecalc = false, const int waitsecs = 0) throw (DmException);

    void setAcl(const std::string&, const Acl&) throw (DmException);

    void utime(const std::string&, const struct utimbuf*) throw (DmException);

    std::string getComment(const std::string&)                     throw (DmException);
    void        setComment(const std::string&, const std::string&) throw (DmException);

    void setGuid(const std::string&, const std::string&) throw (DmException);

    void updateExtendedAttributes(const std::string&,
                                  const Extensible&) throw (DmException);

    Directory* openDir (const std::string&) throw (DmException);
    void       closeDir(Directory*)         throw (DmException);

    struct dirent* readDir (Directory*) throw (DmException);
    ExtendedStat*  readDirx(Directory*) throw (DmException);

    void makeDir(const std::string&, mode_t) throw (DmException);

    void rename     (const std::string&, const std::string&) throw (DmException);
    void removeDir  (const std::string&)                     throw (DmException);

    Replica getReplicaByRFN(const std::string&) throw (DmException);
    void    updateReplica(const Replica&) throw (DmException);

   protected:
    /// The decorated Catalog.
    Catalog* decorated_;
    char*    decoratedId_;
  };

};

#endif	// PROFILERCATALOG_H
