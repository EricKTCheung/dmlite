/*
 * catalogwrapper.cpp
 *
 * Wrapper classes for Python bindings for catalog.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by catalog.cpp.
 */
    

    // The class Catalog has pure virtual methods: Create a wrapper class!
    class CatalogWrapper: public Catalog, public wrapper<Catalog> {
        public:
        virtual void changeDir(const std::string& path) throw (DmException) { this->get_override("changeDir")(path); } 
        virtual std::string getWorkingDir(void) throw (DmException) { return this->get_override("getWorkingDir")(); } 


        virtual ExtendedStat extendedStat(const std::string& path, bool followSym = true) throw (DmException) { return this->get_override("extendedStat")(path, followSym); } 
        virtual void addReplica(const Replica& replica) throw (DmException) { this->get_override("addReplica")(replica); } 
        virtual void deleteReplica(const Replica& replica) throw (DmException) { this->get_override("deleteReplica")(replica); } 
        virtual std::vector<Replica> getReplicas(const std::string& path) throw (DmException) { return this->get_override("getReplicas")(path); } 

        virtual void symlink(const std::string& oldpath, const std::string& newpath) throw (DmException) { this->get_override("symlink")(oldpath, newpath); } 
        virtual std::string readLink(const std::string& path) throw (DmException) { return this->get_override("readLink")(path); } 
        virtual void unlink(const std::string& path) throw (DmException) { this->get_override("unlink")(path); } 
        virtual void create(const std::string& path, mode_t mode) throw (DmException) { this->get_override("create")(path, mode); } 

        virtual mode_t umask(mode_t mask) throw () { return this->get_override("umask")(mask); } 
        virtual void setMode(const std::string& path, mode_t mode) throw (DmException) { this->get_override("setMode")(path, mode); } 
        virtual void setOwner(const std::string& path, uid_t newUid, gid_t newGid, bool followSymLink = true) throw (DmException) { this->get_override("setOwner")(path, newUid, newGid, followSymLink); } 
        virtual void setSize(const std::string& path, size_t newSize) throw (DmException) { this->get_override("setSize")(path, newSize); } 
        virtual void setChecksum(const std::string& path, const std::string& csumtype, const std::string& csumvalue) throw (DmException) { this->get_override("setChecksum")(path, csumtype, csumvalue); } 
        virtual void setAcl(const std::string& path, const std::vector<Acl>& acls) throw (DmException) { this->get_override("setAcl")(path, acls); } 

        virtual void utime(const std::string& path, const struct utimbuf* buf) throw (DmException) { this->get_override("utime")(path, buf); } 
        virtual std::string getComment(const std::string& path) throw (DmException) { return this->get_override("getComment")(path); } 
        virtual void setComment(const std::string& path, const std::string& comment) throw (DmException) { this->get_override("setComment")(path, comment); } 
        virtual void setGuid(const std::string& path, const std::string &guid) throw (DmException) { this->get_override("setGuid")(path, guid); } 

        virtual Directory* openDir(const std::string& path) throw (DmException) { return (this->get_override("openDir")(path)); } 
        virtual void closeDir(Directory* dir) throw (DmException) { this->get_override("closeDir")(dir); } 
        virtual struct dirent* readDir(Directory* dir) throw (DmException) { return this->get_override("readDir")(dir); } 
        virtual ExtendedStat* readDirx(Directory* dir) throw (DmException) { return this->get_override("readDirx")(dir); } 
        

        virtual void makeDir(const std::string& path, mode_t mode) throw (DmException) { this->get_override("makeDir")(path, mode); } 
        virtual void rename(const std::string& oldPath, const std::string& newPath) throw (DmException) { this->get_override("rename")(oldPath, newPath); } 
        virtual void removeDir(const std::string& path) throw (DmException) { this->get_override("removeDir")(path); } 

        virtual Replica getReplica(const std::string& rfn) throw (DmException) { return this->get_override("getReplica")(rfn); } 
        virtual void updateReplica(const Replica& replica) throw (DmException) { this->get_override("updateReplica")(replica); } 
    };
    
    // The class CatalogFactory has pure virtual methods: Create a wrapper class!
    class CatalogFactoryWrapper: public CatalogFactory, public wrapper<CatalogFactory> {
        public:
        virtual Catalog* createCatalog(PluginManager* pm) throw (DmException) { return this->get_override("createCatalog")(pm); } 
    };

    
    // The class Directory has pure virtual methods: Create a wrapper class!
    class DirectoryWrapper: public Directory, public wrapper<Directory> {
    };
