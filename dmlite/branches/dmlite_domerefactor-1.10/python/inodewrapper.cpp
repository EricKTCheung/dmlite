/*
 * inodewrapper.cpp
 *
 * Wrapper classes for Python bindings for inode.h from
 * the c++ dmlite library via Boost:Python.
 * This file is included by inode.cpp.
 */
    
    // some helper functions to make the stat struct more accessible
    bool StatIsDir(struct stat &self)
    {
        return S_ISDIR(self.st_mode);
    }

    bool StatIsReg(struct stat &self)
    {
        return S_ISREG(self.st_mode);
    }

    bool StatIsLnk(struct stat &self)
    {
        return S_ISLNK(self.st_mode);
    }

    time_t StatGetATime(struct stat &self)
    {
        return self.st_atime;
    }

    time_t StatGetCTime(struct stat &self)
    {
        return self.st_ctime;
    }

    time_t StatGetMTime(struct stat &self)
    {
        return self.st_mtime;
    }
    

    // The class INode has pure virtual methods: Create a wrapper class!
    class INodeWrapper: public INode, public wrapper<INode> {
        public:
        virtual void begin(void) throw (DmException) { this->get_override("begin")(); } 
        virtual void commit(void) throw (DmException) { this->get_override("commit")(); } 
        virtual void rollback(void) throw (DmException) { this->get_override("rollback")(); }
        
        virtual ExtendedStat create(ino_t parent, const std::string& name, uid_t uid, gid_t gid, mode_t mode, size_t size, ExtendedStat::FileStatus status, const std::string& csumtype, const std::string& csumvalue, const Acl& acl) throw (DmException) { return this->get_override("create")(parent, name, uid, gid, mode, size, status, csumtype, csumvalue, acl); } 

        virtual void symlink(ino_t inode, const std::string &link) throw (DmException) { this->get_override("symlink")(inode, link); }
        virtual void unlink(ino_t inode) throw (DmException) { this->get_override("unlink")(inode); } 
        virtual void move(ino_t inode, ino_t dest) throw (DmException) { this->get_override("move")(inode, dest); } 
        virtual void rename(ino_t inode, const std::string& name) throw (DmException) { this->get_override("rename")(inode, name); } 

        virtual ExtendedStat extendedStat(ino_t inode) throw (DmException) { return this->get_override("extendedStat")(inode); } 
        virtual ExtendedStat extendedStat(ino_t parent, const std::string& name) throw (DmException) { return this->get_override("extendedStat")(parent, name); } 
        virtual ExtendedStat extendedStat(const std::string& guid) throw (DmException) { return this->get_override("extendedStat")(guid); } 

        virtual SymLink readLink(ino_t inode) throw (DmException) { return this->get_override("readLink")(inode); }
        virtual void addReplica(const Replica& replica) throw (DmException) { this->get_override("addReplica")(replica); } 
        virtual void deleteReplica(const Replica& replica) throw (DmException) { this->get_override("deleteReplica")(replica); } 

        virtual Replica getReplica(int64_t rid) throw (DmException) { return this->get_override("getReplica")(rid); } 
        virtual Replica getReplica(const std::string& rfn) throw (DmException) { return this->get_override("getReplica")(rfn); } 
        virtual void updateReplica(const Replica& replica) throw (DmException) { this->get_override("updateReplica")(replica); } 
        virtual std::vector<Replica> getReplicas(ino_t inode) throw (DmException) { return this->get_override("getReplicas")(inode); } 

        virtual void utime(ino_t inode, const struct utimbuf* buf) throw (DmException) { this->get_override("utime")(inode, buf); } 
        virtual void setMode(ino_t inode, uid_t uid, gid_t gid, mode_t mode, const Acl& acl) throw (DmException) { this->get_override("setMode")(inode, uid, gid, mode, acl); }
        virtual void setSize(ino_t inode, size_t size) throw(DmException) { this->get_override("setSize")(inode, size); } 

        virtual void setChecksum(ino_t inode, const std::string& csumtype, const std::string& csumvalue) throw(DmException) { this->get_override("setChecksum")(inode, csumtype, csumvalue); } 
        virtual std::string getComment(ino_t inode) throw(DmException) { return this->get_override("getComment")(inode); } 
        virtual void setComment(ino_t inode, const std::string& comment) throw(DmException) { this->get_override("setComment")(inode, comment); } 
        virtual void deleteComment(ino_t inode) throw(DmException) { this->get_override("deleteComment")(inode); }
 
        virtual void setGuid(ino_t inode, const std::string& guid) throw(DmException) { this->get_override("setGuid")(inode, guid); } 

        virtual IDirectory* openDir(ino_t inode) throw(DmException) { return this->get_override("openDir")(inode); } 
        virtual void closeDir(IDirectory* dir) throw(DmException) { this->get_override("closeDir")(dir); } 
        virtual ExtendedStat* readDirx(IDirectory* dir) throw(DmException) { return this->get_override("readDirx")(dir); } 
        virtual struct dirent* readDir(IDirectory* dir) throw(DmException) { return this->get_override("readDir")(dir); } 
    };


    // The class AuthnFactory has pure virtual methods: Create a wrapper class!
    class INodeFactoryWrapper: public INodeFactory, public wrapper<INodeFactory> {
        public:
        virtual INode* createINode(PluginManager* pm) throw (DmException) { return this->get_override("createINode")(pm); } 
    };
