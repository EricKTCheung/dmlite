#ifndef DMLITE_PYTHON_PYTHONINODE_H
#define DMLITE_PYTHON_PYTHONINODE_H

#include <boost/python.hpp>

#include <dmlite/cpp/inode.h>

#include "PythonINodeFactories.h"

namespace dmlite {

class PythonINode : public INode {
public:
    PythonINode(boost::python::object inode_obj) throw (DmException);

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

    SymLink readLink(ino_t inode) throw (DmException);

    void addReplica   (const Replica&) throw (DmException);  
    void deleteReplica(const Replica&) throw (DmException);

    std::vector<Replica> getReplicas(ino_t inode) throw (DmException);

    Replica getReplica   (int64_t rid) throw (DmException);
    Replica getReplica   (const std::string& sfn) throw (DmException);
    void    updateReplica(const Replica& replica) throw (DmException);

    void utime(ino_t inode, const struct utimbuf* buf) throw (DmException);

    void setMode(ino_t inode, uid_t uid, gid_t gid,
                mode_t mode, const Acl& acl) throw (DmException);

    void setSize    (ino_t inode, size_t size) throw (DmException);
    void setChecksum(ino_t inode, const std::string& csumtype, const std::string& csumvalue) throw (DmException);

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

private:
  PythonMain py;
};

};

#endif // DMLITE_PYTHON_PYTHONINODE_H
