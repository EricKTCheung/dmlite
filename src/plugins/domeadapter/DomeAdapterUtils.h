/// @file   DomeAdapterUtils.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>
#ifndef DOME_ADAPTER_UTILS_H
#define DOME_ADAPTER_UTILS_H

#include <dmlite/cpp/poolmanager.h>
#include <dmlite/cpp/inode.h>
#include <dmlite/cpp/utils/security.h>

inline void ptree_to_xstat(const boost::property_tree::ptree &ptree, dmlite::ExtendedStat &xstat) {
  xstat.stat.st_size = ptree.get<uint64_t>("size");
  xstat.stat.st_mode = ptree.get<mode_t>("mode");
  xstat.stat.st_ino   = ptree.get<ino_t>("fileid");
  xstat.parent = ptree.get<ino_t>("parentfileid");
  xstat.stat.st_atime = ptree.get<time_t>("atime");
  xstat.stat.st_ctime = ptree.get<time_t>("ctime");
  xstat.stat.st_mtime = ptree.get<time_t>("mtime");
  xstat.stat.st_nlink = ptree.get<nlink_t>("nlink");
  xstat.stat.st_gid = ptree.get<gid_t>("gid");
  xstat.stat.st_uid = ptree.get<uid_t>("uid");
  xstat.name = ptree.get<std::string>("name");
  xstat.acl = dmlite::Acl(ptree.get<std::string>("acl"));
  xstat.deserialize(ptree.get<std::string>("xattrs"));
}

inline void ptree_to_groupinfo(const boost::property_tree::ptree &ptree, dmlite::GroupInfo &groupInfo) {
  groupInfo.name = ptree.get<std::string>("groupname");
  groupInfo["gid"] = ptree.get<uint64_t>("gid");
  groupInfo["banned"] = ptree.get<uint64_t>("banned");
}

inline void ptree_to_userinfo(const boost::property_tree::ptree &ptree, dmlite::UserInfo &userInfo) {
  userInfo.name = ptree.get<std::string>("username");
  userInfo["uid"] = ptree.get<uint64_t>("userid");
  userInfo["banned"] = ptree.get<int>("banned");

  std::string xattr = ptree.get<std::string>("xattr");
  if(!xattr.empty()) {
    userInfo.deserialize(xattr);
  }
}

inline void ptree_to_replica(const boost::property_tree::ptree &ptree, dmlite::Replica &replica) {
  replica.replicaid = ptree.get<int64_t>("replicaid");
  replica.fileid = ptree.get<int64_t>("fileid");
  replica.nbaccesses = ptree.get<int64_t>("nbaccesses");
  replica.atime = ptree.get<time_t>("atime");
  replica.ptime = ptree.get<time_t>("ptime");
  replica.ltime = ptree.get<time_t>("ltime");

  char status = atoi(ptree.get<std::string>("status").c_str());
  char type = atoi(ptree.get<std::string>("type").c_str());
  
  replica.rfn = ptree.get<std::string>("rfn", "");
  replica.status = static_cast<dmlite::Replica::ReplicaStatus>(status);
  replica.type = static_cast<dmlite::Replica::ReplicaType>(type);

  replica.server = ptree.get<std::string>("server");
  replica.setname = ptree.get<std::string>("setname");
  replica.deserialize(ptree.get<std::string>("xattrs"));
}

inline dmlite::Pool deserializePool(boost::property_tree::ptree::const_iterator it) {
    using namespace dmlite;
    using namespace boost::property_tree;

    Pool p;
    p.name = it->first;
    p.type = "filesystem";
    p["freespace"] = it->second.get<uint64_t>("freespace", 0);
    p["physicalsize"] = it->second.get<uint64_t>("physicalsize", 0);

    p["poolstatus"] = it->second.get<std::string>("poolstatus", "");
    p["s_type"] = it->second.get<std::string>("s_type", "");
    p["defsize"] = it->second.get<uint64_t>("defsize", 0);

    // fetch info about the filesystems
    std::vector<boost::any> filesystems;

    if(it->second.count("fsinfo") != 0) {
      ptree fsinfo = it->second.get_child("fsinfo");

      // iterating over servers
      for(ptree::const_iterator it2 = fsinfo.begin(); it2 != fsinfo.end(); it2++) {
        // iterating over filesystems
        for(ptree::const_iterator it3 = it2->second.begin(); it3 != it2->second.end(); it3++) {
          Extensible fs;
          fs["server"] = it2->first;
          fs["fs"] = it3->first;
          fs["status"] = it3->second.get<uint64_t>("fsstatus", 0);
          fs["freespace"] = it3->second.get<uint64_t>("freespace", 0);
          fs["physicalsize"] = it3->second.get<uint64_t>("physicalsize", 0);

          filesystems.push_back(fs);
        }
      }
    }

    p["filesystems"] = filesystems;
    return p;
}

#endif
