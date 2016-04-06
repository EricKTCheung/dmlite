/// @file   DomeAdapterCatalog.cpp
/// @brief  Dome adapter catalog
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <iostream>

#include "DomeAdapterCatalog.h"
#include "DomeAdapter.h"
#include "utils/DomeTalker.h"
#include "utils/DomeUtils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>

using namespace dmlite;

DomeAdapterCatalog::DomeAdapterCatalog(DomeAdapterFactory *factory) throw (DmException) {
  factory_ = factory;
  creds_ = NULL;
}

std::string DomeAdapterCatalog::getImplId() const throw() {
  return std::string("DomeAdapterCatalog");
}

void DomeAdapterCatalog::setStackInstance(StackInstance* si) throw (DmException)
{
  this->si_ = si;
}

void DomeAdapterCatalog::setSecurityContext(const SecurityContext* secCtx) throw (DmException)
{
  this->creds_ = &secCtx->credentials;
}

SecurityContext* DomeAdapterCatalog::createSecurityContext(void) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "");

  UserInfo user;
  std::vector<GroupInfo> groups;
  GroupInfo group;

  user.name    = "root";
  user["uid"]  = 0;
  group.name   = "root";
  group["gid"] = 0;
  groups.push_back(group);

  SecurityContext* sec = new SecurityContext(SecurityCredentials(), user, groups);
  Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, SecurityCredentials().clientName << " " << SecurityCredentials().remoteAddress);

  return sec;
}

SecurityContext* DomeAdapterCatalog::createSecurityContext(const SecurityCredentials& cred) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, cred.clientName << " " << cred.remoteAddress);

  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.clientName, cred.fqans, &user, &groups);
  SecurityContext* sec = new SecurityContext(cred, user, groups);

  Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, cred.clientName << " " << cred.remoteAddress);
  return sec;
}

bool DomeAdapterCatalog::accessReplica(const std::string& replica, int mode) throw (DmException) {
  return true; // TODO: FIX!!!!
}

static void ptree_to_xstat(const boost::property_tree::ptree &ptree, dmlite::ExtendedStat &xstat) {
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
  xstat.deserialize(ptree.get<std::string>("xattrs"));
}

ExtendedStat DomeAdapterCatalog::extendedStat(const std::string& path, bool follow) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path << " follow (ignored) :" << follow);

  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "GET", "dome_getstatinfo");

  if(!talker.execute("lfn", path)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    ExtendedStat xstat;

    std::vector<std::string> components = Url::splitPath(path);
    xstat.name = components.back();
    ptree_to_xstat(talker.jresp(), xstat);
    return xstat;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << &talker.response()[0]));
  }
}

void DomeAdapterCatalog::getIdMap(const std::string& userName,
                          const std::vector<std::string>& groupNames,
                          UserInfo* user,
                          std::vector<GroupInfo>* groups) throw (DmException)
{
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Username: " << userName);
  groups->clear();

  // our own credentials have not been initialized yet
  DomeTalker talker(factory_->davixPool_, NULL, factory_->domehead_,
                    "GET", "dome_getidmap");

  using namespace boost::property_tree;
  ptree params, groups_ptree;
  params.put("username", userName);

  for(std::vector<std::string>::const_iterator it = groupNames.begin(); it != groupNames.end(); it++) {
    groups_ptree.push_back(std::make_pair("", *it));
  }

  if(groupNames.size() != 0) {
    params.add_child("groupnames", groups_ptree);
  }

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    user->name = userName;
    (*user)["uid"] = talker.jresp().get<uint64_t>("uid");
    (*user)["banned"] = talker.jresp().get<uint64_t>("banned");

    boost::optional<const ptree&> groups_resp = talker.jresp().get_child_optional("groups");
    if(groups_resp) {
      for(ptree::const_iterator it = groups_resp->begin(); it != groups_resp->end(); it++) {
        GroupInfo ginfo;
        ginfo.name = it->first;
        ginfo["gid"] = it->second.get<uint64_t>("gid");
        ginfo["banned"] = it->second.get<uint64_t>("banned");

        groups->push_back(ginfo);
      }
    }
  }
  catch(ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << &talker.response()[0]));
  }

  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, "Exiting. Username: " << userName);
}

Directory* DomeAdapterCatalog::openDir(const std::string& path) throw (DmException) {
  using namespace boost::property_tree;
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path);
  DomeTalker talker(factory_->davixPool_, creds_, factory_->domehead_,
                    "GET", "dome_getdir");

  ptree params;
  params.put("path", path);
  params.put("statentries", "true");

  if(!talker.execute(params)) {
    throw DmException(EINVAL, talker.err());
  }

  try {
    DomeDir *domedir = new DomeDir(path);

    ptree entries = talker.jresp().get_child("entries");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      ExtendedStat xstat;
      xstat.name = it->second.get<std::string>("name");

      Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "entry " << xstat.name);

      ptree_to_xstat(it->second, xstat);
      domedir->entries_.push_back(xstat);
    }
    return domedir;
  }
  catch(ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response - " << e.what() << " : " << &talker.response()[0]));
  }
}

void DomeAdapterCatalog::closeDir(Directory* dir) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");
  DomeDir *domedir = static_cast<DomeDir*>(dir);
  delete domedir;
}

ExtendedStat* DomeAdapterCatalog::readDirx(Directory* dir) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");
  if (dir == NULL) {
    throw DmException(DMLITE_SYSERR(EFAULT), "Tried to read a null dir");
  }

  DomeDir *domedir = static_cast<DomeDir*>(dir);
  if(domedir->pos_ >= domedir->entries_.size()) {
    return NULL;
  }

  domedir->pos_++;
  return &domedir->entries_[domedir->pos_ - 1];
}


