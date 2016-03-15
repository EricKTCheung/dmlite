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
  this->secCtx_ = secCtx;
}

GroupInfo DomeAdapterCatalog::getGroup(const std::string& groupName) throw (DmException) {

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

ExtendedStat DomeAdapterCatalog::extendedStat(const std::string& path, bool follow) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "path: " << path << " follow (ignored) :" << follow);

  Davix::Uri uri(factory_->domehead_ + "/");
  DomeTalker talker(factory_->davixPool_, secCtx_,
                    "GET", uri, "dome_getstatinfo");

  if(!talker.execute("lfn", path)) {
    throw DmException(EINVAL, talker.err());
  }

  std::cout << &talker.response()[0] << std::endl;

  try {
    ExtendedStat xstat;

    std::vector<std::string> components = Url::splitPath(path);
    xstat.name = components.back();

    xstat.stat.st_size = talker.jresp().get<uint64_t>("size");
    xstat.stat.st_mode = talker.jresp().get<mode_t>("mode");
    xstat.stat.st_ino   = talker.jresp().get<ino_t>("fileid");
    xstat.parent = talker.jresp().get<ino_t>("parentfileid");
    xstat.stat.st_atime = talker.jresp().get<time_t>("atime");
    xstat.stat.st_ctime = talker.jresp().get<time_t>("ctime");
    xstat.stat.st_mtime = talker.jresp().get<time_t>("mtime");
    xstat.deserialize(talker.jresp().get<std::string>("xattr"));

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

  // temporary credentials to use in DomeTalker - necessary because our own
  // credentials have not been initialized yet
  SecurityCredentials creds;
  SecurityContext ctx(creds, *user, *groups);

  Davix::Uri uri(factory_->domehead_ + "/");
  DomeTalker talker(factory_->davixPool_, &ctx,
                    "GET", uri, "dome_getidmap");

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


UserInfo DomeAdapterCatalog::getUser(const std::string& username) throw (DmException) {
  throw DmException(EINVAL, "getUser not implemented");
  // Davix::Uri uri(factory_->domehead_ + "/");



  // DomeTalker talker(factory_->davixPool_, secCtx_,
  //                   "GET", uri, "dome_getuser");

  // if(!talker.execute("username", username)) {
  //   throw DmException(EINVAL, talker.err());
  // }

  // try {
  //   UserInfo userinfo;
  //   userinfo.name = username;
  //   userinfo["uid"] = talker.jresp().get<std::string>("uid");
  //   userinfo["banned"] = talker.jresp().get<std::string>("banned");
  //   return userinfo;
  // }
  // catch(boost::property_tree::ptree_error &e) {
  //   throw DmException(EINVAL, SSTR("Error when parsing json response: " << &talker.response()[0]));
  // }
}

void DomeAdapterCatalog::changeDir(const std::string& dir) throw (DmException)
{
  std::cout << "change dir to: " << dir << std::endl;
  cwd_ = dir;
}

std::string DomeAdapterCatalog::getWorkingDir(void) throw (DmException) {
  std::cout << "get working dir: " << cwd_ << std::endl;
  return cwd_;
}

Directory* DomeAdapterCatalog::openDir(const std::string& path) throw (DmException) {
  std::cout << "open dir: " << path << std::endl;
  return new DomeDir(path);
}

void DomeAdapterCatalog::closeDir(Directory* dir) throw (DmException) {
  std::cout << "close dir called" << std::endl;
  delete dir;
}

struct dirent* DomeAdapterCatalog::readDir(Directory* dir) throw (DmException) {
  std::cout << "readDir called" << std::endl;
  return NULL;
}

ExtendedStat* DomeAdapterCatalog::readDirx(Directory* dir) throw (DmException) {
  // std::cout << "readDirx called on " << domedir->path_ << std::endl;
  return NULL;
}


