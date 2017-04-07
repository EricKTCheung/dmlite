/// @file   DomeAdapterAuthn.cpp
/// @brief  Dome adapter authn
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#include <dmlite/cpp/dmlite.h>
#include <iostream>

#include "DomeAdapterAuthn.h"
#include "DomeAdapter.h"
#include "utils/DomeTalker.h"
#include "utils/DomeUtils.h"
#include "DomeAdapterUtils.h"

#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>

using namespace dmlite;
using namespace boost::property_tree;

DomeAdapterAuthn::DomeAdapterAuthn(DomeAdapterFactory *factory)
: factory_(factory) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "");
}

std::string DomeAdapterAuthn::getImplId() const throw() {
  return std::string("DomeAdapterAuthn");
}

SecurityContext* DomeAdapterAuthn::createSecurityContext(void) throw (DmException) {
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

SecurityContext* DomeAdapterAuthn::createSecurityContext(const SecurityCredentials& cred) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, cred.clientName << " " << cred.remoteAddress);

  UserInfo user;
  std::vector<GroupInfo> groups;

  this->getIdMap(cred.clientName, cred.fqans, &user, &groups);
  SecurityContext* sec = new SecurityContext(cred, user, groups);

  Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, cred.clientName << " " << cred.remoteAddress);
  return sec;
}

void DomeAdapterAuthn::getIdMap(const std::string& userName,
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
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }

  Log(Logger::Lvl3, domeadapterlogmask, domeadapterlogname, "Exiting. Username: " << userName);
}

GroupInfo DomeAdapterAuthn::newGroup(const std::string& gname) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Group name: " << gname);

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "POST", "dome_newgroup");

  if(!talker.execute("groupname", gname)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  return this->getGroup(gname);
}

GroupInfo DomeAdapterAuthn::getGroup(const std::string& groupName) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering. Group name: " << groupName);

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "GET", "dome_getgroup");

  if(!talker.execute("groupname", groupName)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    GroupInfo ginfo;
    ptree_to_groupinfo(talker.jresp(), ginfo);
    return ginfo;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

GroupInfo DomeAdapterAuthn::getGroup(const std::string& key, const boost::any& value) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  if (key != "gid")
    throw DmException(DMLITE_UNKNOWN_KEY,
                      "DomeAdapterAuthn AuthnMySql does not support querying by %s",
                      key.c_str());

  gid_t gid = Extensible::anyToUnsigned(value);
  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "GET", "dome_getgroup");

  if(!talker.execute("groupid", SSTR(gid))) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    GroupInfo ginfo;
    ptree_to_groupinfo(talker.jresp(), ginfo);
    return ginfo;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR("Error when parsing json response: " << talker.response()));
  }
}

void DomeAdapterAuthn::updateGroup(const GroupInfo& group) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering: groupName: '" << group.name << "'");

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "POST", "dome_updategroup");

  boost::property_tree::ptree params;
  params.put("groupname", group.name);
  params.put("banned", group.getLong("banned"));
  params.put("xattr", group.serialize());

  if(!talker.execute(params)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

void DomeAdapterAuthn::deleteGroup(const std::string& groupName) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering: groupName: '" << groupName << "'");

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "POST", "dome_deletegroup");

  if(!talker.execute("groupname", groupName)) {
    throw DmException(talker.dmlite_code(), talker.err());
  }
}

UserInfo DomeAdapterAuthn::newUser(const std::string& uname) throw (DmException) {
  throw DmException(ENOTSUP, "SENTINEL-IMPLEMENT-ME");

}

UserInfo DomeAdapterAuthn::getUser(const std::string& userName) throw (DmException) {
  throw DmException(ENOTSUP, "SENTINEL-IMPLEMENT-ME");

}

UserInfo DomeAdapterAuthn::getUser(const std::string& key, const boost::any& value) throw (DmException) {
  throw DmException(ENOTSUP, "SENTINEL-IMPLEMENT-ME");

}

void DomeAdapterAuthn::updateUser(const UserInfo& user) throw (DmException) {
  throw DmException(ENOTSUP, "SENTINEL-IMPLEMENT-ME");

}

void DomeAdapterAuthn::deleteUser(const std::string& userName) throw (DmException) {
  throw DmException(ENOTSUP, "SENTINEL-IMPLEMENT-ME");

}

std::vector<GroupInfo> DomeAdapterAuthn::getGroups(void) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "GET", "dome_getgroupsvec");

  if(!talker.execute()) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    std::vector<GroupInfo> groups;
    ptree entries = talker.jresp().get_child("groups");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      GroupInfo group;
      ptree_to_groupinfo(it->second, group);
      groups.push_back(group);
    }
    return groups;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR(e.what() << " Error when parsing json response: " << talker.response() << ": " << e.what()));
  }
}

std::vector<UserInfo> DomeAdapterAuthn::getUsers(void) throw (DmException) {
  Log(Logger::Lvl4, domeadapterlogmask, domeadapterlogname, "Entering.");

  DomeTalker talker(factory_->davixPool_, emptycreds, factory_->domehead_,
                    "GET", "dome_getusersvec");

  if(!talker.execute()) {
    throw DmException(talker.dmlite_code(), talker.err());
  }

  try {
    std::vector<UserInfo> users;
    ptree entries = talker.jresp().get_child("users");
    for(ptree::const_iterator it = entries.begin(); it != entries.end(); it++) {
      UserInfo user;
      ptree_to_userinfo(it->second, user);
      users.push_back(user);
    }
    return users;
  }
  catch(boost::property_tree::ptree_error &e) {
    throw DmException(EINVAL, SSTR(e.what() << " Error when parsing json response: " << talker.response() << ": " << e.what()));
  }
}
