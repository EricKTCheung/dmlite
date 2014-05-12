/// @file    plugins/proc/Providers.h
/// @brief   Build-in proc providers
/// @author  Alejandro Álvarez Ayllon <aalvarez@cern.ch>
#ifndef PROVIDERS_H
#define PROVIDERS_H

#include "Provider.h"
#include <dmlite/cpp/poolmanager.h>

namespace dmlite {

#define SAFE_GET_IMPLID(store, call) \
  try { \
    store = call->getImplId();\
  } catch(...) {\
    store = 0;\
  }

struct StackInfoProvider: public ProcProvider
{
  StackInfoProvider(const std::string& name): ProcProvider(name) {}

  ExtendedStat stat() const {
    ExtendedStat xs;
    xs.name = this->name;
    xs.status = dmlite::ExtendedStat::kOnline;
    ::memset(&xs.stat, 0, sizeof(xs.stat));
    xs.stat.st_mode = 0444;
    xs.stat.st_atime = xs.stat.st_ctime = xs.stat.st_mtime = time(NULL);
    return xs;
  }

  std::string getContent() const {
    Extensible ex;
    SAFE_GET_IMPLID(ex["authn"], this->stack->getAuthn());
    SAFE_GET_IMPLID(ex["catalog"], this->stack->getCatalog());
    SAFE_GET_IMPLID(ex["inode"], this->stack->getINode());
    SAFE_GET_IMPLID(ex["poolmanager"], this->stack->getPoolManager());
    SAFE_GET_IMPLID(ex["iodriver"], this->stack->getIODriver());
    return ex.serialize();
  }
};

struct AuthnInfoProvider: public ProcProvider
{
  AuthnInfoProvider(const std::string& name): ProcProvider(name) {}

  ExtendedStat stat() const {
    ExtendedStat xs;
    xs.name = this->name;
    xs.status = dmlite::ExtendedStat::kOnline;
    ::memset(&xs.stat, 0, sizeof(xs.stat));
    xs.stat.st_mode = 0444;
    xs.stat.st_atime = xs.stat.st_ctime = xs.stat.st_mtime = time(NULL);
    return xs;
  }

  std::string getContent() const {
    const SecurityContext* sec = this->stack->getSecurityContext();
    Extensible ex;

    Extensible credentials(sec->credentials);
    credentials["mech"] = sec->credentials.mech;
    credentials["clientName"] = sec->credentials.clientName;
    credentials["remoteAddress"] = sec->credentials.remoteAddress;
    credentials["sessionId"] = sec->credentials.sessionId;
    ex["credentials"] = credentials;

    Extensible uinfo(sec->user);
    uinfo["name"] = sec->user.name;
    ex["user"] = uinfo;

    std::vector<boost::any> groups;
    std::vector<GroupInfo>::const_iterator i;
    for (i = sec->groups.begin(); i != sec->groups.end(); ++i) {
      Extensible group(*i);
      group["name"] = i->name;
      groups.push_back(group);
    }
    ex["groups"] = groups;

    return ex.serialize();
  }
};

}

#endif // PROVIDERS_H
