/// @file   DomeAdapterAuthn.h
/// @brief  Dome adapter
/// @author Georgios Bitzes <georgios.bitzes@cern.ch>

#ifndef DOME_ADAPTER_IDMAP_CACHE_H
#define DOME_ADAPTER_IDMAP_CACHE_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace dmlite {

  // returns time difference in seconds
  inline int64_t time_difference(const struct timespec &start, const struct timespec &end) {
    return (((int64_t)end.tv_sec - (int64_t)start.tv_sec)*(int64_t)1000000000 +
           ((int64_t)end.tv_nsec - (int64_t)start.tv_nsec)) / 1000000000;
  }

  class CacheKey {
  public:
    CacheKey(const std::string &user, const std::vector<std::string> &groups)
    : username(user), groupnames(groups) { }

    // std::string getUsername() const {
    //   return username;
    // }
    //
    // std::vector<std::string> getGroupnames() const {
    //   return groupnames;
    // }

    bool operator<(const CacheKey& other) const {
      if(username != other.username) {
        return username < other.username;
      }
      return groupnames < other.groupnames;
    }
  private:
    std::string username;
    std::vector<std::string> groupnames;
  };

  class CacheContents {
  public:
    CacheContents() {
      clock_gettime(CLOCK_MONOTONIC, &created);
    }

    CacheContents(
      const UserInfo &user,
      const std::vector<GroupInfo> &groups
    ) : userinfo(user), groupinfo(groups) {
      clock_gettime(CLOCK_MONOTONIC, &created);
    }

    bool expired() const {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      return (time_difference(created, now) > 300);
    }

    const UserInfo& getUserInfo() const {
      return userinfo;
    }

    const std::vector<GroupInfo>& getGroupInfo() const {
      return groupinfo;
    }

  private:
    UserInfo userinfo;
    std::vector<GroupInfo> groupinfo;
    struct timespec created;
  };

  class IdMapCache {
  public:
    IdMapCache() {}

    bool lookup(const CacheKey &key, UserInfo* user, std::vector<GroupInfo>* groups) {
      boost::mutex::scoped_lock lock(mtx);
      std::map<CacheKey, CacheContents>::const_iterator it = cache.find(key);

      if(it == cache.end()) {
        Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "SENTINEL NOT FOUND");
        return false;
      }

      if(it->second.expired()) {
        Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "SENTINEL EXPIRED");
        return false;
      }

      *user = it->second.getUserInfo();
      *groups = it->second.getGroupInfo();

      Log(Logger::Lvl1, domeadapterlogmask, domeadapterlogname, "SENTINEL FOUND");
      return true;
    }

    void update(const CacheKey &key, const UserInfo& user, const std::vector<GroupInfo>& groups) {
      boost::mutex::scoped_lock lock(mtx);
      cache[key] = CacheContents(user, groups);
    }

  private:
    boost::mutex mtx;
    std::map<CacheKey, CacheContents> cache;
  };

}

#endif
