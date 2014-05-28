/// @file   Profiler.h
/// @brief  Profiler plugin.
#ifndef MONITOR_H
#define MONITOR_H

#include <ctime>
#include <stdio.h>
#include <string>
#include <syslog.h>
#include <unistd.h>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread/mutex.hpp>

#include "XrdXrootdMonData.hh"

namespace dmlite {

  class Monitor {
    public:
      Monitor();
      ~Monitor();

      void init();

      int send(const void *buf, size_t buf_len);

      int send_mon_map(kXR_char code, kXR_unt32 dictid, char *info);

      int send_server_ident();
      int send_user_open_path(const std::string user, const std::string path);
      int send_user_login(const std::string user);

      time_t startup_time;
      std::string collector_addr;
    private:
      boost::mutex send_mutex_;

      int FD_;
      struct sockaddr dest_addr_;
      socklen_t dest_addr_len_;
      pid_t pid_;
      char sid_;
      std::string hostname_;
      std::string username_;

      char pseq_counter_;
      boost::mutex pseq_mutex_;
      char get_pseq_counter();
  };
};
#endif
