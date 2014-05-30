/// @file   Profiler.h
/// @brief  Profiler plugin.
#ifndef MONITOR_H
#define MONITOR_H

#include <ctime>
#include <stdio.h>
#include <string>
#include <syslog.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <boost/algorithm/string.hpp>
#include <boost/thread/mutex.hpp>

#include "XrdXrootdMonData.hh"

#define XROOTD_MON_SIDMASK 0xFFFFFFFFFFFF // 48 bit


namespace dmlite {

  class XrdMonitor {
    public:
      XrdMonitor();
      ~XrdMonitor();

      int init();

      bool isInitialized();

      int sendUserIdent(const kXR_char dictid, const std::string &userDN, const std::string &userHostname);
      int sendMonMap(kXR_char code, kXR_unt32 dictid, char *info);

      //int send_user_open_path(const std::string user, const std::string path);

      boost::mutex redir_mutex_;

      int sendRedirBuffer();
      XrdXrootdMonRedir* getRedirBufferNextEntry(int msg_size);
      int advanceRedirBufferNextEntry(int msg_size);

      std::string getHostname();
      kXR_unt32 getDictId();

      time_t startup_time;
      std::string collector_addr;
    private:
      friend class ProfilerFactory;

      bool is_initialized;

      int send(const void *buf, size_t buf_len);

      int sendServerIdent();

      boost::mutex send_mutex_;

      static int FD_;
      static struct sockaddr dest_addr_;
      static socklen_t dest_addr_len_;

      // information for server ident msg
      static pid_t pid_;
      static kXR_int64 sid_;
      static std::string hostname_;
      static std::string username_;

      char pseq_counter_;
      boost::mutex pseq_mutex_;
      char getPseqCounter();

      boost::mutex dictid_mutex_;
      static kXR_unt32 dictid_;

      int initCollector();
      int initServerIdentVars();

      int redir_max_buffer_size;
      int initRedirBuffer(int max_size);
      int insertRedirBufferWindowEntry();

      static struct RedirBuffer
      {
       XrdXrootdMonBurr *msg_buffer;
       int               max_slots;
       int               next_slot;
       time_t            last_window_end;
      }                  redirBuffer;
  };
};
#endif
