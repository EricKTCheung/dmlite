/// @file   XrdMonitor.h
/// @brief  Profiler plugin.
#ifndef XRDMONITOR_H
#define XRDMONITOR_H

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

      static int init();

      static bool isInitialized();

      static int sendUserIdent(const kXR_char dictid, const std::string &userDN, const std::string &userHostname);
      static int sendMonMap(kXR_char code, kXR_unt32 dictid, char *info);

      //int send_user_open_path(const std::string user, const std::string path);

      static void reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id);

      static std::string getHostname();
      static kXR_unt32 getDictId();

      static time_t startup_time;
      static std::string collector_addr;
    private:
      friend class ProfilerFactory;

      ~XrdMonitor();

      static bool is_initialized;

      static int send(const void *buf, size_t buf_len);

      static int sendServerIdent();

      static boost::mutex send_mutex_;

      static int FD_;
      static struct sockaddr dest_addr_;
      static socklen_t dest_addr_len_;

      // information for server ident msg
      static pid_t pid_;
      static kXR_int64 sid_;
      static std::string hostname_;
      static std::string username_;

      static char pseq_counter_;
      static boost::mutex pseq_mutex_;
      static char getPseqCounter();

      static boost::mutex dictid_mutex_;
      static kXR_unt32 dictid_;

      static int initCollector();
      static int initServerIdentVars();

      static int redir_max_buffer_size_;
      static boost::mutex redir_mutex_;

      static int initRedirBuffer(int max_size);
      static int insertRedirBufferWindowEntry();

      static int sendRedirBuffer();
      static XrdXrootdMonRedir* getRedirBufferNextEntry(int msg_size);
      static int advanceRedirBufferNextEntry(int msg_size);

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
