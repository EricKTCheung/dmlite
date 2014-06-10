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

#if defined(_BIG_ENDIAN) || defined(__BIG_ENDIAN__) || \
   defined(__IEEE_BIG_ENDIAN) || \
   (defined(__BYTE_ORDER) && __BYTE_ORDER == __BIG_ENDIAN)
#define Xrd_Big_Endian
#ifndef htonll
#define htonll(_x_)  _x_
#endif
#ifndef h2nll
#define h2nll(_x_, _y_) memcpy((void *)&_y_,(const void *)&_x_,sizeof(long long))
#endif
#ifndef ntohll
#define ntohll(_x_)  _x_
#endif
#ifndef n2hll
#define n2hll(_x_, _y_) memcpy((void *)&_y_,(const void *)&_x_,sizeof(long long))
#endif

#elif defined(_LITTLE_ENDIAN) || defined(__LITTLE_ENDIAN__) || \
     defined(__IEEE_LITTLE_ENDIAN) || \
     (defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN)
#if !defined(__GNUC__) || defined(__APPLE__)

#if !defined(__sun) || (defined(__sun) && (!defined(_LP64) || defined(__SunOS_5_10)))
extern "C" unsigned long long Swap_n2hll(unsigned long long x);
#ifndef htonll
#define htonll(_x_) Swap_n2hll(_x_)
#endif
#ifndef ntohll
#define ntohll(_x_) Swap_n2hll(_x_)
#endif
#endif

#else

#ifndef htonll
#define htonll(_x_) __bswap_64(_x_)
#endif
#ifndef ntohll
#define ntohll(_x_) __bswap_64(_x_)
#endif

#endif

#ifndef h2nll
#define h2nll(_x_, _y_) memcpy((void *)&_y_,(const void *)&_x_,sizeof(long long));\
                        _y_ = htonll(_y_)
#endif
#ifndef n2hll
#define n2hll(_x_, _y_) memcpy((void *)&_y_,(const void *)&_x_,sizeof(long long));\
                        _y_ = ntohll(_y_)
#endif

#else
#ifndef WIN32
#error Unable to determine target architecture endianness!
#endif
#endif

#define XROOTD_MON_SIDMASK 0xFFFFFFFFFFFF // 48 bit

#define XRDMON_FUNC_IS_NOP 1000


namespace dmlite {

  class XrdMonitor {
    public:
      XrdMonitor();

      static int initOrNOP();

      static int sendShortUserIdent(const kXR_char dictid);
      static int sendUserIdent(const kXR_char dictid,
                               const std::string &userDN,
                               const std::string &userHostname,
                               const std::string &vo);

      static int sendFileOpen(const kXR_char fileid, const std::string &path);

      static int sendMonMap(kXR_char code, kXR_unt32 dictid, char *info);

      static void reportXrdRedirNsCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id);
      static void reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &host, const int port,
                                    const std::string &path, const int cmd_id);
      static void reportXrdFileOpen(const kXR_unt32 dictid, const kXR_unt32 fileid,
                                    const std::string &path, const long long file_size);
      static void reportXrdFileClose(const kXR_unt32 fileid, const XrdXrootdMonStatXFR xfr, const bool forced = false);

      static std::string getHostname();
      static kXR_unt32 getDictId();

      static time_t startup_time;
      static std::string collector_addr;
    private:
      friend class ProfilerFactory;

      ~XrdMonitor();

      // configuration options
      static bool include_lfn_;

      static bool is_initialized_;
      static boost::mutex init_mutex_;

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

      static char fstream_pseq_counter_;
      static boost::mutex fstream_pseq_mutex_;
      static char getFstreamPseqCounter();

      static char rstream_pseq_counter_;
      static boost::mutex rstream_pseq_mutex_;
      static char getRstreamPseqCounter();

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

      static int file_max_buffer_size_;
      static boost::mutex file_mutex_;

      static int initFileBuffer(int max_size);

      static int sendFileBuffer();
      static XrdXrootdMonFileHdr* getFileBufferNextEntry(int msg_size);
      static int advanceFileBufferNextEntry(int msg_size);

      struct XrdFStreamBuff
      {
        XrdXrootdMonHeader    hdr;
        XrdXrootdMonFileTOD   tod;
        XrdXrootdMonFileHdr   info[sizeof(XrdXrootdMonFileHdr)];
      };

      static struct FileBuffer
      {
        XrdFStreamBuff   *msg_buffer;
        int               max_slots;
        int               next_slot;
        int               xfr_msgs;
        int               total_msgs;
      }                   fileBuffer;

  };
};
#endif
