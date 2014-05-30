/// @file   XrdMonitor.cpp
/// @brief  Profiler plugin.

#include "XrdMonitor.h"

using namespace dmlite;

time_t XrdMonitor::startup_time = 0;
std::string XrdMonitor::collector_addr;

boost::mutex XrdMonitor::send_mutex_;

int XrdMonitor::FD_ = 0;
struct sockaddr XrdMonitor::dest_addr_;
socklen_t XrdMonitor::dest_addr_len_;

pid_t XrdMonitor::pid_ = 0;
kXR_int64 XrdMonitor::sid_ = 0;
std::string XrdMonitor::hostname_;
std::string XrdMonitor::username_;

char XrdMonitor::pseq_counter_;
boost::mutex XrdMonitor::pseq_mutex_;

boost::mutex XrdMonitor::dictid_mutex_;
kXR_unt32 XrdMonitor::dictid_;

boost::mutex XrdMonitor::redir_mutex_;

XrdMonitor::RedirBuffer XrdMonitor::redirBuffer;

XrdMonitor::XrdMonitor()
//  collector_addr(""), redir_max_buffer_size(10),
//  is_initialized(false), pseq_counter_(0)
{
  // Nothing
}


XrdMonitor::~XrdMonitor() {}

int XrdMonitor::init()
{
  int ret;
  // get process startup time, or rather:
  // a time before any request is handled close to the startup time
  time(&startup_time);

  // initialize the message buffers
  ret = initRedirBuffer(redir_max_buffer_size);
  ret = insertRedirBufferWindowEntry();

  ret = initCollector();

  ret = initServerIdentVars();

  if (ret == 0) {
    sendServerIdent();
  }

  return ret;
}

int XrdMonitor::initServerIdentVars()
{
  // get PID
  pid_ = getpid();

  // construct sid, leave out port (pnum) for now
  sid_ = pid_ << 16;

  // get the hostname and username for monitorMap messages
  int ret;
  char hostname[1024];
  ret = gethostname(hostname, 1024);
  hostname_.assign(hostname);

  char username[1024];
  if (ret == 0) {
    ret = getlogin_r(username, 1024);
    username_.assign(username);
  }

  return ret;
}

int XrdMonitor::initCollector()
{
  if (collector_addr == "")
    return -1;

  FD_ = socket(PF_INET, SOCK_DGRAM, 0);

  const char* host;
  const char* port = "9930";

  std::vector<std::string> server;
  boost::split(server, collector_addr, boost::is_any_of(":/?"));

  if (server.size() > 0) {
    host = server[0].c_str();
  } else {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s = %s",
        "could not read the collector server address",
        "adding a server failed",
        "could not parse value",
        collector_addr.c_str());
    return -1;
  }
  if (server.size() > 1) {
    port = server[1].c_str();
  }

  int ret;
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_family = AF_INET;

  ret = getaddrinfo(host, port, &hints, &res);

  memcpy(&dest_addr_, res->ai_addr, sizeof(dest_addr_));
  dest_addr_len_ = res->ai_addrlen;

  freeaddrinfo(res);

  is_initialized = true;

  return 0;
}

bool XrdMonitor::isInitialized()
{
  return is_initialized;
}

int XrdMonitor::send(const void *buf, size_t buf_len)
{
  boost::mutex::scoped_lock(send_mutex_);

  int ret;
  ret = sendto(FD_, buf, buf_len, 0, &dest_addr_, dest_addr_len_);

  return ret;
}

int XrdMonitor::sendMonMap(kXR_char code, kXR_unt32 dictid, char *info)
{
  int ret;

  struct XrdXrootdMonMap mon_map;
  memset(&mon_map, 0, sizeof(mon_map));

  mon_map.hdr.code = code;
  mon_map.hdr.pseq = getPseqCounter();
  mon_map.hdr.plen = htons(sizeof(mon_map));
  mon_map.hdr.stod = htonl(startup_time);

  mon_map.dictid = dictid;

  strncpy(mon_map.info, info, 1024+256);

  ret = send(&mon_map, sizeof(mon_map));

  return ret;
}


int XrdMonitor::sendServerIdent()
{
  int ret;
  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n&pgm=%s&ver=%s",
      username_.c_str(), pid_, sid_, hostname_.c_str(), "dpm", "1.8.8");

  ret = sendMonMap('=', 0, info);
  return ret;
}


int XrdMonitor::sendUserIdent(const kXR_char dictid, const std::string &userDN, const std::string &userHostname)
{
  int ret;

  std::string userHost;
  if (userHostname.length() > 0) {
    userHost = userHostname;
  } else {
    userHost = "null";
  }

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n&p=%s&n=%s&h=%s&o=%s&r=%s&g=%s&m=%s",
           username_.c_str(), pid_, sid_, hostname_.c_str(),
           "null", userDN.c_str(), userHost.c_str(), "null", "null", "null", "null");

  ret =sendMonMap('u', dictid, info);
  return ret;
}

//int XrdMonitor::send_user_open_path(const std::string user, const std::string path)
//{
//  int ret;
//
//  kXR_char dictid = 0;
//
//  char info[1024+256];
//
//  snprintf(info, 1024+256, "%s.%d:%d@%s\n%s",
//           user.c_str(), pid_, sid_, hostname_.c_str(), path.c_str());
//
//  sendMonMap('d', dictid, info);
//
//  return ret;
//}

char XrdMonitor::getPseqCounter()
{
  boost::mutex::scoped_lock(pseq_mutex_);
  pseq_counter_ = (pseq_counter_ + 1) % 256;

  return pseq_counter_;
}

kXR_unt32 XrdMonitor::getDictId()
{
  boost::mutex::scoped_lock(dictid_mutex_);
  dictid_ += 1;

  return dictid_;
}

int XrdMonitor::initRedirBuffer(int max_size)
{
  int max_slots = (max_size - 16) / sizeof(XrdXrootdMonRedir); // round down

  int msg_buffer_size = max_slots * sizeof(XrdXrootdMonRedir) + 16; // 16 for the msg header
  redirBuffer.max_slots = max_slots;
  redirBuffer.next_slot = 0;
  redirBuffer.msg_buffer = static_cast<XrdXrootdMonBurr *>(malloc(msg_buffer_size));
  if (redirBuffer.msg_buffer == NULL) {
    return -ENOMEM;
  }
  redirBuffer.msg_buffer->sID = sid_;
  redirBuffer.msg_buffer->sXX[0] = XROOTD_MON_REDSID;

  return 0;
}

XrdXrootdMonRedir* XrdMonitor::getRedirBufferNextEntry(int slots)
{
  // space from the last msg + this msg + the ending window message
  if (redirBuffer.next_slot + slots + 1 < redirBuffer.max_slots) {
    // advance the internal msg pointer so that someone else can write, too
    return &(redirBuffer.msg_buffer->info[redirBuffer.next_slot]);
  } else
    return 0x00;
}

std::string XrdMonitor::getHostname()
{
  return hostname_;
}

int XrdMonitor::advanceRedirBufferNextEntry(int slots)
{
  int ret;

  redirBuffer.next_slot += slots + 1;

  return ret;
}

int XrdMonitor::insertRedirBufferWindowEntry()
{
  int ret;

  time_t cur_time = time(NULL);

  XrdXrootdMonRedir *msg = &(redirBuffer.msg_buffer->info[redirBuffer.next_slot]);
  msg->arg0.rdr.Type = XROOTD_MON_REDTIME;
  // ensure only 24 bit are written
  msg->arg0.Window = (cur_time - redirBuffer.last_window_end) & 0x00FFFFFF;
  msg->arg1.Window = cur_time;
  redirBuffer.last_window_end = cur_time;

  advanceRedirBufferNextEntry(1);

  return ret;
}

int XrdMonitor::sendRedirBuffer()
{
  int ret;

  XrdXrootdMonBurr *buffer = redirBuffer.msg_buffer;

  // write the window entry
  insertRedirBufferWindowEntry();

  // the size of the part of the buffer that is filled with messages
  int buffer_size = (redirBuffer.next_slot - 1) * sizeof(XrdXrootdMonRedir) + 16;

  // Fill the msg header
  buffer->hdr.code = 'r';
  buffer->hdr.pseq = getPseqCounter();
  buffer->hdr.plen = htons(buffer_size);
  buffer->hdr.stod = htonl(startup_time);

  ret = send(buffer, buffer_size);

  // Clean up the buffer:
  // Delete everything after the header + server identification
  memset(redirBuffer.msg_buffer + 2 * sizeof(XrdXrootdMonHeader),
         0, (redirBuffer.max_slots - 2) * sizeof(XrdXrootdMonRedir));
  redirBuffer.next_slot = 0;

  // initialize the window start entry
  insertRedirBufferWindowEntry();

  return ret;
}


void XrdMonitor::reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id)
{
  std::string full_path = getHostname() + ":" + path;

  {
    boost::mutex::scoped_lock(redir_mutex_);
    int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
    int slots = msg_size / sizeof(XrdXrootdMonRedir);
    if (msg_size % sizeof(XrdXrootdMonRedir)) {
      ++slots;
    }

    XrdXrootdMonRedir *msg = XrdMonitor::getRedirBufferNextEntry(slots);

    // the buffer could be full ..
    if (msg == 0x00) {
      XrdMonitor::sendRedirBuffer();
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
          "sent XROOTD_MON_OPENDIR msg");
      msg = XrdMonitor::getRedirBufferNextEntry(slots);
    }
    // now it must be free, otherwise forget about it ..
    if (msg != 0x00) {
      msg->arg0.rdr.Type = XROOTD_MON_REDIRECT | cmd_id;
      msg->arg0.rdr.Dent = slots - 1;
      msg->arg0.rdr.Port = 0; // ??
      msg->arg1.dictid = dictid;
      // arg1 + (XrdXrootdMonRedir) 1 = arg1 + 8*char
      char *dest = (char *) (msg + 1);
      strncpy(dest, full_path.c_str(), full_path.length() + 1);

      XrdMonitor::advanceRedirBufferNextEntry(slots);

      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
          "added new XROOTD_MON_OPENDIR msg");
    } else {
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
          "did not send/add new XROOTD_MON_OPENDIR msg");
    }
  }
}
