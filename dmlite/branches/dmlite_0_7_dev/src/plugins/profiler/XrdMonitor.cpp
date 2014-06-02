/// @file   XrdMonitor.cpp
/// @brief  Profiler plugin.

#include "XrdMonitor.h"

using namespace dmlite;

time_t XrdMonitor::startup_time = 0;
std::string XrdMonitor::collector_addr;

bool XrdMonitor::is_initialized_ = false;
boost::mutex XrdMonitor::init_mutex_;

boost::mutex XrdMonitor::send_mutex_;

int XrdMonitor::FD_ = 0;
struct sockaddr XrdMonitor::dest_addr_;
socklen_t XrdMonitor::dest_addr_len_;

pid_t XrdMonitor::pid_ = 0;
kXR_int64 XrdMonitor::sid_ = 0;
std::string XrdMonitor::hostname_;
std::string XrdMonitor::username_;

char XrdMonitor::pseq_counter_ = 0;
boost::mutex XrdMonitor::pseq_mutex_;

kXR_unt32 XrdMonitor::dictid_ = 0;
boost::mutex XrdMonitor::dictid_mutex_;

int XrdMonitor::redir_max_buffer_size_ = 0;
boost::mutex XrdMonitor::redir_mutex_;

XrdMonitor::RedirBuffer XrdMonitor::redirBuffer;

XrdMonitor::XrdMonitor()
{
  // Nothing
}


XrdMonitor::~XrdMonitor() {}

int XrdMonitor::initOrNOP()
{
  boost::mutex::scoped_lock(init_mutex_);
  if (is_initialized_ == true) {
    return XRDMON_FUNC_IS_NOP;
  }

  int ret = 0;
  // get process startup time, or rather:
  // a time before any request is handled close to the startup time
  time(&startup_time);

  // initialize the message buffers
  ret = initRedirBuffer(redir_max_buffer_size_);
  if (ret < 0) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: error code = %d",
        "initRedirBuffer failed",
        ret);
    return ret;
  }

  ret = insertRedirBufferWindowEntry();
  if (ret < 0) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: error code = %d",
        "insertRedirBufferWindowEntry failed",
        ret);
    return ret;
  }

  ret = initCollector();
  if (ret < 0) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: error code = %d",
        "initCollector failed",
        ret);
    return ret;
  }

  ret = initServerIdentVars();
  if (ret < 0) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: error code = %d",
        "initServerIdentVars failed",
        ret);
    return ret;
  }

  if (ret >= 0) {
    is_initialized_ = true;
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
  int ret = 0;
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

  int ret = 0;
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_family = AF_INET;

  ret = getaddrinfo(host, port, &hints, &res);

  memcpy(&dest_addr_, res->ai_addr, sizeof(dest_addr_));
  dest_addr_len_ = res->ai_addrlen;

  freeaddrinfo(res);

  return 0;
}

bool XrdMonitor::isInitialized()
{
  boost::mutex::scoped_lock(init_mutex_);
  return is_initialized_;
}

int XrdMonitor::send(const void *buf, size_t buf_len)
{
  boost::mutex::scoped_lock(send_mutex_);

  ssize_t ret;
  ret = sendto(FD_, buf, buf_len, 0, &dest_addr_, dest_addr_len_);

  if (ret == buf_len) {
    return 0;
  } else {
    return ret;
  }
}

int XrdMonitor::sendMonMap(kXR_char code, kXR_unt32 dictid, char *info)
{
  int ret = 0;

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
  int ret = 0;
  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n&pgm=%s&ver=%s",
      username_.c_str(), pid_, sid_, hostname_.c_str(), "dpm", "1.8.8");

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:\n%s",
        "send serverident",
        info);

  ret = sendMonMap(XROOTD_MON_MAPIDNT, 0, info);
  if (ret) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "failed sending ServerIdent msg");
  }
  return ret;
}


int XrdMonitor::sendShortUserIdent(const kXR_char dictid)
{
  int ret = 0;

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n",
           username_.c_str(), pid_, sid_, hostname_.c_str());

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:\n%s",
        "send short userident",
        info);

  ret = sendMonMap(XROOTD_MON_MAPUSER, dictid, info);
  if (ret) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "failed sending UserIdent msg");
  }
  return ret;
}


int XrdMonitor::sendUserIdent(const kXR_char dictid,
                              const std::string &userDN,
                              const std::string &userHostname,
                              const std::string group)
{
  int ret = 0;

  std::string userHost;
  if (userHostname.length() > 0) {
    userHost = userHostname;
  } else {
    userHost = "null";
  }

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n&p=%s&n=%s&h=%s&o=%s&r=%s&g=%s&m=%s",
           username_.c_str(), pid_, sid_, hostname_.c_str(),
           "null", userDN.c_str(), userHost.c_str(),
           "null", "null", group.c_str(), "null");

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:\n%s",
        "send userident",
        info);

  ret = sendMonMap('u', dictid, info);
  if (ret) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "failed sending UserIdent msg");
  }
  return ret;
}

char XrdMonitor::getPseqCounter()
{
  char this_counter;
  {
    boost::mutex::scoped_lock(pseq_mutex_);
    pseq_counter_ = (pseq_counter_ + 1) % 256;
    this_counter = pseq_counter_;
  }
  return pseq_counter_;
}

kXR_unt32 XrdMonitor::getDictId()
{
  kXR_unt32 this_dictid;
  {
    boost::mutex::scoped_lock(dictid_mutex_);
    dictid_ += 1;
    this_dictid = dictid_;
  }
  return this_dictid;
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
  int ret = 0;

  redirBuffer.next_slot += slots + 1;

  return ret;
}

int XrdMonitor::insertRedirBufferWindowEntry()
{
  int ret = 0;

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
  int ret = 0;

  XrdXrootdMonBurr *buffer = redirBuffer.msg_buffer;

  // write the window entry
  insertRedirBufferWindowEntry();

  // the size of the part of the buffer that is filled with messages
  int buffer_size = (redirBuffer.next_slot - 1) * sizeof(XrdXrootdMonRedir) + 16;

  // Fill the msg header
  buffer->hdr.code = XROOTD_MON_MAPREDR;
  buffer->hdr.pseq = getPseqCounter();
  buffer->hdr.plen = htons(buffer_size);
  buffer->hdr.stod = htonl(startup_time);

  ret = send(buffer, buffer_size);

  // Clean up the buffer:
  // Delete everything after the header + server identification
  memset(redirBuffer.msg_buffer->info,
         0, (redirBuffer.max_slots) * sizeof(XrdXrootdMonRedir));
  redirBuffer.next_slot = 0;

  // initialize the window start entry
  insertRedirBufferWindowEntry();

  return ret;
}


void XrdMonitor::reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id)
{
  std::string full_path = getHostname() + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = msg_size / sizeof(XrdXrootdMonRedir);
  if (msg_size % sizeof(XrdXrootdMonRedir)) {
    ++slots;
  }

  XrdXrootdMonRedir *msg;
  {
    boost::mutex::scoped_lock(redir_mutex_);

    msg = getRedirBufferNextEntry(slots);

    // the buffer could be full ..
    if (msg == 0x00) {
      int ret = XrdMonitor::sendRedirBuffer();
      if (ret) {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "failed sending REDIR msg");
      } else {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "sent REDIR msg");
      }
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

      advanceRedirBufferNextEntry(slots);
    }
  }

  if (msg != 0x00) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "added new REDIR msg");
  } else {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "did not send/add new REDIR msg");
  }
}
