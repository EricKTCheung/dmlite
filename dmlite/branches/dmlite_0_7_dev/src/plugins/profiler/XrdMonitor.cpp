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

int XrdMonitor::redir_max_buffer_size_ = 32768;
boost::mutex XrdMonitor::redir_mutex_;

XrdMonitor::RedirBuffer XrdMonitor::redirBuffer;

int XrdMonitor::file_max_buffer_size_ = 32768;
boost::mutex XrdMonitor::file_mutex_;

XrdMonitor::FileBuffer XrdMonitor::fileBuffer;

#if defined(_LITTLE_ENDIAN) || defined(__LITTLE_ENDIAN__) || \
      defined(__IEEE_LITTLE_ENDIAN) || \
   (defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN)
#if !defined(__GNUC__) || defined(__APPLE__)
extern "C"
{
  unsigned long long Swap_n2hll(unsigned long long x)
  {
    unsigned long long ret_val;
    *( (unsigned int  *)(&ret_val) + 1) = ntohl(*( (unsigned int  *)(&x)));
    *(((unsigned int  *)(&ret_val)))    = ntohl(*(((unsigned int  *)(&x))+1));
    return ret_val;
  }
}
#endif

#endif

XrdMonitor::XrdMonitor()
{
  // Nothing
}


XrdMonitor::~XrdMonitor() {}

int XrdMonitor::initOrNOP()
{
  int ret = 0;

  boost::mutex::scoped_lock(init_mutex_);
  if (is_initialized_ == true) {
    ret = XRDMON_FUNC_IS_NOP;
    return ret;
  }

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

  ret = initFileBuffer(file_max_buffer_size_);
  if (ret < 0) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: error code = %d",
        "initFileBuffer failed",
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
                              const std::string vo)
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
           vo.c_str(), "null", "null", "null");

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:\n%s",
        "send userident",
        info);

  ret = sendMonMap(XROOTD_MON_MAPUSER, dictid, info);
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

int XrdMonitor::advanceRedirBufferNextEntry(int slots)
{
  int ret = 0;

  redirBuffer.next_slot += slots;

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

std::string XrdMonitor::getHostname()
{
  return hostname_;
}


void XrdMonitor::reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id)
{
  std::string full_path = getHostname() + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = msg_size / sizeof(XrdXrootdMonRedir);
  // TODO: optimize, get rid of the %
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
      msg = getRedirBufferNextEntry(slots);
    }
    // now it must be free, otherwise forget about it ..
    if (msg != 0x00) {
      msg->arg0.rdr.Type = XROOTD_MON_REDIRECT | cmd_id;
      msg->arg0.rdr.Dent = slots - 1;
      msg->arg0.rdr.Port = 0; // TODO: ??
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


int XrdMonitor::initFileBuffer(int max_size)
{
  int max_slots = (max_size - sizeof(XrdXrootdMonHeader) - sizeof(XrdXrootdMonFileTOD)) / sizeof(XrdXrootdMonFileHdr); // round down

  int msg_buffer_size = max_slots * sizeof(XrdXrootdMonFileHdr) + sizeof(XrdXrootdMonHeader) + sizeof(XrdXrootdMonFileTOD);
  fileBuffer.max_slots = max_slots;
  fileBuffer.next_slot = 0;
  fileBuffer.msg_buffer = static_cast<XrdFStreamBuff *>(malloc(msg_buffer_size));
  if (fileBuffer.msg_buffer == NULL) {
    return -ENOMEM;
  }

  XrdXrootdMonFileTOD *msg = &(fileBuffer.msg_buffer->tod);
  msg->Hdr.recType = XrdXrootdMonFileHdr::isTime;
  msg->Hdr.recFlag = 0;
  msg->Hdr.recSize = htons(sizeof(XrdXrootdMonFileTOD));

  fileBuffer.xfr_msgs = 0;
  fileBuffer.total_msgs = 0;

  return 0;
}

XrdXrootdMonFileHdr* XrdMonitor::getFileBufferNextEntry(int slots)
{

  // space from the last msg + this msg + the ending window message
  if (fileBuffer.next_slot + slots + 1 < fileBuffer.max_slots) {
    ++fileBuffer.total_msgs;
    fileBuffer.msg_buffer->tod.tBeg = htonl(static_cast<int>(time(0)));
    return &(fileBuffer.msg_buffer->info[fileBuffer.next_slot]);
  } else
    return 0x00;
}

int XrdMonitor::advanceFileBufferNextEntry(int slots)
{
  int ret = 0;

  fileBuffer.next_slot += slots;

  return ret;
}


int XrdMonitor::sendFileBuffer()
{
  int ret = 0;

  XrdFStreamBuff *buffer = fileBuffer.msg_buffer;

  // the size of the part of the buffer that is filled with messages
  int buffer_size = (redirBuffer.next_slot) * sizeof(XrdXrootdMonFileHdr) + sizeof(XrdXrootdMonHeader) + sizeof(XrdXrootdMonFileTOD);

  // Fill the msg header
  buffer->hdr.code = XROOTD_MON_MAPFSTA;
  buffer->hdr.pseq = getPseqCounter();
  buffer->hdr.plen = htons(buffer_size);
  buffer->hdr.stod = htonl(startup_time);

  buffer->tod.Hdr.nRecs[0] = htons(static_cast<short>(fileBuffer.xfr_msgs));
  buffer->tod.Hdr.nRecs[1] = htons(static_cast<short>(fileBuffer.total_msgs));

  buffer->tod.tEnd = htonl(static_cast<int>(time(0)));

  ret = send(buffer, buffer_size);

  // Clean up the buffer:
  // Delete everything after the header + server identification
  memset(fileBuffer.msg_buffer->info,
         0, (fileBuffer.max_slots) * sizeof(XrdXrootdMonFileHdr));
  fileBuffer.next_slot = 0;

  fileBuffer.xfr_msgs = 0;
  fileBuffer.total_msgs = 0;

  buffer->tod.tBeg = buffer->tod.tEnd;

  return ret;
}

void XrdMonitor::reportXrdFileOpen(const kXR_unt32 dictid, const kXR_unt32 fileid,
                                   const std::string &path,
                                   const long long file_size)
{
  static const int min_msg_size = sizeof(XrdXrootdMonFileOPN) - sizeof(XrdXrootdMonFileLFN);
  int msg_size = min_msg_size;
  //if (include_lfn) {
  msg_size += sizeof(kXR_unt32) + path.length();
  //}
  // TODO: optimize, get rid of the %
  int slots = msg_size / sizeof(XrdXrootdMonFileHdr);
  if (msg_size % sizeof(XrdXrootdMonFileHdr)) {
    ++slots;
  }
  int aligned_path_len = path.length() + (slots * sizeof(XrdXrootdMonFileHdr) - msg_size);

  XrdXrootdMonFileOPN *msg;
  {
    boost::mutex::scoped_lock(file_mutex_);

    msg = (XrdXrootdMonFileOPN *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "failed sending FILE msg");
      } else {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "sent FILE msg");
      }
      msg = (XrdXrootdMonFileOPN *) getFileBufferNextEntry(slots);
    }

    if (msg != 0x00) {
      msg->Hdr.recType = XrdXrootdMonFileHdr::isOpen;
      msg->Hdr.recFlag = XrdXrootdMonFileHdr::hasRW;
      msg->Hdr.recSize = htons(static_cast<short>(slots*sizeof(XrdXrootdMonFileHdr)));
      msg->Hdr.fileID = fileid;
      msg->fsz = htonll(file_size);

      //if (include_lfn) {
      msg->Hdr.recFlag |= XrdXrootdMonFileHdr::hasLFN;
      msg->ufn.user = dictid;
      strncpy(msg->ufn.lfn, path.c_str(), aligned_path_len);
      //}

      advanceFileBufferNextEntry(slots);
    }
  }

  if (msg != 0x00) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "added new FILE msg");
  } else {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "did not send/add new FILE msg");
  }
}


void XrdMonitor::reportXrdFileClose(const kXR_unt32 fileid, const XrdXrootdMonStatXFR xfr)
{

}
