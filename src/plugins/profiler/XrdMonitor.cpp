/// @file   XrdMonitor.cpp
/// @brief  Profiler plugin.

#include "XrdMonitor.h"

using namespace dmlite;

time_t XrdMonitor::startup_time = 0;
std::set<std::string> XrdMonitor::collector_addr_list;

// configuration options
bool XrdMonitor::include_lfn_ = false;

bool XrdMonitor::is_initialized_ = false;
boost::mutex XrdMonitor::init_mutex_;

boost::mutex XrdMonitor::send_mutex_;

int XrdMonitor::FD_ = 0;
struct XrdMonitor::collector_info XrdMonitor::collector_[XrdMonitor::collector_max_];
int XrdMonitor::collector_count_ = 0;
//struct sockaddr XrdMonitor::dest_addr_;
//socklen_t XrdMonitor::dest_addr_len_;

// information for server ident msg
pid_t XrdMonitor::pid_ = 0;
kXR_int64 XrdMonitor::sid_ = 0;
std::string XrdMonitor::hostname_;
std::string XrdMonitor::username_;

// pseq counters
char XrdMonitor::pseq_counter_ = 0;
boost::mutex XrdMonitor::pseq_mutex_;

char XrdMonitor::fstream_pseq_counter_ = 0;
boost::mutex XrdMonitor::fstream_pseq_mutex_;

// dictid generator and mapping
kXR_unt32 XrdMonitor::dictid_ = 0;
boost::mutex XrdMonitor::dictid_mutex_;
std::map<std::string, kXR_unt32> XrdMonitor::dictid_map_;
boost::mutex XrdMonitor::dictid_map_mutex_;

// r- and f-stream buffers
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
  FD_ = socket(PF_INET, SOCK_DGRAM, 0);

  collector_count_ = 0;
  std::set<std::string>::iterator it;
  int i = 0;
  for (it = collector_addr_list.begin(); it != collector_addr_list.end(); ++i, ++it) {
    std::string collector_addr = *it;

    if (i > 1) {
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s",
          "could not add another collector server address",
          "maximum of two is already reached",
          collector_addr.c_str());

      break;
    }

    if (collector_addr == "")
      continue;

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
      continue;
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

    if (ret != 0) { // if it fails, we do not need to clean up
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s = %s",
          "could not connect to the collector server address",
          "adding a server failed",
          "could not connect",
          collector_addr.c_str());
      continue;
    }

    memcpy(&collector_[i].dest_addr, res->ai_addr, sizeof(collector_[i].dest_addr));
    collector_[i].dest_addr_len = res->ai_addrlen;

    ++collector_count_;

    freeaddrinfo(res);
  }

  return 0;
}

int XrdMonitor::send(const void *buf, size_t buf_len)
{
  boost::mutex::scoped_lock(send_mutex_);

  ssize_t ret;

  for (int i = 0; i < collector_count_; ++i) {
    struct sockaddr dest_addr = collector_[i].dest_addr;
    socklen_t dest_addr_len = collector_[i].dest_addr_len;

    ret = sendto(FD_, buf, buf_len, 0, &dest_addr, dest_addr_len);

    if (ret != buf_len) {
      syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s\n",
            "sending a message failed");
    }
  }

  if (ret == buf_len) {
    return 0;
  } else {
    return ret;
  }
}

int XrdMonitor::sendMonMap(const kXR_char code, const kXR_unt32 dictid, char *info)
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


int XrdMonitor::sendShortUserIdent(const kXR_unt32 dictid)
{
  int ret = 0;

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s",
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


int XrdMonitor::sendUserIdent(const kXR_unt32 dictid,
                              const std::string &protocol,
                              const std::string &userName,
                              const std::string &userHostname,
                              const std::string &vo,
                              const std::string &userDN)
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
           userName.c_str(), getpid(), /* gets the thread id */
           sid_, hostname_.c_str(),
           protocol.c_str(), userDN.c_str(), userHost.c_str(),
           vo.c_str(), "null", "null", userDN.c_str());

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

int XrdMonitor::sendFileOpen(const kXR_unt32 fileid, const std::string &path)
{
  if (include_lfn_) // do not send this if the path is included in the fstream
    return 0;

  int ret = 0;

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s\n%s",
           username_.c_str(), pid_, sid_, hostname_.c_str(),
           path.c_str());

  syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s:\n%s",
        "send fileopen",
        info);

  ret = sendMonMap(XROOTD_MON_MAPPATH, fileid, info);
  if (ret) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "failed sending FileOpen/Path msg");
  }
  return ret;
}

char XrdMonitor::getPseqCounter()
{
  char this_counter;
  {
    boost::mutex::scoped_lock(pseq_mutex_);
    pseq_counter_ = (pseq_counter_ + 1) & 0xFF;
    this_counter = pseq_counter_;
  }
  return this_counter;
}

char XrdMonitor::getFstreamPseqCounter()
{
  char this_counter;
  {
    boost::mutex::scoped_lock(fstream_pseq_mutex_);
    fstream_pseq_counter_ = (fstream_pseq_counter_ + 1) & 0xFF;
    this_counter = fstream_pseq_counter_;
  }
  return this_counter;
}

kXR_unt32 XrdMonitor::getDictId()
{
  kXR_unt32 this_dictid;
  {
    boost::mutex::scoped_lock(dictid_mutex_);
    dictid_ += 1;
    this_dictid = dictid_;
  }
  return htonl(this_dictid);
}

kXR_unt32 XrdMonitor::getDictIdFromDn(const std::string &dn)
{
  kXR_unt32 dictid;
  std::map<std::string, kXR_unt32>::iterator it;
  {
    boost::mutex::scoped_lock(dictid_map_mutex_);
    if ((it = dictid_map_.find(dn)) != dictid_map_.end()) {
      dictid = it->second;
    } else {
      dictid = getDictId();
      dictid_map_[dn] = dictid;
    }
  }
  return dictid;
}

void XrdMonitor::rmDictIdFromDn(const std::string &dn)
{
  boost::mutex::scoped_lock(dictid_map_mutex_);
  dictid_map_.erase(dn);
}

std::pair<kXR_unt32, bool> XrdMonitor::getDictIdFromDnMarkNew(const std::string &dn)
{
  kXR_unt32 dictid;
  bool new_dictid = false;
  std::map<std::string, kXR_unt32>::iterator it;
  {
    boost::mutex::scoped_lock(dictid_map_mutex_);
    if ((it = dictid_map_.find(dn)) != dictid_map_.end()) {
      dictid = it->second;
    } else {
      dictid = getDictId();
      dictid_map_[dn] = dictid;
      new_dictid = true;
    }
  }
  return std::pair<kXR_unt32, bool>(dictid, new_dictid);
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


void XrdMonitor::reportXrdRedirNsCmd(const kXR_unt32 dictid, const std::string &path, const int cmd_id)
{
  std::string full_path = getHostname() + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = (msg_size + 8) >> 3;

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


void XrdMonitor::reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &host, const int port,
                                   const std::string &path, const int cmd_id)
{
  std::string full_path = host + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = (msg_size + 8) >> 3;

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
      msg->arg0.rdr.Port = port;
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
  int buffer_size = (fileBuffer.next_slot) * sizeof(XrdXrootdMonFileHdr) + sizeof(XrdXrootdMonHeader) + sizeof(XrdXrootdMonFileTOD);

  // Fill the msg header
  buffer->hdr.code = XROOTD_MON_MAPFSTA;
  buffer->hdr.pseq = getFstreamPseqCounter();
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
  if (include_lfn_) {
    msg_size += sizeof(kXR_unt32) + path.length();
  }
  int slots = (msg_size + 8) >> 3;
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

      if (include_lfn_) {
        msg->Hdr.recFlag |= XrdXrootdMonFileHdr::hasLFN;
        msg->ufn.user = dictid;
        strncpy(msg->ufn.lfn, path.c_str(), aligned_path_len);
      }

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


void XrdMonitor::reportXrdFileClose(const kXR_unt32 fileid, const XrdXrootdMonStatXFR xfr, const bool forced)
{
  int msg_size = sizeof(XrdXrootdMonFileHdr) + sizeof(XrdXrootdMonStatXFR);
  // TODO: optimize, get rid of the %
  int slots = (msg_size + 8) >> 3;

  XrdXrootdMonFileCLS *msg;
  {
    boost::mutex::scoped_lock(file_mutex_);

    msg = (XrdXrootdMonFileCLS *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "failed sending FILE msg");
      } else {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "sent FILE msg");
      }
      msg = (XrdXrootdMonFileCLS *) getFileBufferNextEntry(slots);
    }

    if (msg != 0x00) {
      msg->Hdr.recType = XrdXrootdMonFileHdr::isClose;
      msg->Hdr.recFlag = 0;
      if (forced)
        msg->Hdr.recFlag |= XrdXrootdMonFileHdr::forced;
      msg->Hdr.recSize = htons(static_cast<short>(slots*sizeof(XrdXrootdMonFileHdr)));
      msg->Hdr.fileID = fileid;

      msg->Xfr.read = xfr.read;
      msg->Xfr.readv = xfr.readv;
      msg->Xfr.write = xfr.write;

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

void XrdMonitor::reportXrdFileDisc(const kXR_unt32 dictid)
{
  int msg_size = sizeof(XrdXrootdMonFileHdr);
  int slots = 1;

  XrdXrootdMonFileDSC *msg;
  {
    boost::mutex::scoped_lock(file_mutex_);

    msg = (XrdXrootdMonFileDSC *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "failed sending FILE msg");
      } else {
        syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
            "sent FILE msg");
      }
      msg = (XrdXrootdMonFileDSC *) getFileBufferNextEntry(slots);
    }

    if (msg != 0x00) {
      msg->Hdr.recType = XrdXrootdMonFileHdr::isDisc;
      msg->Hdr.recFlag = 0;
      msg->Hdr.recSize = htons(static_cast<short>(slots*sizeof(XrdXrootdMonFileHdr)));
      msg->Hdr.userID = dictid;

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

void XrdMonitor::flushXrdFileStream()
{
  int ret = XrdMonitor::sendFileBuffer();
  if (ret) {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "failed sending FILE msg");
  } else {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s",
        "sent FILE msg");
  }
}
