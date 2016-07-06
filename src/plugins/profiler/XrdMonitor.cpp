/// @file   XrdMonitor.cpp
/// @brief  Profiler plugin.

#include <sys/syscall.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "XrdMonitor.h"

extern "C" char *program_invocation_short_name;

using namespace dmlite;

time_t XrdMonitor::startup_time = 0;
std::set<std::string> XrdMonitor::collector_addr_list;

int XrdMonitor::file_flags_ = 0;

// configuration options
bool XrdMonitor::include_lfn_ = false;
bool XrdMonitor::include_auth_ = false;
bool XrdMonitor::include_dn_ = false;

bool XrdMonitor::is_initialized_ = false;
boost::mutex XrdMonitor::init_mutex_;

boost::mutex XrdMonitor::send_mutex_;

int XrdMonitor::FD_ = 0;
struct XrdMonitor::collector_info XrdMonitor::collector_[XrdMonitor::collector_max_];
int XrdMonitor::collector_count_ = 0;

// information for server ident msg
pid_t XrdMonitor::pid_ = 0;
kXR_int64 XrdMonitor::sid_ = 0;
std::string XrdMonitor::hostname_;
std::string XrdMonitor::processname_;
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
  close(FD_);
}


XrdMonitor::~XrdMonitor() {}


int XrdMonitor::initOrNOP()
{
  int ret = 0;

  boost::mutex::scoped_lock lock(init_mutex_);
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
    Err(profilerlogname, "initRedirBuffer failed: error code = " << ret);
    return ret;
  }

  ret = insertRedirBufferWindowEntry();
  if (ret < 0) {
    Err(profilerlogname, "insertRedirBufferWindowEntry failed: error code = " << ret);
    return ret;
  }

  ret = initFileBuffer(file_max_buffer_size_);
  if (ret < 0) {
    Err(profilerlogname, "initFileBuffer failed: error code = " << ret);
    return ret;
  }

  ret = initCollector();
  if (ret < 0) {
    Err(profilerlogname, "initCollector failed: error code = " << ret);
    return ret;
  }

  ret = initServerIdentVars();
  if (ret < 0) {
    Err(profilerlogname, "initServerIdentVars failed: error code = " << ret);
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

  processname_ = "dpm-";
#if defined(__APPLE__)
  processname_.append(std::string(getprogname()));
#else
  processname_.append(std::string(program_invocation_short_name));
#endif

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
  for (it = collector_addr_list.begin(); it != collector_addr_list.end(); ++it) {
    std::string collector_addr = *it;

    if (i >= XrdMonitor::collector_max_) {
      Err(profilerlogname, "could not add another collector server address: "
              << collector_addr
              << ": maximum of "
              << XrdMonitor::collector_max_
              << " is already reached"
          );
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
      Err(profilerlogname,
              "could not read the collector server address: adding a server failed: could not parse value = "
              << collector_addr.c_str()
         );
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
      Err(profilerlogname,
              "could not connect to the collector server address: adding a server failed: could not connect = "
              << collector_addr.c_str()
         );
      continue;
    }

    memcpy(&collector_[i].dest_addr, res->ai_addr, sizeof(collector_[i].dest_addr));
    collector_[i].dest_addr_len = res->ai_addrlen;
    collector_[i].name = collector_addr;

    ++i; // put the next collector in the next slot

    freeaddrinfo(res);
  }

  collector_count_ = i;

  return 0;
}

std::string XrdMonitor::getHostFromIP(const std::string& hostOrIp)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "host = " << hostOrIp);
  std::string hostname = hostOrIp;
  int ret;
  struct sockaddr_in sa;
  struct sockaddr_in6 sa6;
  bool isIPv6 = false;
  // try IPv4
  sa.sin_family = AF_INET;
  ret = inet_pton(sa.sin_family, hostOrIp.c_str(), &(sa.sin_addr));
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "IP address is IPv4: " << ((ret == 1) ? "true" : "false"));
  if (ret < 1) {
    //try IPv6
    isIPv6 = true;
    sa6.sin6_family = AF_INET6;
    ret = inet_pton(sa6.sin6_family, hostOrIp.c_str(), &(sa6.sin6_addr));
    Log(Logger::Lvl3, profilerlogmask, profilerlogname, "IP address is IPv6: " << ((ret == 1) ? "true" : "false"));
  }
  if (ret == 1) {
    char hostname_cstr[1024];
    if (isIPv6)
      ret = getnameinfo((struct sockaddr *) &sa6, sizeof(sa6), hostname_cstr, sizeof(hostname_cstr), NULL, 0, 0);
    else
      ret = getnameinfo((struct sockaddr *) &sa, sizeof(sa), hostname_cstr, sizeof(hostname_cstr), NULL, 0, 0);
    if (ret == 0) {
      hostname = std::string(hostname_cstr);
      Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Hostname is " << hostname);
    } else {
      Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Could not get hostname."
          << " Error code = " << gai_strerror(ret));
    }
  } else {
    Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Argument is not valid ip address.");
  }
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "Exiting.");
  return hostname;
}

int XrdMonitor::send(const void *buf, size_t buf_len)
{
  boost::mutex::scoped_lock lock(send_mutex_);

  ssize_t ret;
  int errsv;

  for (int i = 0; i < collector_count_; ++i) {
    struct sockaddr dest_addr = collector_[i].dest_addr;
    socklen_t dest_addr_len = collector_[i].dest_addr_len;

    ret = sendto(FD_, buf, buf_len, 0, &dest_addr, dest_addr_len);
    errsv = errno;

    if (ret != (ssize_t) buf_len) {
      char errbuffer[256];
      strerror_r(errsv, errbuffer, 256);
      Err(profilerlogname, "sending a message failed collector = "
              << collector_[i].name.c_str()
              << ", reason = " << errbuffer
         );
    }
  }

  if (ret == (ssize_t) buf_len) {
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
      username_.c_str(), pid_, sid_, hostname_.c_str(), processname_.c_str(), "1.8.9");

  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "send serverident:\n" << info);

  ret = sendMonMap(XROOTD_MON_MAPIDNT, 0, info);
  if (ret) {
    Err(profilerlogname, "failed sending ServerIdent msg: error code = " << ret);
  }
  return ret;
}


int XrdMonitor::sendShortUserIdent(const kXR_unt32 dictid)
{
  int ret = 0;

  char info[1024+256];
  snprintf(info, 1024+256, "%s.%d:%lld@%s",
           username_.c_str(), pid_, sid_, hostname_.c_str());

  Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "send short userident:\n" << info);

  ret = sendMonMap(XROOTD_MON_MAPUSER, dictid, info);
  if (ret) {
    Err(profilerlogname, "failed sending UserIdent msg: error code = " << ret);
  }
  return ret;
}


int XrdMonitor::sendUserIdent(const kXR_unt32 dictid,
                              const std::string &protocol,
                              const std::string &authProtocol,
                              const std::string &userName,
                              const std::string &userHostname,
                              const std::string &vo,
                              const std::string &userDN)
{
  int ret = 0;

  std::string userHost;
  if (userHostname.length() > 0) {
    userHost = getHostFromIP(userHostname);
  } else {
    userHost = "null";
  }

  pid_t tid = syscall(SYS_gettid);

  /* combine thread id with the dictid
   *
   * with sufficient load on apache, we see again (some)
   * orphaned f-stream messages. with its thread model in
   * event mode, it _might_ be (totally unconfirmed) that
   * this is due to packet reordering on the network. If a
   * user disconnects, we send its fstream. When the same
   * user reconnects and gets served by the same thread,
   * its userIdent msg will have the same userName.tid
   * combination. If then the packets of the fstream and
   * userIdent gets reordered, so that the userIdent
   * arrives before, its user will replace the old one
   * in the gled usermap and its dictid be orphaned --
   * the fstream cannot be parsed then.
   *
   * Because of this hypothetical situation, or any
   * other that might lead to the same result, we add the
   * dictid to the thread id and call it user_id :)
   *
   * Remember the dictid is in network byte order,
   * we convert it to limit its influence.
   */
  unsigned int user_id = static_cast<unsigned int>(tid) + ntohl(dictid);

  char info[1024+256];
  int cnt = 0;
  cnt = snprintf(info, 1024+256, "%s/%s.%d:%lld@%s",
           protocol.c_str(), userName.c_str(), user_id, sid_, hostname_.c_str());
  if (XrdMonitor::include_auth_) {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "including auth info");
    if (XrdMonitor::include_dn_) {
      Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "including userdn");
      snprintf(info+cnt, 1024+256-cnt, "\n&p=%s&n=%s&h=%s&o=%s&r=%s&g=%s&m=%s",
               authProtocol.c_str(), userDN.c_str(), userHost.c_str(),
               vo.c_str(), "null", "null", userDN.c_str());
    } else {
      snprintf(info+cnt, 1024+256-cnt, "\n&p=%s&n=%s&h=%s&o=%s&r=%s&g=%s&m=%s",
               authProtocol.c_str(), "nobody", userHost.c_str(),
               "nogroup", "null", "null", "null");
    }
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "NOT including any auth info");
  }

  Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "send userident:\n" << info);

  ret = sendMonMap(XROOTD_MON_MAPUSER, dictid, info);
  if (ret) {
    Err(profilerlogname, "failed sending UserIdent msg, error code = " << ret);
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

  Log(Logger::Lvl4, profilerlogmask, profilerlogname,  "send fileopen:\n" << info);

  ret = sendMonMap(XROOTD_MON_MAPPATH, fileid, info);
  if (ret) {
    Err(profilerlogname, "failed sending FileOpen/Path msg, error code = " << ret);
  }
  return ret;
}

char XrdMonitor::getPseqCounter()
{
  char this_counter;
  {
    boost::mutex::scoped_lock lock(pseq_mutex_);
    pseq_counter_ = (pseq_counter_ + 1) & 0xFF;
    this_counter = pseq_counter_;
  }
  return this_counter;
}

char XrdMonitor::getFstreamPseqCounter()
{
  char this_counter;
  {
    boost::mutex::scoped_lock lock(fstream_pseq_mutex_);
    fstream_pseq_counter_ = (fstream_pseq_counter_ + 1) & 0xFF;
    this_counter = fstream_pseq_counter_;
  }
  return this_counter;
}

kXR_unt32 XrdMonitor::getDictId()
{
  kXR_unt32 this_dictid;
  {
    boost::mutex::scoped_lock lock(dictid_mutex_);
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
    boost::mutex::scoped_lock lock(dictid_map_mutex_);
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
  boost::mutex::scoped_lock lock(dictid_map_mutex_);
  dictid_map_.erase(dn);
}

std::pair<kXR_unt32, bool> XrdMonitor::getDictIdFromDnMarkNew(const std::string &dn)
{
  kXR_unt32 dictid;
  bool new_dictid = false;
  std::map<std::string, kXR_unt32>::iterator it;
  {
    boost::mutex::scoped_lock lock(dictid_map_mutex_);
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
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  std::string full_path = getHostname() + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = (msg_size + 8) >> 3;

  XrdXrootdMonRedir *msg;
  {
    boost::mutex::scoped_lock lock(redir_mutex_);

    msg = getRedirBufferNextEntry(slots);

    // the buffer could be full ..
    if (msg == 0x00) {
      int ret = XrdMonitor::sendRedirBuffer();
      if (ret) {
        Err(profilerlogname, "failed sending REDIR msg, error code = " << ret);
      } else {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent REDIR msg");
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
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "added new REDIR msg");
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "did not send/add new REDIR msg");
  }
}


void XrdMonitor::reportXrdRedirCmd(const kXR_unt32 dictid, const std::string &host, const int port,
                                   const std::string &path, const int cmd_id)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  std::string full_path = host + ":" + path;

  int msg_size = sizeof(XrdXrootdMonRedir) + full_path.length() + 1;
  int slots = (msg_size + 8) >> 3;

  XrdXrootdMonRedir *msg;
  {
    boost::mutex::scoped_lock lock(redir_mutex_);

    msg = getRedirBufferNextEntry(slots);

    // the buffer could be full ..
    if (msg == 0x00) {
      int ret = XrdMonitor::sendRedirBuffer();
      if (ret) {
        Err(profilerlogname, "failed sending REDIR msg, error code = " << ret);
      } else {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent REDIR msg");
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
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "added new REDIR msg");
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "did not send/add new REDIR msg");
  }
}


int XrdMonitor::initFileBuffer(int max_size)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

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

  fileBuffer.msg_buffer->tod.tBeg = htonl(static_cast<int>(time(0)));

  return 0;
}

XrdXrootdMonFileHdr* XrdMonitor::getFileBufferNextEntry(int slots)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  // space from the last msg + this msg + the ending window message
  if (fileBuffer.next_slot + slots + 1 < fileBuffer.max_slots) {
    ++fileBuffer.total_msgs;
    return &(fileBuffer.msg_buffer->info[fileBuffer.next_slot]);
  } else
    return 0x00;
}

int XrdMonitor::advanceFileBufferNextEntry(int slots)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  int ret = 0;

  fileBuffer.next_slot += slots;

  return ret;
}


int XrdMonitor::sendFileBuffer()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

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
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  static const int min_msg_size = sizeof(XrdXrootdMonFileOPN) - sizeof(XrdXrootdMonFileLFN);
  int msg_size = min_msg_size;
  if (include_lfn_) {
    msg_size += sizeof(kXR_unt32) + path.length();
  }
  int slots = (msg_size + 8) >> 3;
  int aligned_path_len = path.length() + (slots * sizeof(XrdXrootdMonFileHdr) - msg_size);

  XrdXrootdMonFileOPN *msg;
  {
    boost::mutex::scoped_lock lock(file_mutex_);

    msg = (XrdXrootdMonFileOPN *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        Err(profilerlogname, "failed sending FILE msg, error code = " << ret);
      } else {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent FILE msg");
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
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "added new FILE msg");
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "did not send/add new REDIR msg");
  }
}


void XrdMonitor::reportXrdFileClose(const kXR_unt32 fileid, const XrdXrootdMonStatXFR xfr,
                                    const XrdXrootdMonStatOPS ops,
                                    const XrdXrootdMonStatSSQ ssq,
                                    const int flags)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  int msg_size = sizeof(XrdXrootdMonFileHdr) + sizeof(XrdXrootdMonStatXFR);
  if (flags & XrdXrootdMonFileHdr::hasSSQ)
    msg_size += sizeof(XrdXrootdMonStatOPS) + sizeof(XrdXrootdMonStatSSQ);
  else if (flags & XrdXrootdMonFileHdr::hasOPS)
    msg_size += sizeof(XrdXrootdMonStatOPS);
  int slots = (msg_size + 8) >> 3;

  XrdXrootdMonFileCLS *msg;
  {
    boost::mutex::scoped_lock lock(file_mutex_);

    msg = (XrdXrootdMonFileCLS *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        Err(profilerlogname, "failed sending FILE msg, error code = " << ret);
      } else {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent FILE msg");
      }
      msg = (XrdXrootdMonFileCLS *) getFileBufferNextEntry(slots);
    }

    if (msg != 0x00) {
      msg->Hdr.recType = XrdXrootdMonFileHdr::isClose;
      msg->Hdr.recFlag = 0;
      msg->Hdr.recFlag |= flags;
      msg->Hdr.recSize = htons(static_cast<short>(slots*sizeof(XrdXrootdMonFileHdr)));
      msg->Hdr.fileID = fileid;

      msg->Xfr.read = htonll(xfr.read);
      msg->Xfr.readv = htonll(xfr.readv);
      msg->Xfr.write = htonll(xfr.write);

      // report unusual values -- > 2^30
      if (xfr.read > 8589934592LL || xfr.read < 0LL)
        Err(profilerlogname, " bytes read:" << xfr.read);
      if (xfr.readv > 8589934592LL || xfr.readv < 0LL)
        Err(profilerlogname, " bytes readv:" << xfr.readv);
      if (xfr.write > 8589934592LL || xfr.write < 0LL)
        Err(profilerlogname, " bytes write:" << xfr.write);

      if (flags & XrdXrootdMonFileHdr::hasOPS || flags & XrdXrootdMonFileHdr::hasSSQ) {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "add OPS file statistics");
        msg->Ops.read  = htonl(ops.read);   // Number of read()  calls
        msg->Ops.readv = htonl(ops.readv);   // Number of readv() calls
        msg->Ops.write = htonl(ops.write);   // Number of write() calls
        msg->Ops.rsMin = htons(ops.rsMin);   // Smallest  readv() segment count
        msg->Ops.rsMax = htons(ops.rsMax);   // Largest   readv() segment count
        msg->Ops.rsegs = htonll(ops.rsegs);   // Number of readv() segments
        msg->Ops.rdMin = htonl(ops.rdMin);   // Smallest  read()  request size
        msg->Ops.rdMax = htonl(ops.rdMax);   // Largest   read()  request size
        msg->Ops.rvMin = htonl(ops.rvMin);   // Smallest  readv() request size
        msg->Ops.rvMax = htonl(ops.rvMax);   // Largest   readv() request size
        msg->Ops.wrMin = htonl(ops.wrMin);   // Smallest  write() request size
        msg->Ops.wrMax = htonl(ops.wrMax);   // Largest   write() request size

        // withouth any read/readv/write ops, reset their min value
        if (!ops.read) {
          msg->Ops.rdMin = 0;
        }
        if (!ops.rsegs) {
          msg->Ops.rsMin = 0;
          msg->Ops.rvMin = 0;
        }
        if (!ops.write) {
          msg->Ops.wrMin = 0;
        }
      }
      if (flags & XrdXrootdMonFileHdr::hasSSQ) {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "add SSQ file statistics");
        // WARNING: these value are already in network-byte order!
        msg->Ssq.read.dlong = ssq.read.dlong;
        msg->Ssq.readv.dlong = ssq.readv.dlong;
        msg->Ssq.rsegs.dlong = ssq.rsegs.dlong;
        msg->Ssq.write.dlong = ssq.write.dlong;
      }

      advanceFileBufferNextEntry(slots);
    }
  }

  if (msg != 0x00) {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "added new FILE msg");
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "did not send/add new REDIR msg");
  }
}

void XrdMonitor::reportXrdFileDisc(const kXR_unt32 dictid)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  int slots = 1;

  XrdXrootdMonFileDSC *msg;
  {
    boost::mutex::scoped_lock lock(file_mutex_);

    msg = (XrdXrootdMonFileDSC *) getFileBufferNextEntry(slots);

    if (msg == 0x00) {
      int ret = XrdMonitor::sendFileBuffer();
      if (ret) {
        Err(profilerlogname, "failed sending FILE msg, error code = " << ret);
      } else {
        Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent FILE msg");
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
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "added new FILE msg");
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "did not send/add new REDIR msg");
  }
}

void XrdMonitor::flushXrdFileStream()
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Entering");

  int ret;
  {
    boost::mutex::scoped_lock lock(file_mutex_);
    ret = XrdMonitor::sendFileBuffer();
  }
  if (ret) {
    Err(profilerlogname, "failed sending FILE msg, error code = " << ret);
  } else {
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "sent FILE msg");
  }
}
