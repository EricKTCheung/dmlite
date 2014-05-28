/// @file   Profiler.cpp
/// @brief  Profiler plugin.

#include "Monitor.h"

using namespace dmlite;

Monitor::Monitor(): FD_(0), collector_addr(""), pseq_counter_(0)
{
  // get process startup time, or rather:
  // a time before any request is handled close to the startup time
  time(&startup_time);

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
  ret = getlogin_r(username, 1024);
  username_.assign(username);
}

Monitor::~Monitor() {}

void Monitor::init()
{
  if (collector_addr == "")
    return;

  FD_ = socket(PF_INET, SOCK_DGRAM, 0);

  const char* host;
  const char* port = "9930";

  std::vector<std::string> server;
  boost::split(server, collector_addr, boost::is_any_of(":/?"));

  if (server.size() > 0) {
    host = server[0].c_str();
  } else {
    syslog(LOG_MAKEPRI(LOG_USER, LOG_DEBUG), "%s: %s: %s = %s",
        "creating a memcache connection failed",
        "adding a server failed",
        "could not parse value",
        collector_addr.c_str());
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
}
int Monitor::send(const void *buf, size_t buf_len)
{
  boost::mutex::scoped_lock(send_mutex_);

  int ret;
  ret = sendto(FD_, buf, buf_len, 0, &dest_addr_, dest_addr_len_);

  return ret;
}

int Monitor::send_mon_map(kXR_char code, kXR_unt32 dictid, char *info)
{
  int ret;

  struct XrdXrootdMonMap mon_map;
  memset(&mon_map, 0, sizeof(mon_map));

  mon_map.hdr.code = code;
  mon_map.hdr.pseq = get_pseq_counter();
  mon_map.hdr.plen = htons(sizeof(mon_map));
  mon_map.hdr.stod = htonl(startup_time);

  mon_map.dictid = dictid;

  strncpy(mon_map.info, info, 1024+256);

  send(&mon_map, sizeof(mon_map));

  return ret;
}

int Monitor::send_server_ident()
{
  int ret;

  char info[1024+256];

  snprintf(info, 1024+256, "%s.%d:%d@%s\n&pgm=%s&ver=%s",
           username_.c_str(), pid_, sid_, hostname_.c_str(), "dpm", "1.8.8");

  send_mon_map('=', 0, info);

  return ret;
}


int Monitor::send_user_open_path(const std::string user, const std::string path)
{
  int ret;

  kXR_char dictid = 0;

  char info[1024+256];

  snprintf(info, 1024+256, "%s.%d:%d@%s\n%s",
           user.c_str(), pid_, sid_, hostname_.c_str(), path.c_str());

  send_mon_map('d', dictid, info);

  return ret;
}

int Monitor::send_user_login(std::string user)
{
  int ret;

  kXR_char dictid = 0;

  char info[1024+256];

  snprintf(info, 1024+256, "%s.%d:%d@%s\n&p=%s&n=%s&o=%s&r=%s&g=%s&m=%s",
           user.c_str(), pid_, sid_, hostname_.c_str(),
           "null", "null", "null", "null", "null", "null");

  send_mon_map('u', dictid, info);

  return ret;
}

char Monitor::get_pseq_counter()
{
  boost::mutex::scoped_lock(pseq_mutex_);
  pseq_counter_ = (pseq_counter_ + 1) % 256;

  return pseq_counter_;
}
