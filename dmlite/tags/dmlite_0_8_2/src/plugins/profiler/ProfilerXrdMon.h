/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>
#ifndef PROFILERXRDMON_H
#define PROFILERXRDMON_H

#include "XrdMonitor.h"
#include "XrdXrootdMonData.hh"

#include <dmlite/cpp/pooldriver.h>
#include <dmlite/cpp/authn.h>
#include <dmlite/cpp/dmlite.h>

namespace dmlite {

  class ProfilerXrdMon
  {
    public:
      ProfilerXrdMon();

      virtual ~ProfilerXrdMon();

      void reportXrdRedirCmd(const std::string &path, const int cmd_id);
      void reportXrdRedirCmd(const Location &loc, const int cmd_id);
      void reportXrdFileOpen(const std::string &path, const long long file_size);
      void reportXrdFileClose(const XrdXrootdMonStatXFR xfr,
                              const XrdXrootdMonStatOPS ops,
                              const XrdXrootdMonStatSSQ ssq,
                              const int flags = 0);
      void reportXrdFileDiscAndFlush();
      void reportXrdFileDiscAndFlushOrNOP();

      void sendUserIdentOrNOP(std::string user_dn);

    protected:
      XrdXrootdMonStatXFR xfrstats_;
      XrdXrootdMonStatOPS opsstats_;
      XrdXrootdMonStatSSQ ssqstats_;
      bool file_closed_;

      // either the stackinstance or protocol_
      // and secCtx_ have to exist
      StackInstance *stack_;

      kXR_unt32 dictid_;
      kXR_unt32 fileid_;
      std::string protocol_;
      SecurityContext secCtx_;

      kXR_unt32 getDictId();
      bool hasDictId();
      void rmDictId();
      kXR_unt32 getFileId();
      void rmFileId();
      std::string getShortUserName(const std::string &username);
      void fillSsqStats();

      struct {
        double read;     // sum(read_size[i] **2) i = 1 to Ops.read
        double readv;    // sum(readv_size[i]**2) i = 1 to Ops.readv
        double rsegs;    // sum(readv_segs[i]**2) i = 1 to Ops.readv
        double write;    // sum(write_size[i]**2) i = 1 to Ops.write
      } ssq_;

      const SecurityContext* getSecurityContext();
      std::string getProtocol();
  };
};

#endif //ProfilerXrdMon
