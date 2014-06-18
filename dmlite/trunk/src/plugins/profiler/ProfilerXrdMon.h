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
      void reportXrdFileClose(const XrdXrootdMonStatXFR xfr, const bool forced = false);
      void reportXrdFileDiscAndFlush();
      void reportXrdFileDiscAndFlushOrNOP();

      void sendUserIdentOrNOP();

    protected:
      XrdXrootdMonStatXFR xfrstats_;
      bool file_closed_;

      StackInstance *stack_;

      kXR_unt32 getDictId();
      bool hasDictId();
      void rmDictId();
      kXR_unt32 getFileId();
      void rmFileId();
      std::string getShortUserName(const std::string &username);
  };
};

#endif //ProfilerXrdMon
