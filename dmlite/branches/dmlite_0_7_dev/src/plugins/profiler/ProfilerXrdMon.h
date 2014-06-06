/// @file   ProfilerXrdMon.h
/// @brief  Profiler plugin.
/// @author Martin Hellmich <mhellmic@cern.ch>
#ifndef PROFILERXRDMON_H
#define PROFILERXRDMON_H

#include "Profiler.h"

namespace dmlite {

  class ProfilerXrdMon
  {
    public:
      ProfilerXrdMon() {};

      ~ProfilerXrdMon();

      void reportXrdRedirCmd(const std::string &path, const int cmd_id);
      void reportXrdRedirCmd(const Location &loc, const int cmd_id);
      void sendUserIdentOrNOP();

    protected:
      XrdXrootdMonStatXFR xfrstats_;
      bool file_closed_;

      StackInstance *stack_;
  };
};

#endif //ProfilerXrdMon
