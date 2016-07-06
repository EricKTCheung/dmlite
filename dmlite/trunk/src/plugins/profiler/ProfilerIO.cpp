/// @file   ProfilerPoolManager.cpp
/// @brief  ProfilerPoolManager implementation.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#include "Profiler.h"
#include "ProfilerXrdMon.h"
#include "XrdXrootdMonData.hh"

#include <string.h>

using namespace dmlite;

// --------------- ProfilerIOHandler

ProfilerIOHandler::ProfilerIOHandler(IOHandler* decorates,
    const std::string& pfn, int flags, const Extensible& extras,
    SecurityContext secCtx) throw(DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " path:" << pfn);

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );

  secCtx_ = secCtx;
  protocol_ = extras.getString("protocol");

  resetCounters();

  size_t file_size = 0;
  try {
    file_size = this->fstat().st_size;
  } catch (DmException& e) {
    Err(profilerlogname, "Could not determine filesize for " << pfn << ": " << e.code() << ", " << e.what());
  }

  sendUserIdentOrNOP(extras.getString("dav_user"));
  // we actually never use this, but active LFN in the stream
  //sendFileOpen(pfn);

  const std::string sfn_key = "dav_sfn";
  if (extras.hasField(sfn_key)) {
    reportXrdFileOpen(extras.getString(sfn_key), file_size);
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "Found an SFN for the file: " << extras.getString(sfn_key));
  } else {
    reportXrdFileOpen(pfn, file_size);
    Log(Logger::Lvl4, profilerlogmask, profilerlogname, "No SFN found, use PFN: " << pfn);
  }
}

ProfilerIOHandler::~ProfilerIOHandler()
{
  if (!file_closed_) {
    fillSsqStats();
    reportXrdFileClose(this->xfrstats_,
                       this->opsstats_,
                       this->ssqstats_,
                       XrdMonitor::file_flags_ | XrdXrootdMonFileHdr::forced);
  }
  resetCounters();
  reportXrdFileDiscAndFlushOrNOP();

  delete this->decorated_;
  free(this->decoratedId_);
}

size_t ProfilerIOHandler::read(char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, read, buffer, count);
  xfrstats_.read += ret;

  opsstats_.read += 1;
  if (opsstats_.rdMin > (int) ret)
    opsstats_.rdMin = ret;
  if (opsstats_.rdMax < (int) ret)
    opsstats_.rdMax = ret;

  ssq_.read += static_cast<double>(ret) * static_cast<double>(ret);

  return ret;
}

void ProfilerIOHandler::close(void) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");

  PROFILE(close);

  fillSsqStats();
  reportXrdFileClose(this->xfrstats_,
                     this->opsstats_,
                     this->ssqstats_,
                     XrdMonitor::file_flags_);
  resetCounters();
  file_closed_ = true;
}
struct ::stat ProfilerIOHandler::fstat(void) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");

  PROFILE_RETURN(struct ::stat, fstat);
}
size_t ProfilerIOHandler::write(const char* buffer, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, write, buffer, count);
  xfrstats_.write += ret;

  opsstats_.write += 1;
  if (opsstats_.wrMin > (int) ret)
    opsstats_.wrMin = ret;
  if (opsstats_.wrMax < (int) ret)
    opsstats_.wrMax = ret;

  ssq_.write += static_cast<double>(ret) * static_cast<double>(ret);

  return ret;
}
size_t ProfilerIOHandler::readv(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, readv, vector, count);
  xfrstats_.readv += ret;

  opsstats_.readv += 1;
  // ret: the number of bytes read
  if (opsstats_.rvMin > (int) ret)
    opsstats_.rvMin = ret;
  if (opsstats_.rvMax < (int) ret)
    opsstats_.rvMax = ret;
  // count: the number of segments to be read into
  opsstats_.rsegs += count;
  if (opsstats_.rsMin > (int) count)
    opsstats_.rsMin = count;
  if (opsstats_.rsMax < (int) count)
    opsstats_.rsMax = count;

  ssq_.readv += static_cast<double>(ret) * static_cast<double>(ret);
  ssq_.rsegs += static_cast<double>(count) * static_cast<double>(count);

  return ret;
}
size_t ProfilerIOHandler::writev(const struct iovec* vector, size_t count) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, writev, vector, count);
  xfrstats_.write += ret;

  opsstats_.write += 1;
  if (opsstats_.wrMin > (int) ret)
    opsstats_.wrMin = ret;
  if (opsstats_.wrMax < (int) ret)
    opsstats_.wrMax = ret;

  ssq_.write += static_cast<double>(ret) * static_cast<double>(ret);

  return ret;
}
size_t ProfilerIOHandler::pread(void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, pread, buffer, count, offset);
  xfrstats_.read += ret;

  opsstats_.read += 1;
  if (opsstats_.rdMin > (int) ret)
    opsstats_.rdMin = ret;
  if (opsstats_.rdMax < (int) ret)
    opsstats_.rdMax = ret;

  return ret;
}
size_t ProfilerIOHandler::pwrite(const void* buffer, size_t count, off_t offset) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " count:" << count);

  PROFILE_ASSIGN(size_t, pwrite, buffer, count, offset);
  xfrstats_.write += ret;

  opsstats_.write += 1;
  if (opsstats_.wrMin > (int) ret)
    opsstats_.wrMin = ret;
  if (opsstats_.wrMax < (int) ret)
    opsstats_.wrMax = ret;

  return ret;
}
void ProfilerIOHandler::seek(off_t offset, Whence whence) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " offs:" << offset);

  PROFILE(seek, offset, whence);
}
off_t ProfilerIOHandler::tell(void) throw (DmException)
{
  PROFILE_RETURN(off_t, tell);
}
void ProfilerIOHandler::flush(void) throw (DmException)
{
  PROFILE(flush);
}
bool ProfilerIOHandler::eof(void) throw (DmException)
{
  PROFILE_RETURN(bool, eof);
}

void ProfilerIOHandler::resetCounters()
{
  xfrstats_.read = 0;
  xfrstats_.readv = 0;
  xfrstats_.write = 0;

  opsstats_.read = 0;
  opsstats_.readv = 0;
  opsstats_.write = 0;
  opsstats_.rsMin = 0x7fff;
  opsstats_.rsMax = 0;
  opsstats_.rsegs = 0;
  opsstats_.rdMin = 0x7fffffff;
  opsstats_.rdMax = 0;
  opsstats_.rvMin = 0x7fffffff;
  opsstats_.rvMax = 0;
  opsstats_.wrMin = 0x7fffffff;
  opsstats_.wrMax = 0;

  ssq_.read = 0;
  ssq_.readv = 0;
  ssq_.rsegs = 0;
  ssq_.write = 0;

}


// --------------- ProfilerIODriver

ProfilerIODriver::ProfilerIODriver(IODriver* decorates) throw(DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, " Ctor");

  this->decorated_   = decorates;
  this->decoratedId_ = strdup( decorates->getImplId().c_str() );
}

ProfilerIODriver::~ProfilerIODriver() {

  delete this->decorated_;
  free(this->decoratedId_);

  Log(Logger::Lvl3, profilerlogmask, profilerlogname, "");
}


IOHandler* ProfilerIODriver::createIOHandler(const std::string& pfn,
                                             int flags,
                                             const Extensible& extras,
                                             mode_t mode) throw (DmException)
{
      Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");

      Extensible profilerExtras;
      profilerExtras.copy(extras);
      if (stack_->contains("protocol")) {
        profilerExtras["protocol"] = this->stack_->get("protocol");
      } else {
        profilerExtras["protocol"] = std::string("null");
      }
      SecurityContext secCtx = *(this->stack_->getSecurityContext());
      return new ProfilerIOHandler( decorated_->createIOHandler(pfn, flags, extras, mode),
                                   pfn, flags, profilerExtras, secCtx);
}


void ProfilerIODriver::setStackInstance(StackInstance* si) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  BaseInterface::setStackInstance(this->decorated_, si);
  this->stack_ = si;
}



void ProfilerIODriver::setSecurityContext(const SecurityContext* ctx) throw (DmException)
{
  Log(Logger::Lvl4, profilerlogmask, profilerlogname, "");
  BaseInterface::setSecurityContext(this->decorated_, ctx);
}

void ProfilerIODriver::doneWriting(const Location& loc) throw (DmException)
{
  Log(Logger::Lvl3, profilerlogmask, profilerlogname, " loc:" << loc.toString());

  PROFILE(doneWriting, loc);
}
