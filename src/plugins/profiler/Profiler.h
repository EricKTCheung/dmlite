/// @file   Profiler.h
/// @brief  Profiler plugin.
/// @author Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILER_H
#define	PROFILER_H

#include <cstdarg>
#include <ctime>
#include <iostream>
#include <string>

#ifdef __APPLE__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include <dmlite/cpp/dmlite.h>

#include "XrdMonitor.h"
#include "ProfilerCatalog.h"
#include "ProfilerPoolManager.h"
#include "dmlite/cpp/io.h"
#include "ProfilerIO.h"

#include "utils/logger.h"



namespace dmlite {
  
  
  
extern Logger::bitmask profilerlogmask;
extern Logger::component profilerlogname;

extern Logger::bitmask profilertimingslogmask;
extern Logger::component profilertimingslogname;

/// Concrete factory for Profiler plugin.
  class ProfilerFactory: public CatalogFactory, public PoolManagerFactory, public IODriverFactory {
public:
  /// Constructor
  ProfilerFactory(CatalogFactory* catalogFactory, PoolManagerFactory* poolManagerFactory,
                  IODriverFactory *ioFactory) throw (DmException);
  /// Destructor
  ~ProfilerFactory();

  /// String ID of the implementation.
  std::string getImplId(void) const throw() {
    return std::string("ProfilerFactory");
  }

  void configure(const std::string& key, const std::string& value) throw (DmException);

  virtual Catalog*     createCatalog(PluginManager*)     throw (DmException);
  virtual PoolManager* createPoolManager(PluginManager*) throw (DmException);
  virtual IODriver*   createIODriver(PluginManager*)   throw (DmException);
protected:
  void initXrdMonitorIfNotInitialized() throw (DmException);

  /// The decorated Catalog factory.
  CatalogFactory* nestedCatalogFactory_;
  /// The decorated PoolManager factory.
  PoolManagerFactory* nestedPoolManagerFactory_;

  /// The decorated IODriver factory
  IODriverFactory* nestedIODriverFactory_;

private:
};

/// Profiler macro
#ifndef __APPLE__
#define PROFILE(method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
struct timespec  start, end;\
double           duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &start);\
}\
try {\
  this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
    clock_gettime(CLOCK_REALTIME, &end);\
    duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
    duration /= 1000;\
    Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
  }\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &end);\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
}


/// Profile with pointers
#define PROFILE_RETURN(type, method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
type             ret;\
struct timespec  start, end;\
double           duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &start);\
}\
try {\
  ret = this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
    clock_gettime(CLOCK_REALTIME, &end);\
    duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
    duration /= 1000;\
    Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
  }\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &end);\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
}\
return ret;

/// Profile and use ret afterwards
#define PROFILE_ASSIGN(type, method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
type             ret;\
struct timespec  start, end;\
double           duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &start);\
}\
try {\
  ret = this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
    clock_gettime(CLOCK_REALTIME, &end);\
    duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
    duration /= 1000;\
    Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
  }\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_gettime(CLOCK_REALTIME, &end);\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << "::"#method << " " << duration);\
}

#else // ifndef __APPLE__
#define PROFILE(method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
struct timespec  start, end;\
double           duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_serv_t cclock;\
  mach_timespec_t mts;\
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
  clock_get_time(cclock, &mts);\
  mach_port_deallocate(mach_task_self(), cclock);\
  start.tv_sec = mts.tv_sec;\
  start.tv_nsec = mts.tv_nsec;\
}\
try {\
  this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
    {\
      clock_serv_t cclock;\
      mach_timespec_t mts;\
      host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
      clock_get_time(cclock, &mts);\
      mach_port_deallocate(mach_task_self(), cclock);\
      end.tv_sec = mts.tv_sec;\
      end.tv_nsec = mts.tv_nsec;\
    }\
    duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
    duration /= 1000;\
    Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
  }\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  {\
    clock_serv_t cclock;\
    mach_timespec_t mts;\
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
    clock_get_time(cclock, &mts);\
    mach_port_deallocate(mach_task_self(), cclock);\
    end.tv_sec = mts.tv_sec;\
    end.tv_nsec = mts.tv_nsec;\
  }\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
}

/// Profile with pointers
#define PROFILE_RETURN(type, method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
type             ret;\
struct timespec  start, end;\
double           duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_serv_t cclock;\
  mach_timespec_t mts;\
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
  clock_get_time(cclock, &mts);\
  mach_port_deallocate(mach_task_self(), cclock);\
  start.tv_sec = mts.tv_sec;\
  start.tv_nsec = mts.tv_nsec;\
}\
try {\
  ret = this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
    {\
      clock_serv_t cclock;\
      mach_timespec_t mts;\
      host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
      clock_get_time(cclock, &mts);\
      mach_port_deallocate(mach_task_self(), cclock);\
      end.tv_sec = mts.tv_sec;\
      end.tv_nsec = mts.tv_nsec;\
    }\
    duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
    duration /= 1000;\
    Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
  }\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  {\
    clock_serv_t cclock;\
    mach_timespec_t mts;\
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
    clock_get_time(cclock, &mts);\
    mach_port_deallocate(mach_task_self(), cclock);\
    end.tv_sec = mts.tv_sec;\
    end.tv_nsec = mts.tv_nsec;\
  }\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
}\
return ret;

/// Profile and use ret afterwards
#define PROFILE_ASSIGN(type, method, ...)\
if (this->decorated_ == 0x00)\
  throw DmException(DMLITE_SYSERR(EFAULT),\
                    std::string("There is no plugin to delegate the call "#method));\
type            ret;\
struct timespec start, end;\
double          duration;\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  clock_serv_t cclock;\
  mach_timespec_t mts;\
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
  clock_get_time(cclock, &mts);\
  mach_port_deallocate(mach_task_self(), cclock);\
  start.tv_sec = mts.tv_sec;\
  start.tv_nsec = mts.tv_nsec;\
}\
try {\
  ret = this->decorated_->method(__VA_ARGS__);\
} catch (DmException& e) {\
  {\
    clock_serv_t cclock;\
    mach_timespec_t mts;\
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
    clock_get_time(cclock, &mts);\
    mach_port_deallocate(mach_task_self(), cclock);\
    end.tv_sec = mts.tv_sec;\
    end.tv_nsec = mts.tv_nsec;\
  }\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
  throw;\
}\
if (Logger::get()->getLevel() >= Logger::Lvl4 && Logger::get()->isLogged(profilertimingslogmask)) {\
  {\
    clock_serv_t cclock;\
    mach_timespec_t mts;\
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);\
    clock_get_time(cclock, &mts);\
    mach_port_deallocate(mach_task_self(), cclock);\
    end.tv_sec = mts.tv_sec;\
    end.tv_nsec = mts.tv_nsec;\
  }\
  duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
  duration /= 1000;\
  Log(Logger::Lvl4, profilertimingslogmask, profilertimingslogname, this->decoratedId_ << " " << duration);\
}
#endif
};

#endif	// PROFILER_H
