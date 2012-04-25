/// @file    plugins/profiler/Profiler.h
/// @brief   Profiler plugin.
/// @author  Alejandro Álvarez Ayllón <aalvarez@cern.ch>
#ifndef PROFILER_H
#define	PROFILER_H

#include <cstdarg>
#include <ctime>
#include <iostream>
#include <string>
#include <syslog.h>

#include <dmlite/dmlite++.h>

#include "ProfilerCatalog.h"
#include "ProfilerPoolManager.h"

namespace dmlite {

/// Concrete factory for Profiler plugin.
class ProfilerFactory: public CatalogFactory, public PoolManagerFactory {
public:
  /// Constructor
  ProfilerFactory(CatalogFactory* catalogFactory, PoolManagerFactory* poolManagerFactory) throw (DmException);
  /// Destructor
  ~ProfilerFactory();

  void configure(const std::string& key, const std::string& value) throw (DmException);

  Catalog*     createCatalog(StackInstance* si)     throw (DmException);
  PoolManager* createPoolManager(StackInstance* si) throw (DmException);

protected:
  /// The decorated Catalog factory.
  CatalogFactory* nestedCatalogFactory_;
  /// The decorated PoolManager factory.
  PoolManagerFactory* nestedPoolManagerFactory_;

private:
};

/// Profiler macro
#define PROFILE(method, ...)\
struct timespec  start, end;\
double           duration;\
DmException exception;\
bool             failed = false;\
if (this->decorated_ == 0x00)\
  throw DmException(DM_NULL_POINTER, std::string("There is no plugin to delegate the call "#method));\
clock_gettime(CLOCK_REALTIME, &start);\
try {\
  this->decorated_->method(__VA_ARGS__);\
} catch (DmException e) {\
  exception = e;\
  failed = true;\
}\
clock_gettime(CLOCK_REALTIME, &end);\
duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
duration /= 1000;\
syslog(LOG_USER | LOG_DEBUG, "%s::"#method" %f", this->decoratedId_, duration);\
if (failed)\
  throw exception;

/// Profile with pointers
#define PROFILE_RETURN(type, method, ...)\
struct timespec start, end;\
double          duration;\
DmException     exception;\
bool            failed = false;\
type            ret;\
if (this->decorated_ == 0x00)\
  throw DmException(DM_NULL_POINTER, std::string("There is no plugin to delegate the call "#method));\
clock_gettime(CLOCK_REALTIME, &start);\
try {\
  ret = this->decorated_->method(__VA_ARGS__);\
} catch (DmException e) {\
  exception = e;\
  failed = true;\
}\
clock_gettime(CLOCK_REALTIME, &end);\
duration = ((end.tv_sec - start.tv_sec) * 1E9) + (end.tv_nsec - start.tv_nsec);\
duration /= 1000;\
syslog(LOG_USER | LOG_DEBUG, "%s::"#method" %f", this->decoratedId_, duration);\
if (failed)\
  throw exception;\
return ret;

};

#endif	// PROFILER_H
