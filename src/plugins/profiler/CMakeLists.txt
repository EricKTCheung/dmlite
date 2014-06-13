cmake_minimum_required (VERSION 2.6)

set (BOOST_INCLUDEDIR "/usr/include/boost141") # EPEL5
find_package (Boost 1.41 REQUIRED)

include_directories(${Boost_INCLUDE_DIR})

add_library(profiler SHARED XrdMonitor.cpp
                            Profiler.cpp
                            ProfilerCatalog.cpp
                            ProfilerPoolManager.cpp
                            ProfilerIO.cpp
                            ProfilerXrdMon.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries (profiler dmlite boost_system)
ELSE()
  target_link_libraries (profiler dmlite rt)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set_target_properties (profiler PROPERTIES PREFIX "plugin_")

install(TARGETS       profiler
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/)
