cmake_minimum_required (VERSION 2.6)

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

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/profiler.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/profiler.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/profiler.conf
         DESTINATION etc/dmlite.conf.d)
