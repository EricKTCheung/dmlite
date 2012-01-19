cmake_minimum_required (VERSION 2.6)

add_library(profiler SHARED Profiler.cpp ProfilerCatalog.cpp)

target_link_libraries (profiler dmlite rt)
set_target_properties (profiler PROPERTIES PREFIX "plugin_")

install(TARGETS       profiler
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
