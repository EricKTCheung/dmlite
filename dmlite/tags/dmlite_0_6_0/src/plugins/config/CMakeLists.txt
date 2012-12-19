cmake_minimum_required (VERSION 2.6)

add_library (config MODULE Config.cpp)

target_link_libraries (config dmlite)
set_target_properties (config PROPERTIES PREFIX "plugin_")

install (TARGETS       config
         LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                       COMPONENT   dmlite-plugins)
