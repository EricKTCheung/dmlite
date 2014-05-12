cmake_minimum_required (VERSION 2.6)

add_library (proc MODULE Proc.cpp)

target_link_libraries (proc dmlite)
set_target_properties (proc PROPERTIES PREFIX "plugin_")

install (TARGETS       proc
         LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                       COMPONENT   dmlite-plugins)
