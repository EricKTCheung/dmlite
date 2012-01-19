cmake_minimum_required (VERSION 2.6)

include_directories (/usr/include/dpm)
add_definitions (-D_REENTRANT)

add_library(adapter MODULE Adapter.cpp
                           NsAdapter.cpp DpmAdapter.cpp
                           Throw.cpp)

target_link_libraries (adapter dpm dmlite dmlitecommon)
set_target_properties (adapter PROPERTIES PREFIX "plugin_")

install(TARGETS       adapter
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
