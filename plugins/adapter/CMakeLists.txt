cmake_minimum_required (VERSION 2.6)

set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")
find_package (DPM REQUIRED)

include_directories (${DPM_INCLUDE_DIR}/dpm)
add_definitions (-D_REENTRANT)

add_library (adapter MODULE Adapter.cpp
                            NsAdapter.cpp DpmAdapter.cpp
                            Throw.cpp)

target_link_libraries (adapter ${DPM_LIBRARIES} dmlite dmlitecommon)
set_target_properties (adapter PROPERTIES PREFIX "plugin_")

install (TARGETS       adapter
         LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                       COMPONENT   dmlite-plugins)
