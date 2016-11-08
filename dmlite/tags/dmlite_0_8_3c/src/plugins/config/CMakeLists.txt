cmake_minimum_required (VERSION 2.6)

add_library (config MODULE Config.cpp)

target_link_libraries (config dmlite)
set_target_properties (config PROPERTIES PREFIX "plugin_")

install (TARGETS       config
         LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/
                       COMPONENT   dmlite-plugins)
