cmake_minimum_required (VERSION 2.6)

add_library (domeadapter MODULE DomeAdapter.cpp
								DomeAdapterCatalog.cpp)

include_directories ( ${DPM_INCLUDE_DIR}/dpm ${DAVIX_PKG_INCLUDE_DIRS})

target_link_libraries (domeadapter ${DPM_LIBRARIES} ${DAVIX_PKG_LIBRARIES} pthread dmlite )
set_target_properties (domeadapter PROPERTIES PREFIX "plugin_")

install (TARGETS       domeadapter
         LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/
                       COMPONENT   dmlite-plugins)


# Configuration files
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/domeadapter.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/domeadapter.conf
                @ONLY)

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/domeadapter.conf
         DESTINATION ${INSTALL_PFX_ETC}/dmlite.conf.d)
