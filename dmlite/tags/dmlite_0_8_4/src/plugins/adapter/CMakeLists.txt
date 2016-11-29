cmake_minimum_required (VERSION 2.6)

add_library (adapter MODULE Adapter.cpp
                            DpmAdapter.cpp
                            FilesystemDriver.cpp
                            FunctionWrapper.cpp
                            IO.cpp
                            NsAdapter.cpp
                            NsAdapterINode.cpp
                            RFIO.cpp
            )

include_directories ( ${DPM_INCLUDE_DIR}/dpm )

target_link_libraries (adapter ${DPM_LIBRARIES} pthread dmlite )
set_target_properties (adapter PROPERTIES PREFIX "plugin_")

install (TARGETS       adapter
         LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/
                       COMPONENT   dmlite-plugins)


# Configuration files
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/adapter.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/adapter.conf
                @ONLY)

install (FILES ${CMAKE_CURRENT_BINARY_DIR}/adapter.conf
         DESTINATION ${INSTALL_PFX_ETC}/dmlite.conf.d)
