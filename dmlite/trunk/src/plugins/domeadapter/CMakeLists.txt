cmake_minimum_required (VERSION 2.6)

add_library (domeadapter MODULE DomeAdapter.cpp
								DomeAdapterDiskCatalog.cpp
                DomeAdapterIO.cpp
                DomeAdapterPools.cpp
                DomeAdapterDriver.cpp
							  DomeAdapterHeadCatalog.cpp
                DomeAdapterAuthn.cpp
							  ${DMLITE_DAVIX_POOL_SOURCES})

include_directories (${DAVIX_PKG_INCLUDE_DIRS} ${Boost_INCLUDE_DIR})

target_link_libraries (domeadapter ${Boost_LIBRARIES} ${DAVIX_PKG_LIBRARIES} pthread dmlite )
set_target_properties (domeadapter PROPERTIES PREFIX "plugin_")

install (TARGETS       domeadapter
         LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/
                       COMPONENT   dmlite-plugins)


# Configuration files
# configure_file (${CMAKE_CURRENT_SOURCE_DIR}/domeadapter.example.in
#                 ${CMAKE_CURRENT_BINARY_DIR}/domeadapter.conf
#                 @ONLY)

install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/domeadapter.example
         DESTINATION ${INSTALL_PFX_ETC}/dmlite.conf.d)
