cmake_minimum_required (VERSION 2.6)

set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")
find_package (ORACLE REQUIRED)

link_directories (${ORACLE_LIBRARY_DIR})

include_directories (${ORACLE_INCLUDE_DIR})

add_library (oracle MODULE OracleFactories.cpp NsOracle.cpp)

target_link_libraries (oracle dmlite dmlitecommon occi clntsh)
set_target_properties (oracle PROPERTIES PREFIX "plugin_")

install(TARGETS       oracle
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
