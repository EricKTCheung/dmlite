cmake_minimum_required (VERSION 2.6)

include_directories(${Boost_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR} ${DAVIX_PKG_INCLUDE_DIRS})

set(Dome_SOURCES DomeGenQueue.cpp
                 DomeCore.cpp
                 DomeCoreXeq.cpp
                 DomeLog.cpp
                 DomeTaskExec.cpp
                 DomeReq.cpp
                 DomeMysql.cpp
                 DomeMysql_cns.cpp
                 DomeMysql_authn.cpp
                 DomeStatus.cpp
                 DomeMetadataCache.cpp
                 ../utils/MySqlPools.cpp
                 ../utils/MySqlWrapper.cpp
                 ../utils/Config.cc
                 ${DMLITE_UTILS_SOURCES})


include_directories(${DAVIX_PKG_INCLUDE_DIRS})

set ( CMAKE_CXX_FLAGS "-Wall ${CMAKE_CXX_FLAGS}" )
add_library           (libdome STATIC ${Dome_SOURCES} ${DMLITE_UTILS_SOURCES} ${DMLITE_DAVIX_POOL_SOURCES})
target_link_libraries (libdome fcgi ${DAVIX_PKG_LIBRARIES} ${MYSQL_LIBRARIES} ${Boost_LIBRARIES} crypto dmlite pthread dl)
set_target_properties (libdome PROPERTIES PREFIX "") # fixes liblib issue

add_executable        (dome DomeMain.cpp)
target_link_libraries (dome libdome ${DAVIX_PKG_LIBRARIES})

add_executable        (dome-checksum DomeChecksum.cpp)
target_link_libraries (dome-checksum z crypto ssl ${DAVIX_PKG_LIBRARIES})

# Install
install (TARGETS dome
         DESTINATION            ${INSTALL_PFX_VAR}/fcgi-bin/
         PERMISSIONS            OWNER_EXECUTE OWNER_WRITE OWNER_READ
                                GROUP_EXECUTE GROUP_READ
                                WORLD_EXECUTE WORLD_READ )

install (TARGETS dome-checksum
         DESTINATION            ${INSTALL_PFX_BIN}
         PERMISSIONS            OWNER_EXECUTE OWNER_WRITE OWNER_READ
                                GROUP_EXECUTE GROUP_READ
                                WORLD_EXECUTE WORLD_READ )
# tests
add_subdirectory(tests)
