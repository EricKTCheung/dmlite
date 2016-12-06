cmake_minimum_required (VERSION 2.6)


find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
ADD_CUSTOM_COMMAND(
         OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Memcache.pb.h ${CMAKE_CURRENT_BINARY_DIR}/Memcache.pb.cc
         COMMAND protoc -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/Memcache.proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
         DEPENDS Memcache.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp
                            MemcacheFunctionCounter.cpp
                            MemcacheCommon.cpp
                            MemcachePoolManager.cpp
                            Memcache.pb.cc)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries (memcache dmlite boost_system memcached protobuf pthread)
ELSE()
  target_link_libraries (memcache dmlite memcached protobuf pthread)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION ${INSTALL_PFX_LIB}/dmlite/)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/memcache.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/zmemcache.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/zmemcache.conf
         DESTINATION ${INSTALL_PFX_ETC}/dmlite.conf.d)
