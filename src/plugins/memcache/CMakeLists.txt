cmake_minimum_required (VERSION 2.6)


find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
ADD_CUSTOM_COMMAND(
         OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/MemcacheCatalog.pb.h ${CMAKE_CURRENT_BINARY_DIR}/MemcacheCatalog.pb.cc
         COMMAND protoc -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/MemcacheCatalog.proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
         DEPENDS MemcacheCatalog.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp
                            MemcacheFunctionCounter.cpp
                            MemcacheCatalog.pb.cc)

include_directories (${Boost_INCLUDE_DIR} ${DMLITE_INCLUDE_DIR})

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries (memcache dmlite boost_system memcached protobuf pthread)
ELSE()
  target_link_libraries (memcache dmlite memcached protobuf pthread)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/)



configure_file (${CMAKE_CURRENT_SOURCE_DIR}/memcache.conf.in
                ${CMAKE_CURRENT_BINARY_DIR}/zmemcache.conf
                @ONLY)

install (FILES       ${CMAKE_CURRENT_BINARY_DIR}/zmemcache.conf
         DESTINATION etc/dmlite.conf.d)