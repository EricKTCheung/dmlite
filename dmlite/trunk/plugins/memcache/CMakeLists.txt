cmake_minimum_required (VERSION 2.6)

set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")
find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
wrap_proto(PROTO_SRC MemcacheCatalog.proto)

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp
 ${PROTO_SRC})

#find_package(Memcache)
target_link_libraries (memcache dmlite dmlitecommon memcached protobuf pthread)
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
