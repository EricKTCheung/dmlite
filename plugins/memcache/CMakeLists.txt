cmake_minimum_required (VERSION 2.6)

find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
wrap_proto(PROTO_SRC MemcacheCatalog.proto)

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp
 ${PROTO_SRC})

#find_package(Memcache)
target_link_libraries (memcache dmlite dmliteutils memcached protobuf pthread)
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
