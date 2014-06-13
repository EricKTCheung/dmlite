cmake_minimum_required (VERSION 2.6)
set(CMAKE_INCLUDE_CURRENT_DIR ON)
set (BOOST_INCLUDEDIR "/usr/include/boost141") # EPEL5
find_package(Boost 1.41 REQUIRED)
find_package(Protobuf REQUIRED)

# build the protobuf *.pb.(h|cc) files
wrap_proto(PROTO_SRC MemcacheCatalog.proto)

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp
                            MemcacheBloomFilter.cpp
                            MemcacheFunctionCounter.cpp
                            ${PROTO_SRC})

include_directories(${PROJECT_BINARY_DIR})
include_directories(${PROJECT_BINARY_DIR}/src)

include_directories (${Boost_INCLUDE_DIR})

target_link_libraries (memcache dmlite memcached protobuf pthread)
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/)
