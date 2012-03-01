cmake_minimum_required (VERSION 2.6)

add_library(memcache MODULE Memcache.cpp MemcacheCatalog.cpp)

find_package(Boost COMPONENTS wserialization REQUIRED)
#find_package(Memcache)
include_directories(${Boost_INCLUDE_DIR})

target_link_libraries(memcache ${Boost_LIBRARIES})

target_link_libraries (memcache dmlite)
target_link_libraries (memcache memcached)
set_target_properties (memcache PROPERTIES PREFIX "plugin_")

install(TARGETS       memcache
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
