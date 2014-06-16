cmake_minimum_required (VERSION 2.6)

add_subdirectory (config)

if (BUILD_MEMCACHE)
  add_subdirectory (memcache)
else()
 message("-- The memcache plugin will NOT be built")
endif()

add_subdirectory (profiler)

if (BUILD_MYSQL)
  add_subdirectory (mysql)
else()
    message("-- The mysql plugin will NOT be built")
endif()
