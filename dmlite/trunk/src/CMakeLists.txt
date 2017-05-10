cmake_minimum_required (VERSION 2.6)

add_subdirectory (c)
add_subdirectory (core)
add_subdirectory (utils)

include_directories(${DAVIX_PKG_INCLUDE_DIRS})

add_library(dmlite SHARED ${DMLITE_CAPI_SOURCES}
                          ${DMLITE_CORE_SOURCES}
                          ${DMLITE_UTILS_SOURCES})

set_target_properties(dmlite PROPERTIES COMPILE_FLAGS -rdynamic )



IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries (dmlite boost_regex boost_system ${Boost_THREAD_LIBRARY_RELEASE} crypto dl pthread ssl z)
ELSE()
  target_link_libraries (dmlite boost_regex ${Boost_THREAD_LIBRARY_RELEASE} crypto dl pthread ssl z)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

set_target_properties(dmlite PROPERTIES
                             VERSION ${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}
                             SOVERSION 0) # ${dmlite_MAJOR})

install(TARGETS dmlite
        LIBRARY DESTINATION ${INSTALL_PFX_LIB})

add_subdirectory (plugins)

if (BUILD_DOME)
  add_subdirectory(dome)
else()
    message("-- The dpmrest fastcgi component will NOT be built")
endif()
