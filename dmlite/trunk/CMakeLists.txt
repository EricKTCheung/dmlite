cmake_minimum_required (VERSION 2.6)

project (dmlite)

# Version
set(dmlite_MAJOR 0)
set(dmlite_MINOR 6)
set(dmlite_PATCH 1)

# Optional builds
option (BUILD_PYTHON "Build Python bindings" ON)

# Lib suffix
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX ""
  		CACHE STRING "library directory suffix value")
else (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX "64"
		CACHE STRING "library directory suffix value")
endif (CMAKE_SIZEOF_VOID_P EQUAL 4)

# Cmake module directory
set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")

# Find Boost 1.41
set (BOOST_INCLUDEDIR "/usr/include/boost141") # EPEL5
set (BOOST_LIBRARYDIR "/usr/lib${LIB_SUFFIX}/boost141/") # EPEL5
set (Boost_ADDITIONAL_VERSIONS "1.41" "1.41.0")

find_package (Boost 1.41 REQUIRED)

if (Boost_FOUND)
  # Hack for SL5 with EPEL5 enabled
  if (Boost_INCLUDE_DIR STREQUAL "/usr/include/boost141")
    message ("INFO: Overriding Boost library location")
    set (Boost_LIBRARY_DIRS "/usr/lib${LIB_SUFFIX}/boost141/")
  endif ()

  message ("Boost headers under: ${Boost_INCLUDE_DIR}")
  message ("Boost libraries: ${Boost_LIBRARY_DIRS}")
endif (Boost_FOUND)

# Include path
include_directories (${CMAKE_CURRENT_SOURCE_DIR}/include/ 
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite 
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite/cpp
                     ${CMAKE_CURRENT_BINARY_DIR}/include/
                     ${Boost_INCLUDE_DIRS})

# Link path
link_directories (${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)

# Definitions
add_definitions (-D_REENTRANT -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DBOOST_SPIRIT_THREADSAFE)

# Subdirectories
add_subdirectory (doc)
add_subdirectory (etc)
add_subdirectory (include)
add_subdirectory (src)
add_subdirectory (tests)

if (BUILD_PYTHON)
  add_subdirectory (python)
endif (BUILD_PYTHON)

#######################################################
# Configure an 'uninstall' target
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake")

