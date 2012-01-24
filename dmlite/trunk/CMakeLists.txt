cmake_minimum_required (VERSION 2.6)

project (libdm)

# Version
set(libdm_MAJOR 0)
set(libdm_MINOR 2)
set(libdm_PATCH 0)

# Lib suffix
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX "")
  set (PKG_ARCH "i386")
else (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX 64)
  set (PKG_ARCH "x86_64")
endif (CMAKE_SIZEOF_VOID_P EQUAL 4)

# Include path
include_directories (${CMAKE_SOURCE_DIR}/include/)

# Subdirectories
add_subdirectory (core)
add_subdirectory (plugins)
add_subdirectory (tests)
add_subdirectory (etc)

# Install headers
install (DIRECTORY   include/
         DESTINATION usr/include
         COMPONENT   dmlite-devel
         PATTERN     ".svn" EXCLUDE)
