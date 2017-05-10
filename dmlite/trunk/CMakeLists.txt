cmake_minimum_required (VERSION 2.6)

set (CMAKE_INSTALL_PREFIX "/" CACHE PATH "Global installation prefix. Works for relocating the complete installation. Normally / ")

project (dmlite)

include(FindPkgConfig)

# Version
set(dmlite_MAJOR 1)
set(dmlite_MINOR 10)
set(dmlite_PATCH 0)

# Optional builds
option (BUILD_PYTHON "Build Python bindings" ON)
option (BUILD_SHELL "Build the dmlite shell" ON)
option (BUILD_MYSQL "Build the mysql plugin" ON)
option (BUILD_MEMCACHE "Build the memcache plugin" ON)
option (BUILD_ADAPTER "Build the LCGDM adapter plugin" ON)
option (BUILD_DOME  "Build the dome fastcgi component" ON)
option (BUILD_DOMEADAPTER "Build the dome adapter plugin" ON)

OPTION(RSYSLOG_SUPPORT "Enable rsyslog Support" ON)
OPTION(LOGROTATE_SUPPORT "Enable logrotate Support" ON)

set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build mode, debug/release")

# Lib suffix
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX ""
  		CACHE STRING "library directory suffix value")
else (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX "64"
		CACHE STRING "library directory suffix value")
endif (CMAKE_SIZEOF_VOID_P EQUAL 4)


# Installation paths. These are supposed to be relative to CMAKE_INSTALL_PREFIX,
# which is normally supposed to be "/"



set (INSTALL_PFX_ETC "etc/" CACHE PATH "relative path suffix to the etc directory where to install")
set (INSTALL_PFX_USR "usr/" CACHE PATH "relative path suffix to the usr directory where to install")
set (INSTALL_PFX_LIB "${INSTALL_PFX_USR}/lib${LIB_SUFFIX}/" CACHE PATH "relative path suffix to the lib/lib64 directory where to install")
set( INSTALL_PFX_DOC "${INSTALL_PFX_USR}/share/doc/${PROJECT_NAME}-${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}" CACHE PATH "relative path suffix to the docs directory where to install")
set (INSTALL_PFX_VAR "var/www/" CACHE STRING "relative path suffix to the /var/www directory where to install fast-cgi")
set (INSTALL_PFX_BIN "usr/bin/" CACHE STRING "relative path suffix to the /usr/bin directory where to install dome-checksum")



# Cmake module directory
set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")

#---------------------------------
#--------------------------------
# needed for Boost 2.8
set (Boost_NO_BOOST_CMAKE ON)
#---------------------------------
# ---------------- Find Boost 1.41
set (BOOST_INCLUDEDIR "/usr/include/boost148") # EPEL5
set (BOOST_LIBRARYDIR "/usr/lib${LIB_SUFFIX}/boost148/") # EPEL5
set (Boost_ADDITIONAL_VERSIONS "1.48" "1.48.0")

#needed for GCC6
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++98")

find_package (Boost 1.48 COMPONENTS date_time system thread regex REQUIRED)

if (Boost_FOUND)
  # Hack for SL5 with EPEL5 enabled
  if (Boost_INCLUDE_DIR STREQUAL "/usr/include/boost148")
    message ("INFO: Overriding Boost library location")
    set (Boost_LIBRARY_DIRS "/usr/lib${LIB_SUFFIX}/boost148/")
  endif ()

  message ("Boost headers under: ${Boost_INCLUDE_DIR}")
  message ("Boost libraries: ${Boost_LIBRARY_DIRS}")
endif (Boost_FOUND)



#--------------------------------
# Find MySQL
find_package (MySQL)
if (NOT MYSQL_FOUND)
  message("-- Mysql not found... disabling the MySQL plugin")
  message("-- Mysql not found... disabling the dome fastcgi component")
  set (BUILD_MYSQL OFF)
  set (BUILD_DOME OFF)
endif()

#--------------------------------
# Find Protocol Buffers
find_package(Protobuf REQUIRED)

#--------------------------------
# Find DPM/LCGDM
find_package (DPM)
if (NOT DPM_FOUND OR NOT DPM_FOUND)
  message("-- DPM not found... disabling the adapter plugin")
  set (BUILD_ADAPTER OFF)
endif()



#--------------------------------
# Find Davix
pkg_check_modules(DAVIX_PKG davix)
if (NOT DAVIX_PKG_FOUND)
  message("-- Davix not found... disabling dome fastcgi component")
  message("-- Davix not found... disabling domeadapter component")
  set (BUILD_DOME OFF)
  set (BUILD_DOMEADAPTER OFF)
endif()



# Include path
include_directories (${CMAKE_CURRENT_BINARY_DIR}/include/
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite/cpp
                     ${CMAKE_CURRENT_SOURCE_DIR}/src
                     ${Boost_INCLUDE_DIRS}
                     ${APR_INCLUDE_DIR}
                     )




# include the directory itself as a path to include directories
set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Link path
link_directories (${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)

# Definitions
add_definitions (-Wall -D_REENTRANT -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DBOOST_SPIRIT_THREADSAFE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++98")

# enable testing
enable_testing()

# Subdirectories
add_subdirectory (doc)
add_subdirectory (etc)
add_subdirectory (include)
add_subdirectory (src)
add_subdirectory (scripts)
add_subdirectory (tests)

if (BUILD_PYTHON)
  add_subdirectory (python)
endif (BUILD_PYTHON)

if (BUILD_SHELL)
  add_subdirectory (shell)
endif (BUILD_SHELL)


message(" ----------------- build summary ----------------")
message(" -- BUILD_PYTHON       : " ${BUILD_PYTHON} )
message(" -- BUILD_SHELL        : " ${BUILD_SHELL} )
message(" -- BUILD_MYSQL        : " ${BUILD_MYSQL} )
message(" -- BUILD_MEMCACHE     : " ${BUILD_MEMCACHE} )
message(" -- BUILD_ADAPTER      : " ${BUILD_ADAPTER} )
message(" -- BUILD_DOME         : " ${BUILD_DOME} )
message(" -- BUILD_DOMEADAPTER  : " ${BUILD_DOMEADAPTER} )
message(" --" )
message(" -- RSYSLOG CFG        : " ${RSYSLOG_SUPPORT} )
message(" -- LOGROTATE CFG      : " ${LOGROTATE_SUPPORT} )
message(" ---------------- ")





#######################################################
# Configure an 'uninstall' target
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake")
