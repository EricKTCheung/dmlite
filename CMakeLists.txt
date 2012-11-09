cmake_minimum_required (VERSION 2.6)

project (dmlite)

# Version
set(dmlite_MAJOR 0)
set(dmlite_MINOR 5)
set(dmlite_PATCH 0)

# Host certificate location
set (HOST_CERTIFICATE "/etc/grid-security/hostcert.pem" CACHE STRING "Location of the host certificate")

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
    message ("WARNING: Overriding Boost library location")
    set (Boost_LIBRARY_DIRS "/usr/lib${LIB_SUFFIX}/boost141/")
  endif ()

  message ("Boost headers under: ${Boost_INCLUDE_DIR}")
  message ("Boost libraries: ${Boost_LIBRARY_DIRS}")
endif (Boost_FOUND)

# Include path
include_directories (${CMAKE_CURRENT_SOURCE_DIR}/include/
                     ${Boost_INCLUDE_DIRS}
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite
                     ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite/cpp
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/include/
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/include/dmlite
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/include/dmlite/python
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/embedding/include/
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/embedding/include/dmlite
                     ${CMAKE_CURRENT_SOURCE_DIR}/python/embedding/include/dmlite/python)

# Link path
link_directories (${Boost_LIBRARY_DIRS} ${CMAKE_BINARY_DIR}/src)

# Definitions
add_definitions (-D_REENTRANT)

# Subdirectories
add_subdirectory (etc)
add_subdirectory (src)

# Enable tests only if CPPUNIT is found
find_package (CppUnit)
if (CPPUNIT_FOUND)
  add_subdirectory (tests)
endif (CPPUNIT_FOUND)

# Enable python only if pythonlib is found
find_package(PythonLibs)
if (Boost_FOUND)
  add_subdirectory (python)
endif (Boost_FOUND)

# Config header
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite/common/config.h.cmake
                ${CMAKE_CURRENT_SOURCE_DIR}/include/dmlite/common/config.h)

# Install headers
install (DIRECTORY   include/
                     python/include/
                     python/embedding/include/
         DESTINATION usr/include
         COMPONENT   dmlite-devel
         PATTERN     ".svn" EXCLUDE
         PATTERN     "*.cmake" EXCLUDE)

# Generate documentation
find_package (Doxygen)

if (DOXYGEN_FOUND)
  configure_file (${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  configure_file (${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile-man.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile-man @ONLY)

  add_custom_target(doc ALL
                   DEPENDS doc/html doc/man)

  add_custom_command(OUTPUT doc/html
                    COMMAND ${DOXYGEN_EXECUTABLE} ARGS ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                     COMMENT "Generating API documentation with Doxygen" VERBATIM)

  add_custom_command(OUTPUT doc/man
                     COMMAND ${DOXYGEN_EXECUTABLE} ARGS ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile-man
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                     COMMENT "Generating man documentation with Doxygen" VERBATIM)


  install (DIRECTORY   ${CMAKE_CURRENT_BINARY_DIR}/doc/html/
           DESTINATION usr/share/doc/${PROJECT_NAME}-${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}/api
           COMPONENT   dmlite-devel)
  install (DIRECTORY   ${CMAKE_CURRENT_BINARY_DIR}/doc/man/
           DESTINATION usr/share/man
           COMPONENT   dmlite-devel)

endif (DOXYGEN_FOUND)

# Tutorial
install (DIRECTORY   ${CMAKE_CURRENT_SOURCE_DIR}/doc/tutorial/
         DESTINATION usr/share/doc/${PROJECT_NAME}-${dmlite_MAJOR}.${dmlite_MINOR}.${dmlite_PATCH}/tutorial
         COMPONENT   dmlite-devel
         PATTERN ".svn" EXCLUDE)

# Configure CTest testing
ENABLE_TESTING()







#######################################################
# Configure an 'uninstall' target
CONFIGURE_FILE(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

ADD_CUSTOM_TARGET(uninstall
  "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake")

