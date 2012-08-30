# - Try to find CPPUNIT
#
#  CPPUNIT_FOUND - System has CPPUNIT
#  CPPUNIT_INCLUDE_DIR - The CPPUNIT include directory
#  CPPUNIT_LIBRARY - The library needed to use CPPUNIT
#
# CPPUNIT_LOCATION
#   setting this enables search for cppunit library / headers in this location


# -----------------------------------------------------
# CPPUNIT Library
# -----------------------------------------------------
find_library(CPPUNIT_LIBRARY
    NAMES cppunit
    HINTS ${CPPUNIT_LOCATION}/lib ${CPPUNIT_LOCATION}/lib64 ${CPPUNIT_LOCATION}/lib32
    DOC "cppunit library"
)
if(CPPUNIT_LIBRARY)
    message(STATUS "cppunit library found in ${CPPUNIT_LIBRARY}")
endif()

# -----------------------------------------------------
# CPPUNIT Include Directories
# -----------------------------------------------------
find_path(CPPUNIT_INCLUDE_DIR
    NAMES cppunit/TestAssert.h
    HINTS ${CPPUNIT_LOCATION} ${CPPUNIT_LOCATION}/include
    DOC "The cppunit include directory"
)
if(CPPUNIT_INCLUDE_DIR)
    message(STATUS "cppunit includes found in ${CPPUNIT_INCLUDE_DIR}")
endif()

# -----------------------------------------------------
# handle the QUIETLY and REQUIRED arguments and set DPPUNIT_FOUND to TRUE if
# all listed variables are TRUE
# -----------------------------------------------------
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CppUnit DEFAULT_MSG CPPUNIT_LIBRARY CPPUNIT_INCLUDE_DIR)
mark_as_advanced(CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)
