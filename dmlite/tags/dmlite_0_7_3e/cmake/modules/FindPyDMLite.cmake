# - Try to find PYDMLITE libraries
#
#  PYDMLITE_FOUND - System has PYDMLITE
#  PYDMLITE_LIBRARIES - The libraries needed to use PYDMLITE
#
# PYDMLITE_LOCATION
#   setting this enables search for dmlite libraries / headers in this location

find_package(PythonLibs REQUIRED)

execute_process (COMMAND python -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)" 
                 OUTPUT_VARIABLE PYTHON_SITE_PACKAGES
                 OUTPUT_STRIP_TRAILING_WHITESPACE)

# -----------------------------------------------------
# DMLITE Libraries
# -----------------------------------------------------
find_path(PYDMLITE_LIBRARIES
    NAMES pydmlite.so
    HINTS ${PYTHON_SITE_PACKAGES}
    DOC "The main pydmlite library"
)
if(PYDMLITE_LIBRARIES)
    message(STATUS "pydmlite library found in ${PYDMLITE_LIBRARIES}")
endif()

# -----------------------------------------------------
# handle the QUIETLY and REQUIRED arguments and set PYDMLITE_FOUND to TRUE if
# all listed variables are TRUE
# -----------------------------------------------------
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pydmlite DEFAULT_MSG PYDMLITE_LIBRARIES)
mark_as_advanced(PYDMLITE_LIBRARIES)
