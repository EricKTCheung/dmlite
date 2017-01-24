cmake_minimum_required (VERSION 2.6)

project (dmlite-shell)

# Lib suffix
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX ""
  		CACHE STRING "library directory suffix value")
else (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (LIB_SUFFIX "64"
		CACHE STRING "library directory suffix value")
endif (CMAKE_SIZEOF_VOID_P EQUAL 4)

# Definitions
add_definitions (-D_REENTRANT)

# Subdirectories
add_subdirectory (src)

