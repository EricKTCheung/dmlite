cmake_minimum_required (VERSION 2.6)
set (CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")

# -------------------------------------------------------
# Detect if an environement variable exists for JAVA_HOME
# Set it to the alternatives java sdk otherwise
# -------------------------------------------------------
if(NOT ENV{JAVA_HOME})
  SET(ENV{JAVA_HOME} "/etc/alternatives/java_sdk")
endif(NOT ENV{JAVA_HOME})

# ----------------------------------------------------
# libjvm.so, libhdfs.so and their associated headers
# are required for this build, this two package finder
# set the cmake variable correctly.
# ----------------------------------------------------
find_package(Hadoop REQUIRED)
find_package(JNI    REQUIRED)

# -----------------------------------------------
# Information to see which libs and dirs are used
# -----------------------------------------------
if(JNI_INCLUDE_DIRS)
	message(STATUS "jni include found in: ${JNI_INCLUDE_DIRS}")
endif(JNI_INCLUDE_DIRS)
if(JNI_LIBRARIES)
	message(STATUS "jni libraries found in: ${JNI_LIBRARIES}")
else(JNI_LIBRRARIES)
	message(STATUS "jni libraries not found")
endif(JNI_LIBRARIES)

# ------------------------------------------
# Hadoop module compilation and installation
# ------------------------------------------
include_directories(${JNI_INCLUDE_DIRS})
add_library(hadoop SHARED Hadoop.cpp)
target_link_libraries (hadoop dmlite dmlitecommon hdfs dl ${JNI_LIBRARIES})
set_target_properties (hadoop PROPERTIES PREFIX "plugin_")

install(TARGETS       hadoop
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
