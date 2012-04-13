cmake_minimum_required (VERSION 2.6)

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
