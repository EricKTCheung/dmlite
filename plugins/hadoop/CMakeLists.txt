cmake_minimum_required (VERSION 2.6)

#include_directories("/root/hadoop/src/c++/libhdfs/")
include_directories("/usr/java/default/include/")
include_directories("/usr/java/default/include/linux/")
include_directories("/etc/alternatives/java_sdk/include/")
include_directories("/etc/alternatives/java_sdk/include/linux")

link_directories("/root/hadoop/c++/Linux-amd64-64/lib/")
link_directories("/usr/java/default/jre/lib/amd64/server/")
link_directories("/usr/java/default/jre/lib/amd64/server/")
link_directories("/etc/alternatives/java_sdk/jre/lib/amd64/server/")

add_library(hadoop SHARED Hadoop.cpp)
target_link_libraries (hadoop dmlite dmlitecommon hdfs jvm dl)
set_target_properties (hadoop PROPERTIES PREFIX "plugin_")

install(TARGETS       hadoop
        LIBRARY       DESTINATION usr/lib${LIB_SUFFIX}/dmlite/
                      COMPONENT   dmlite-plugins)
