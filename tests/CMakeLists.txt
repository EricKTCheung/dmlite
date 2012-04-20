cmake_minimum_required (VERSION 2.6)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/NsMySql.conf
                ${CMAKE_CURRENT_BINARY_DIR}/NsMySql.conf)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/MemcacheNsMySql.conf
                ${CMAKE_CURRENT_BINARY_DIR}/MemcacheNsMySql.conf)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/NsAdapter.conf
                ${CMAKE_CURRENT_BINARY_DIR}/NsAdapter.conf)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/DpmAdapter.conf
                ${CMAKE_CURRENT_BINARY_DIR}/DpmAdapter.conf)

add_executable        (test-load test-load.cpp)
target_link_libraries (test-load dmlite cppunit dl)

add_executable        (test-split_path test-split_path.cpp)
target_link_libraries (test-split_path dmlitecommon dmlite dl)

add_executable        (test-split_url test-split_url.cpp)
target_link_libraries (test-split_url dmlitecommon dmlite dl)

add_executable        (test-security test-security.cpp test-base.cpp)
target_link_libraries (test-security dmlitecommon dmlite cppunit dl)

add_executable        (test-comment test-comment.cpp test-base.cpp)
target_link_libraries (test-comment dmlitecommon dmlite cppunit dl)

add_executable        (test-stat test-stat.cpp test-base.cpp)
target_link_libraries (test-stat dmlitecommon dmlite cppunit dl)

add_executable        (test-pools test-pools.cpp)
target_link_libraries (test-pools dmlitecommon dmlite dl)

add_executable        (test-poolcontainer test-poolcontainer.cpp)
target_link_libraries (test-poolcontainer dmlitecommon dmlite cppunit pthread)

add_executable        (test-chdir test-chdir.cpp test-base.cpp)
target_link_libraries (test-chdir dmlitecommon dmlite cppunit dl)

add_executable        (test-guid test-guid.cpp test-base.cpp)
target_link_libraries (test-guid dmlitecommon dmlite cppunit dl)

add_executable        (test-create test-create.cpp test-base.cpp)
target_link_libraries (test-create dmlitecommon dmlite cppunit dl)

add_executable        (test-replicas test-replicas.cpp test-base.cpp)
target_link_libraries (test-replicas dmlitecommon dmlite cppunit dl)

add_executable        (test-rename test-rename.cpp test-base.cpp)
target_link_libraries (test-rename dmlitecommon dmlite cppunit dl)

add_executable        (test-chown test-chown.cpp test-base.cpp)
target_link_libraries (test-chown dmlitecommon dmlite cppunit dl)

add_executable        (test-utime test-utime.cpp test-base.cpp)
target_link_libraries (test-utime dmlitecommon dmlite cppunit dl)

add_executable        (test-opendir test-opendir.cpp test-base.cpp)
target_link_libraries (test-opendir dmlitecommon dmlite cppunit dl)

add_executable        (test-acls test-acls.cpp test-base.cpp)
target_link_libraries (test-acls dmlitecommon dmlite cppunit dl)

add_executable        (test-io test-io.cpp)
target_link_libraries (test-io dmlitecommon dmlite cppunit dl)

add_executable        (test-checkperm test-checkperm.cpp)
target_link_libraries (test-checkperm dmlitecommon dmlite cppunit dl)


find_package(JNI REQUIRED)

include_directories (${CMAKE_SOURCE_DIR}/plugins/)
include_directories("/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/include/")
include_directories("/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/include/linux")

add_executable        (test-hadoop test-hadoop.cpp)
target_link_libraries (test-hadoop dmlitecommon dmlite dl)

# Configure CTest testing
ENABLE_TESTING()

# Set Test Parameters
#set (CONFIG MemcacheNsMySql.conf)
set (CONFIG NsMySql.conf)
set (FSCONFIG DpmAdapter.conf)
set (PLUGIN plugin_adapter_dpm)
set (PLUGIN_SO ../plugins/adapter/plugin_adapter.so)
set (POOL pool1)

# Add CTest tests
ADD_TEST(test-chdir ${CMAKE_CURRENT_BINARY_DIR}/test-chdir ${CONFIG})
SET_TESTS_PROPERTIES(test-chdir PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-create ${CMAKE_CURRENT_BINARY_DIR}/test-create ${CONFIG})
SET_TESTS_PROPERTIES(test-create PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-load ${CMAKE_CURRENT_BINARY_DIR}/test-load ${CONFIG})
SET_TESTS_PROPERTIES(test-load PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-comment ${CMAKE_CURRENT_BINARY_DIR}/test-comment ${CONFIG})
SET_TESTS_PROPERTIES(test-comment PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-security ${CMAKE_CURRENT_BINARY_DIR}/test-security ${CONFIG})
SET_TESTS_PROPERTIES(test-security PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-replicas ${CMAKE_CURRENT_BINARY_DIR}/test-replicas ${CONFIG})
SET_TESTS_PROPERTIES(test-replicas PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-guid ${CMAKE_CURRENT_BINARY_DIR}/test-guid ${CONFIG})
SET_TESTS_PROPERTIES(test-guid PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-stat ${CMAKE_CURRENT_BINARY_DIR}/test-stat ${CONFIG})
SET_TESTS_PROPERTIES(test-stat PROPERTIES PASS_REGULAR_EXPRESSION "OK")
#ADD_TEST(test-pools ${CMAKE_CURRENT_BINARY_DIR}/test-pools ${PLUGIN_SO} ${PLUGIN} ${POOL})
ADD_TEST(test-pools ${CMAKE_CURRENT_BINARY_DIR}/test-pools ${FSCONFIG})
SET_TESTS_PROPERTIES(test-pools PROPERTIES PASS_REGULAR_EXPRESSION "Pool;Capacity")
ADD_TEST(test-poolcontainer ${CMAKE_CURRENT_BINARY_DIR}/test-poolcontainer ${PLUGIN_SO} ${PLUGIN} ${POOL})
SET_TESTS_PROPERTIES(test-poolcontainer PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-split_path ${CMAKE_CURRENT_BINARY_DIR}/test-split_path ${CONFIG})
SET_TESTS_PROPERTIES(test-split_path PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-split_url ${CMAKE_CURRENT_BINARY_DIR}/test-split_url ${CONFIG})
SET_TESTS_PROPERTIES(test-split_url PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-chown ${CMAKE_CURRENT_BINARY_DIR}/test-chown ${CONFIG})
SET_TESTS_PROPERTIES(test-chown PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-utime ${CMAKE_CURRENT_BINARY_DIR}/test-utime ${CONFIG})
SET_TESTS_PROPERTIES(test-utime PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-opendir ${CMAKE_CURRENT_BINARY_DIR}/test-opendir ${CONFIG})
SET_TESTS_PROPERTIES(test-opendir PROPERTIES PASS_REGULAR_EXPRESSION "OK")
ADD_TEST(test-acls ${CMAKE_CURRENT_BINARY_DIR}/test-acls ${CONFIG})
SET_TESTS_PROPERTIES(test-acls PROPERTIES PASS_REGULAR_EXPRESSION "OK")
