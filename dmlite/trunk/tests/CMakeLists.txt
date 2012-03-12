cmake_minimum_required (VERSION 2.6)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/NsMySql.conf
                ${CMAKE_CURRENT_BINARY_DIR}/NsMySql.conf)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/MemcacheNsMySql.conf
                ${CMAKE_CURRENT_BINARY_DIR}/MemcacheNsMySql.conf)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/NsAdapter.conf
                ${CMAKE_CURRENT_BINARY_DIR}/NsAdapter.conf)

add_executable        (test-load test-load.cpp)
target_link_libraries (test-load dmlite cppunit dl)

add_executable        (test-split_path test-split_path.cpp)
target_link_libraries (test-split_path dmlite dmlitecommon dl)

add_executable        (test-split_url test-split_url.cpp)
target_link_libraries (test-split_url dmlite dmlitecommon dl)

add_executable        (test-security test-security.cpp)
target_link_libraries (test-security dmlite dmlitecommon cppunit dl)

add_executable        (test-comment test-comment.cpp test-base.cpp)
target_link_libraries (test-comment dmlite dmlitecommon cppunit dl)

add_executable        (test-stat test-stat.cpp test-base.cpp)
target_link_libraries (test-stat dmlite dmlitecommon cppunit dl)

add_executable        (test-poolfs test-poolfs.cpp)
target_link_libraries (test-poolfs dmlite dmlitecommon dl)

add_executable        (test-pools test-pools.cpp)
target_link_libraries (test-pools dmlite dmlitecommon dl)

add_executable        (test-poolcontainer test-poolcontainer.cpp)
target_link_libraries (test-poolcontainer dmlite dmlitecommon cppunit pthread)

add_executable        (test-chdir test-chdir.cpp test-base.cpp)
target_link_libraries (test-chdir dmlite dmlitecommon cppunit dl)

add_executable        (test-guid test-guid.cpp test-base.cpp)
target_link_libraries (test-guid dmlite dmlitecommon cppunit dl)

add_executable        (test-create test-create.cpp test-base.cpp)
target_link_libraries (test-create dmlite dmlitecommon cppunit dl)

add_executable        (test-replicas test-replicas.cpp test-base.cpp)
target_link_libraries (test-replicas dmlite dmlitecommon cppunit dl)

add_executable        (test-rename test-rename.cpp test-base.cpp)
target_link_libraries (test-rename dmlite dmlitecommon cppunit dl)

add_executable        (test-chown test-chown.cpp test-base.cpp)
target_link_libraries (test-chown dmlite dmlitecommon cppunit dl)

add_executable        (test-utime test-utime.cpp test-base.cpp)
target_link_libraries (test-utime dmlite dmlitecommon cppunit dl)

add_executable        (test-opendir test-opendir.cpp test-base.cpp)
target_link_libraries (test-opendir dmlite dmlitecommon cppunit dl)

add_executable        (test-acls test-acls.cpp test-base.cpp)
target_link_libraries (test-acls dmlite dmlitecommon cppunit dl)

# Configure CTest testing
ENABLE_TESTING()

# Set Test Parameters
set (CONFIG MemcacheNsMySql.conf)
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
ADD_TEST(test-poolfs ${CMAKE_CURRENT_BINARY_DIR}/test-poolfs ${PLUGIN_SO} ${PLUGIN} ${POOL})
SET_TESTS_PROPERTIES(test-poolfs PROPERTIES PASS_REGULAR_EXPRESSION
"Pool;Filesystem")
ADD_TEST(test-pools ${CMAKE_CURRENT_BINARY_DIR}/test-pools ${PLUGIN_SO} ${PLUGIN} ${POOL})
SET_TESTS_PROPERTIES(test-pools PROPERTIES PASS_REGULAR_EXPRESSION
"Pool;Filesystem")
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
