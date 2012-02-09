cmake_minimum_required (VERSION 2.6)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/NsMySql.conf
                ${CMAKE_CURRENT_BINARY_DIR}/NsMySql.conf)

add_executable        (test-load test-load.cpp)
target_link_libraries (test-load dmlite cppunit dl)

add_executable        (test-split_path test-split_path.cpp)
target_link_libraries (test-split_path dmlite dmlitecommon cppunit dl)

add_executable        (test-split_url test-split_url.cpp)
target_link_libraries (test-split_url dmlite dmlitecommon cppunit dl)

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
