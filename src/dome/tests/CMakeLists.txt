cmake_minimum_required (VERSION 2.6)

find_package (CppUnit)

# Configure CTest testing
ENABLE_TESTING()

include_directories(..)
add_executable(QueueTests QueueTests.cpp)
target_link_libraries (QueueTests libdome ${DAVIX_PKG_LIBRARIES})

if (CPPUNIT_FOUND)
  set (RUN_ONLY_STANDALONE_TESTS OFF CACHE BOOL "Enable only tests that can run without pre-requirements")
  include_directories (${CPPUNIT_INCLUDE_DIR})

  add_executable(DomeTaskTests DomeTaskTest.cpp)
  target_link_libraries (DomeTaskTests libdome  ${CPPUNIT_LIBRARY} ${DAVIX_PKG_LIBRARIES})

  ADD_TEST(test-dometask     ${CMAKE_CURRENT_BINARY_DIR}/DomeTaskTests)
endif (CPPUNIT_FOUND)
