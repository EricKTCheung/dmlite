cmake_minimum_required (VERSION 2.6)

find_package (CppUnit)

# Configure CTest testing
ENABLE_TESTING()

if (CPPUNIT_FOUND)
  set (RUN_ONLY_STANDALONE_TESTS OFF CACHE BOOL "Enable only tests that can run without pre-requirements")

  add_subdirectory (cpp)
  add_subdirectory (c)
endif (CPPUNIT_FOUND)

add_subdirectory(dpm)