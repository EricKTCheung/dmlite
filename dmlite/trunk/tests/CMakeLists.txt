cmake_minimum_required (VERSION 2.6)

find_package (CppUnit)

# Configure CTest testing
ENABLE_TESTING()

if (CPPUNIT_FOUND)
  add_subdirectory (cpp)
  add_subdirectory (c)
endif (CPPUNIT_FOUND)
