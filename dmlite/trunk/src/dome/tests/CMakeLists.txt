cmake_minimum_required (VERSION 2.6)

include_directories(..)
add_executable(QueueTests QueueTests.cpp)
add_executable(DomeTaskTests TestDomeTaskExec.cpp)
target_link_libraries (QueueTests libdome)
target_link_libraries (DomeTaskTests libdome)
