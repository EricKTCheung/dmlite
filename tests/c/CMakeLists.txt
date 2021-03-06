cmake_minimum_required (VERSION 2.6)

# NOTE: Functionality is actually tested by the CPP tests
#       These here are intended to check if the mapping between C and CPP is
#       working as intended, so a mock plugin is used.
#       Since a mock plugin is used, all tests can be run when
#       RUN_ONLY_STANDALONE_TESTS is used.

add_library           (mock MODULE plugin_mock.cpp plugin_mock.h)
set_target_properties (mock PROPERTIES PREFIX "plugin_")
target_link_libraries (mock dmlite)

add_library           (utils STATIC utils.c utils.h)

add_executable        (test-any-c test-any.c)
target_link_libraries (test-any-c dmlite utils)

add_executable        (test-catalog-c test-catalog.c)
target_link_libraries (test-catalog-c dmlite utils)

add_executable        (test-checksums test-checksums.c)
target_link_libraries (test-checksums dmlite utils)

add_executable        (test-converters test-converters.cpp)
target_link_libraries (test-converters dmlite utils)

add_executable        (test-io-c test-io.c)
target_link_libraries (test-io-c dmlite utils)

add_executable        (test-manager-c test-manager.c)
target_link_libraries (test-manager-c dmlite utils)

add_executable        (test-pool-c test-pool.c)
target_link_libraries (test-pool-c dmlite utils)

# Install
install (DIRECTORY 		${CMAKE_CURRENT_BINARY_DIR}/
         DESTINATION		${INSTALL_PFX_LIB}/dmlite/test/c
         FILE_PERMISSIONS	OWNER_EXECUTE OWNER_WRITE OWNER_READ
                      		GROUP_EXECUTE GROUP_READ
                      		WORLD_EXECUTE WORLD_READ
         FILES_MATCHING PATTERN test-*-c
                        PATTERN plugin_mock.so
                        PATTERN *.o   EXCLUDE
                        PATTERN *.dir EXCLUDE
                        PATTERN .svn  EXCLUDE
                        PATTERN CMakeFiles EXCLUDE)

# Add CTest tests
ENABLE_TESTING()

ADD_TEST(test-any-c        ${CMAKE_CURRENT_BINARY_DIR}/test-any-c)
ADD_TEST(test-catalog-c    ${CMAKE_CURRENT_BINARY_DIR}/test-catalog-c)
ADD_TEST(test-checksums-c  ${CMAKE_CURRENT_BINARY_DIR}/test-checksums)
ADD_TEST(test-converters-c ${CMAKE_CURRENT_BINARY_DIR}/test-converters)
ADD_TEST(test-io-c         ${CMAKE_CURRENT_BINARY_DIR}/test-io-c)
ADD_TEST(test-manager-c    ${CMAKE_CURRENT_BINARY_DIR}/test-manager-c)
ADD_TEST(test-pool-c       ${CMAKE_CURRENT_BINARY_DIR}/test-pool-c)
